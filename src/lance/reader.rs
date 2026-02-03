//! Read from LanceDB and write to MCAP format.
//!
//! This is the reverse of `writer.rs` - it reads tables from LanceDB and
//! streams them out as an MCAP file with messages ordered by timestamp.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BinaryHeap};
use std::io::Write;
use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch, TimestampNanosecondArray, UInt64Array};
use arrow::datatypes::Schema;
use futures::StreamExt;
use lancedb::query::ExecutableQuery;
use lancedb::Connection;
use thiserror::Error;

use crate::arrow::ArrowRowSource;
use crate::lance::{
    CHANNELS_TABLE, CHANNEL_ID_COLUMN, LOG_TIME_COLUMN, PUBLISH_TIME_COLUMN, SCHEMAS_TABLE,
    SEQUENCE_COLUMN,
};
use crate::ros1::{arrow_schema_to_msg, FromArrowError, Ros1SchemaInfo, Ros1Writer, WriteError};

/// Errors that can occur during LanceDB to MCAP conversion.
#[derive(Debug, Error)]
pub enum LanceToMcapError {
    #[error("LanceDB error: {0}")]
    Lance(#[from] lancedb::Error),

    #[error("MCAP write error: {0}")]
    Mcap(#[from] mcap::McapError),

    #[error("Schema conversion error: {0}")]
    SchemaConversion(#[from] FromArrowError),

    #[error("Serialization error: {0}")]
    Serialization(#[from] WriteError),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Table '{0}' not found")]
    TableNotFound(String),

    #[error("Table '{0}' missing {1} column - was it created with an older version?")]
    MissingColumn(String, String),
}

/// Options for converting LanceDB to MCAP.
#[derive(Debug, Clone)]
pub struct ConvertToMcapOptions {
    /// Batch size for streaming reads from LanceDB.
    pub batch_size: usize,

    /// Topics (tables) to convert. If None, converts all tables.
    pub topics: Option<Vec<String>>,

    /// Message encoding to write in MCAP (default: "ros1msg").
    pub message_encoding: String,

    /// Schema encoding to write in MCAP (default: "ros1msg").
    pub schema_encoding: String,
}

impl Default for ConvertToMcapOptions {
    fn default() -> Self {
        Self {
            batch_size: 1000,
            topics: None,
            message_encoding: "ros1msg".to_string(),
            schema_encoding: "ros1msg".to_string(),
        }
    }
}

/// Statistics from the conversion.
#[derive(Debug, Default)]
pub struct ConvertToMcapStats {
    /// Total number of messages written.
    pub total_messages: usize,

    /// Number of topics (tables) converted.
    pub topics_converted: usize,

    /// Messages per topic.
    pub messages_per_topic: BTreeMap<String, usize>,
}

/// A pending message from one topic, used for timestamp-ordered merging.
struct PendingMessage {
    /// Log time (nanoseconds) - used for ordering
    log_time: u64,
    /// Topic index (for tiebreaking and channel lookup)
    topic_idx: usize,
    /// Row index within the current batch
    row: usize,
    /// The batch containing this message's data
    batch: Arc<RecordBatch>,
}

// Implement ordering for min-heap (BinaryHeap is a max-heap, so reverse the order)
impl PartialEq for PendingMessage {
    fn eq(&self, other: &Self) -> bool {
        self.log_time == other.log_time && self.topic_idx == other.topic_idx
    }
}

impl Eq for PendingMessage {}

impl PartialOrd for PendingMessage {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PendingMessage {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse order for min-heap behavior
        match other.log_time.cmp(&self.log_time) {
            Ordering::Equal => other.topic_idx.cmp(&self.topic_idx),
            ord => ord,
        }
    }
}

/// Metadata for a topic being streamed.
struct TopicStream {
    table_name: String,
    channel_id: u16,
    /// Schema info for serialization (excludes metadata columns)
    schema_info: Ros1SchemaInfo,
    /// Column indices for message fields (excludes metadata columns)
    message_column_indices: Vec<usize>,
    /// Column index for _log_time (used for timestamp ordering)
    log_time_column_idx: usize,
    /// Current sequence number
    sequence: u32,
}

/// Convert LanceDB tables to an MCAP file.
///
/// This function streams data from LanceDB and writes it to MCAP format,
/// with messages ordered by timestamp across all topics. This enables
/// proper playback for robotics applications.
///
/// The writer only needs to implement `Write` - seeking is handled internally
/// using the NoSeek wrapper, making this suitable for stdout or pipes.
pub async fn convert_lance_to_mcap<W: Write>(
    db: &Connection,
    writer: W,
    options: ConvertToMcapOptions,
) -> Result<ConvertToMcapStats, LanceToMcapError> {
    let mut stats = ConvertToMcapStats::default();

    // Get list of tables to convert (excluding metadata tables)
    let all_tables: Vec<String> = db
        .table_names()
        .execute()
        .await?
        .into_iter()
        .filter(|name| name != CHANNELS_TABLE && name != SCHEMAS_TABLE)
        .collect();
    let tables_to_convert: Vec<String> = match &options.topics {
        Some(topics) => {
            // Validate that all requested topics exist
            for topic in topics {
                if !all_tables.contains(topic) {
                    return Err(LanceToMcapError::TableNotFound(topic.clone()));
                }
            }
            topics.clone()
        }
        None => all_tables,
    };

    if tables_to_convert.is_empty() {
        return Ok(stats);
    }

    // Create MCAP writer with NoSeek wrapper and disabled seeking for non-seekable streams
    let write_options = mcap::write::WriteOptions::default().disable_seeking(true);
    let mut mcap_writer =
        mcap::write::Writer::with_options(mcap::write::NoSeek::new(writer), write_options)?;

    // Set up topic streams and register schemas/channels
    let mut topic_streams: Vec<TopicStream> = Vec::new();

    for table_name in tables_to_convert.iter() {
        let table = db.open_table(table_name).execute().await?;
        let arrow_schema = table.schema().await?;

        // Find the _log_time column
        let log_time_column_idx = arrow_schema
            .fields()
            .iter()
            .position(|f| f.name() == LOG_TIME_COLUMN)
            .ok_or_else(|| {
                LanceToMcapError::MissingColumn(table_name.clone(), LOG_TIME_COLUMN.to_string())
            })?;

        // Identify all metadata columns to exclude from message schema
        let is_metadata_column = |name: &str| {
            name == LOG_TIME_COLUMN
                || name == PUBLISH_TIME_COLUMN
                || name == SEQUENCE_COLUMN
                || name == CHANNEL_ID_COLUMN
        };

        // Build message schema (without metadata columns)
        let message_fields: Vec<_> = arrow_schema
            .fields()
            .iter()
            .filter(|f| !is_metadata_column(f.name()))
            .map(|f| f.as_ref().clone())
            .collect();
        let message_schema = Schema::new(message_fields);

        // Get column indices for message fields (excludes metadata columns)
        let message_column_indices: Vec<usize> = arrow_schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, f)| !is_metadata_column(f.name()))
            .map(|(i, _)| i)
            .collect();

        // Convert Arrow schema to ROS1 msg format
        let msg_schema = arrow_schema_to_msg(&message_schema)?;

        // Build field info for serialization
        let schema_info = Ros1SchemaInfo::from_arrow_schema(&message_schema)?;

        // Add schema to MCAP
        let message_type = table_name_to_message_type(table_name);
        let schema_id = mcap_writer.add_schema(
            &message_type,
            &options.schema_encoding,
            msg_schema.as_bytes(),
        )?;

        // Add channel to MCAP
        let channel_id = mcap_writer.add_channel(
            schema_id,
            &table_name_to_topic(table_name),
            &options.message_encoding,
            &BTreeMap::new(),
        )?;

        topic_streams.push(TopicStream {
            table_name: table_name.clone(),
            channel_id,
            schema_info,
            message_column_indices,
            log_time_column_idx,
            sequence: 0,
        });

        stats.messages_per_topic.insert(table_name.clone(), 0);
    }

    stats.topics_converted = topic_streams.len();

    // Streaming merge of topics by timestamp using a priority queue.
    // We assume each topic's data is already sorted by _log_time (since MCAP is time-ordered
    // and we write sequentially during ingestion).
    let mut heap: BinaryHeap<PendingMessage> = BinaryHeap::new();
    let mut writer = Ros1Writer::new();

    // Track streaming state for each topic
    struct TopicBatchState {
        stream: std::pin::Pin<
            Box<dyn futures::Stream<Item = Result<RecordBatch, lancedb::Error>> + Send>,
        >,
        current_batch: Option<Arc<RecordBatch>>,
        current_row: usize,
    }

    let mut batch_states: Vec<TopicBatchState> = Vec::new();

    // Initialize streams for each topic
    for table_name in &tables_to_convert {
        let table = db.open_table(table_name).execute().await?;
        let stream = table.query().execute().await?;

        batch_states.push(TopicBatchState {
            stream: Box::pin(stream),
            current_batch: None,
            current_row: 0,
        });
    }

    // Load initial batch from each topic and seed the heap
    for (topic_idx, state) in batch_states.iter_mut().enumerate() {
        if let Some(batch_result) = state.stream.next().await {
            let batch = Arc::new(batch_result?);
            if batch.num_rows() > 0 {
                let log_time =
                    get_log_time(&batch, topic_streams[topic_idx].log_time_column_idx, 0);
                heap.push(PendingMessage {
                    log_time,
                    topic_idx,
                    row: 0,
                    batch: batch.clone(),
                });
                state.current_batch = Some(batch);
                state.current_row = 0;
            }
        }
    }

    // Process messages in timestamp order using the heap
    while let Some(pending) = heap.pop() {
        let topic_idx = pending.topic_idx;
        let row = pending.row;
        let batch = &pending.batch;
        let stream = &mut topic_streams[topic_idx];

        // Serialize the message (excluding metadata columns)
        writer.clear();
        let arrays: Vec<ArrayRef> = stream
            .message_column_indices
            .iter()
            .map(|&i| batch.column(i).clone())
            .collect();
        let mut source = ArrowRowSource::from_columns_row(arrays, row);
        writer.write_row(&mut source, &stream.schema_info.fields)?;

        // Write to MCAP
        mcap_writer.write_to_known_channel(
            &mcap::records::MessageHeader {
                channel_id: stream.channel_id,
                sequence: stream.sequence,
                log_time: pending.log_time,
                publish_time: pending.log_time,
            },
            writer.as_bytes(),
        )?;

        stream.sequence += 1;
        *stats
            .messages_per_topic
            .get_mut(&stream.table_name)
            .unwrap() += 1;
        stats.total_messages += 1;

        // Advance to next row for this topic
        let state = &mut batch_states[topic_idx];
        state.current_row += 1;

        // Check if we need the next row from current batch or load next batch
        let batch_exhausted = state
            .current_batch
            .as_ref()
            .map(|b| state.current_row >= b.num_rows())
            .unwrap_or(true);

        if batch_exhausted {
            // Try to get next batch
            if let Some(batch_result) = state.stream.next().await {
                let batch = Arc::new(batch_result?);
                if batch.num_rows() > 0 {
                    let log_time = get_log_time(&batch, stream.log_time_column_idx, 0);
                    heap.push(PendingMessage {
                        log_time,
                        topic_idx,
                        row: 0,
                        batch: batch.clone(),
                    });
                    state.current_batch = Some(batch);
                    state.current_row = 0;
                }
            }
        } else {
            // More rows in current batch
            let batch = state.current_batch.as_ref().unwrap().clone();
            let next_row = state.current_row;
            let log_time = get_log_time(&batch, stream.log_time_column_idx, next_row);
            heap.push(PendingMessage {
                log_time,
                topic_idx,
                row: next_row,
                batch,
            });
        }
    }

    // Finalize MCAP file
    mcap_writer.finish()?;

    Ok(stats)
}

/// Get the log_time value from a batch at a specific row.
/// Handles both UInt64 and Timestamp[ns] types for backwards compatibility.
fn get_log_time(batch: &RecordBatch, column_idx: usize, row: usize) -> u64 {
    let column = batch.column(column_idx);

    // Try UInt64 first
    if let Some(arr) = column.as_any().downcast_ref::<UInt64Array>() {
        return arr.value(row);
    }

    // Try Timestamp[ns]
    if let Some(arr) = column.as_any().downcast_ref::<TimestampNanosecondArray>() {
        return arr.value(row) as u64;
    }

    panic!(
        "_log_time column should be UInt64 or Timestamp[ns], got {:?}",
        column.data_type()
    );
}

/// Convert a table name to a ROS message type.
fn table_name_to_message_type(table_name: &str) -> String {
    table_name.to_string()
}

/// Convert a table name to a topic name.
/// e.g., "camera_image_raw" -> "/camera/image/raw"
fn table_name_to_topic(table_name: &str) -> String {
    let name = table_name.strip_prefix('_').unwrap_or(table_name);
    format!("/{}", name.replace('_', "/"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_name_to_message_type() {
        assert_eq!(
            table_name_to_message_type("camera_image_raw"),
            "camera_image_raw"
        );
    }

    #[test]
    fn test_table_name_to_topic() {
        assert_eq!(table_name_to_topic("camera_image_raw"), "/camera/image/raw");
    }

    mod roundtrip {
        use super::*;
        use crate::lance::{convert_mcap_to_lance, ConvertOptions};
        use lancedb::connect;
        use mcap::{Channel, Message, Schema, Writer};
        use std::borrow::Cow;
        use std::collections::BTreeMap;
        use std::io::Cursor;
        use std::sync::Arc;
        use tempfile::tempdir;

        fn create_test_mcap() -> Vec<u8> {
            let mut buf = Vec::new();
            {
                let mut writer = Writer::new(Cursor::new(&mut buf)).unwrap();

                let schema = Arc::new(Schema {
                    id: 1,
                    name: "geometry_msgs/Point".into(),
                    encoding: "ros1msg".into(),
                    data: Cow::Borrowed(b"float64 x\nfloat64 y\nfloat64 z"),
                });

                let channel = Arc::new(Channel {
                    id: 1,
                    topic: "/points".into(),
                    schema: Some(schema.clone()),
                    message_encoding: "ros1".into(),
                    metadata: BTreeMap::new(),
                });

                // Write 5 point messages
                for i in 0..5u32 {
                    let point_data: Vec<u8> = [i as f64, (i * 2) as f64, (i * 3) as f64]
                        .iter()
                        .flat_map(|f| f.to_le_bytes())
                        .collect();

                    let message = Message {
                        channel: channel.clone(),
                        sequence: i,
                        log_time: (i as u64) * 1_000_000_000,
                        publish_time: (i as u64) * 1_000_000_000,
                        data: Cow::Owned(point_data),
                    };
                    writer.write(&message).unwrap();
                }

                writer.finish().unwrap();
            }
            buf
        }

        fn create_multi_topic_mcap() -> Vec<u8> {
            let mut buf = Vec::new();
            {
                let mut writer = Writer::new(Cursor::new(&mut buf)).unwrap();

                let point_schema = Arc::new(Schema {
                    id: 1,
                    name: "geometry_msgs/Point".into(),
                    encoding: "ros1msg".into(),
                    data: Cow::Borrowed(b"float64 x\nfloat64 y\nfloat64 z"),
                });

                let string_schema = Arc::new(Schema {
                    id: 2,
                    name: "std_msgs/String".into(),
                    encoding: "ros1msg".into(),
                    data: Cow::Borrowed(b"string data"),
                });

                let point_channel = Arc::new(Channel {
                    id: 1,
                    topic: "/robot/position".into(),
                    schema: Some(point_schema.clone()),
                    message_encoding: "ros1".into(),
                    metadata: BTreeMap::new(),
                });

                let string_channel = Arc::new(Channel {
                    id: 2,
                    topic: "/robot/status".into(),
                    schema: Some(string_schema.clone()),
                    message_encoding: "ros1".into(),
                    metadata: BTreeMap::new(),
                });

                // Write alternating messages
                for i in 0..6u32 {
                    if i % 2 == 0 {
                        let point_data: Vec<u8> = [i as f64, 0.0, 0.0]
                            .iter()
                            .flat_map(|f| f.to_le_bytes())
                            .collect();
                        writer
                            .write(&Message {
                                channel: point_channel.clone(),
                                sequence: i,
                                log_time: (i as u64) * 1_000_000_000,
                                publish_time: (i as u64) * 1_000_000_000,
                                data: Cow::Owned(point_data),
                            })
                            .unwrap();
                    } else {
                        let text = format!("status_{}", i);
                        let mut string_data = (text.len() as u32).to_le_bytes().to_vec();
                        string_data.extend(text.as_bytes());
                        writer
                            .write(&Message {
                                channel: string_channel.clone(),
                                sequence: i,
                                log_time: (i as u64) * 1_000_000_000,
                                publish_time: (i as u64) * 1_000_000_000,
                                data: Cow::Owned(string_data),
                            })
                            .unwrap();
                    }
                }

                writer.finish().unwrap();
            }
            buf
        }

        #[tokio::test]
        async fn test_roundtrip_single_topic() {
            let dir = tempdir().unwrap();
            let db_path = dir.path().join("test.lance");

            // Step 1: Create MCAP
            let original_mcap = create_test_mcap();

            // Step 2: Convert MCAP -> LanceDB
            let db = connect(db_path.to_str().unwrap()).execute().await.unwrap();
            let write_stats =
                convert_mcap_to_lance(Cursor::new(&original_mcap), &db, ConvertOptions::default())
                    .await
                    .unwrap();
            assert_eq!(write_stats.total_messages, 5);

            // Step 3: Convert LanceDB -> MCAP
            let mut output_buf = Vec::new();
            let read_stats = convert_lance_to_mcap(
                &db,
                Cursor::new(&mut output_buf),
                ConvertToMcapOptions::default(),
            )
            .await
            .unwrap();

            assert_eq!(read_stats.total_messages, 5);
            assert_eq!(read_stats.topics_converted, 1);

            // Step 4: Parse output MCAP and verify
            let messages: Vec<_> = mcap::MessageStream::new(&output_buf)
                .unwrap()
                .map(|m| m.unwrap())
                .collect();

            assert_eq!(messages.len(), 5);

            // Verify message contents - point data should be preserved
            for (i, msg) in messages.iter().enumerate() {
                assert_eq!(msg.channel.topic, "/points");
                assert_eq!(msg.log_time, (i as u64) * 1_000_000_000);

                // Parse point data (3 f64s)
                let data = &msg.data;
                assert_eq!(data.len(), 24);
                let x = f64::from_le_bytes(data[0..8].try_into().unwrap());
                let y = f64::from_le_bytes(data[8..16].try_into().unwrap());
                let z = f64::from_le_bytes(data[16..24].try_into().unwrap());
                assert_eq!(x, i as f64);
                assert_eq!(y, (i * 2) as f64);
                assert_eq!(z, (i * 3) as f64);
            }
        }

        #[tokio::test]
        async fn test_roundtrip_multi_topic() {
            let dir = tempdir().unwrap();
            let db_path = dir.path().join("test.lance");

            // Step 1: Create multi-topic MCAP
            let original_mcap = create_multi_topic_mcap();

            // Step 2: Convert MCAP -> LanceDB
            let db = connect(db_path.to_str().unwrap()).execute().await.unwrap();
            let write_stats =
                convert_mcap_to_lance(Cursor::new(&original_mcap), &db, ConvertOptions::default())
                    .await
                    .unwrap();
            assert_eq!(write_stats.total_messages, 6);
            assert_eq!(write_stats.tables_created, 2);

            // Step 3: Convert LanceDB -> MCAP
            let mut output_buf = Vec::new();
            let read_stats = convert_lance_to_mcap(
                &db,
                Cursor::new(&mut output_buf),
                ConvertToMcapOptions::default(),
            )
            .await
            .unwrap();

            assert_eq!(read_stats.total_messages, 6);
            assert_eq!(read_stats.topics_converted, 2);

            // Step 4: Parse output MCAP and verify
            let messages: Vec<_> = mcap::MessageStream::new(&output_buf)
                .unwrap()
                .map(|m| m.unwrap())
                .collect();

            assert_eq!(messages.len(), 6);

            // Messages should be in timestamp order
            let mut prev_time = 0u64;
            for msg in &messages {
                assert!(msg.log_time >= prev_time);
                prev_time = msg.log_time;
            }

            // Verify topic counts
            let position_count = messages
                .iter()
                .filter(|m| m.channel.topic == "/robot/position")
                .count();
            let status_count = messages
                .iter()
                .filter(|m| m.channel.topic == "/robot/status")
                .count();
            assert_eq!(position_count, 3);
            assert_eq!(status_count, 3);
        }

        #[tokio::test]
        async fn test_roundtrip_preserves_timestamps() {
            let dir = tempdir().unwrap();
            let db_path = dir.path().join("test.lance");

            let original_mcap = create_test_mcap();

            // Get original timestamps
            let original_messages: Vec<_> = mcap::MessageStream::new(&original_mcap)
                .unwrap()
                .map(|m| m.unwrap())
                .collect();

            // Convert MCAP -> LanceDB -> MCAP
            let db = connect(db_path.to_str().unwrap()).execute().await.unwrap();
            convert_mcap_to_lance(Cursor::new(&original_mcap), &db, ConvertOptions::default())
                .await
                .unwrap();

            let mut output_buf = Vec::new();
            convert_lance_to_mcap(
                &db,
                Cursor::new(&mut output_buf),
                ConvertToMcapOptions::default(),
            )
            .await
            .unwrap();

            // Get roundtripped timestamps
            let roundtripped_messages: Vec<_> = mcap::MessageStream::new(&output_buf)
                .unwrap()
                .map(|m| m.unwrap())
                .collect();

            // Verify timestamps match
            assert_eq!(original_messages.len(), roundtripped_messages.len());
            for (orig, rt) in original_messages.iter().zip(roundtripped_messages.iter()) {
                assert_eq!(orig.log_time, rt.log_time);
            }
        }
    }
}
