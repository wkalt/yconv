//! LanceDB writer for converting MCAP topics to LanceDB tables.

use std::collections::HashMap;
use std::io::Read;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use lancedb::connection::{connect, Connection, LanceFileVersion};
use lancedb::database::listing::{ListingDatabaseOptions, NewTableConfig};
use lancedb::table::{ColumnAlteration, Table};
use thiserror::Error;

use crate::arrow::{ArrowRowSink, BuilderError, RecordBatchBuilder, SinkError};
use crate::cdr::{transcode as cdr_transcode, CdrTranscodeError};
use crate::mcap::{CdrReader, CdrReaderError, ProtobufReader, ProtobufReaderError, Ros1Message, Ros1Reader, Ros1ReaderError};
use crate::protobuf::{transcode as protobuf_transcode, TranscodeError as ProtobufTranscodeError};
use crate::ros1::{CompileError, CompiledTranscoder, TranscodeError};
use crate::schema::{compare_schemas, EvolutionError, SchemaEvolution};


/// Errors that can occur while writing to LanceDB.
#[derive(Debug, Error)]
pub enum WriterError {
    #[error("LanceDB error: {0}")]
    Lance(#[from] lancedb::Error),

    #[error("MCAP reader error: {0}")]
    McapReader(#[from] Ros1ReaderError),

    #[error("MCAP protobuf reader error: {0}")]
    McapProtobufReader(#[from] ProtobufReaderError),

    #[error("MCAP CDR reader error: {0}")]
    McapCdrReader(#[from] CdrReaderError),

    #[error("protobuf transcode error: {0}")]
    ProtobufTranscode(#[from] ProtobufTranscodeError),

    #[error("CDR transcode error: {0}")]
    CdrTranscode(#[from] CdrTranscodeError),

    #[error("Arrow builder error: {0}")]
    Builder(#[from] BuilderError),

    #[error("schema evolution error: {0}")]
    Evolution(#[from] EvolutionError),

    #[error("no messages found for topic '{0}'")]
    NoMessages(String),

    #[error("table '{0}' already exists (use append or overwrite mode)")]
    TableExists(String),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("transcode error: {0}")]
    Transcode(#[from] TranscodeError),

    #[error("sink error: {0}")]
    Sink(#[from] SinkError),

    #[error("transcoder compile error: {0}")]
    Compile(#[from] CompileError),
}

/// How to handle existing tables.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WriteMode {
    /// Error if table exists.
    #[default]
    ErrorIfExists,
    /// Overwrite existing table (drop and recreate).
    Overwrite,
    /// Append to existing table, evolving schema if needed.
    Append,
}

/// Options for converting MCAP to LanceDB.
#[derive(Debug, Clone)]
pub struct ConvertOptions {
    /// Number of messages to batch before writing to LanceDB.
    pub batch_size: usize,
    /// How to handle existing tables.
    pub write_mode: WriteMode,
    /// Optional filter for topic names. If None, all topics are converted.
    pub topics: Option<Vec<String>>,
    /// Lance file format version. Defaults to V2_2.
    pub data_storage_version: LanceFileVersion,
}

impl Default for ConvertOptions {
    fn default() -> Self {
        Self {
            batch_size: 1000,
            write_mode: WriteMode::ErrorIfExists,
            topics: None,
            // Use Legacy format for compatibility with DuckDB lance extension (1.0.0)
            // V2_2 is better for new files but requires lance 2.0+ readers
            data_storage_version: LanceFileVersion::V2_2,
        }
    }
}

impl ConvertOptions {
    /// Create options for appending to existing tables.
    pub fn append() -> Self {
        Self {
            write_mode: WriteMode::Append,
            ..Default::default()
        }
    }

    /// Create options for overwriting existing tables.
    pub fn overwrite() -> Self {
        Self {
            write_mode: WriteMode::Overwrite,
            ..Default::default()
        }
    }
}

/// Create a LanceDB connection with the specified storage version.
///
/// This is the recommended way to connect when you want to control
/// the Lance file format version used for new tables.
pub async fn connect_with_options(
    uri: &str,
    data_storage_version: LanceFileVersion,
) -> Result<Connection, lancedb::Error> {
    connect(uri)
        .database_options(&ListingDatabaseOptions {
            new_table_config: NewTableConfig {
                data_storage_version: Some(data_storage_version),
                ..Default::default()
            },
            ..Default::default()
        })
        .execute()
        .await
}

/// Sanitize a topic name to be a valid table name.
///
/// LanceDB table names have restrictions, so we convert topic names like
/// "/camera/image_raw" to "camera_image_raw".
pub fn sanitize_table_name(topic: &str) -> String {
    topic
        .trim_start_matches('/')
        .replace('/', "_")
        .replace('-', "_")
}

/// Statistics about a conversion operation.
#[derive(Debug, Clone, Default)]
pub struct ConvertStats {
    /// Number of messages processed per topic.
    pub messages_per_topic: HashMap<String, usize>,
    /// Total number of messages processed.
    pub total_messages: usize,
    /// Number of tables created.
    pub tables_created: usize,
}

/// Convert an Arrow DataType to a SQL type string for LanceDB.
fn datatype_to_sql(dt: &DataType) -> String {
    match dt {
        DataType::Boolean => "boolean".to_string(),
        DataType::Int8 => "int8".to_string(),
        DataType::Int16 => "int16".to_string(),
        DataType::Int32 => "int32".to_string(),
        DataType::Int64 => "int64".to_string(),
        DataType::UInt8 => "uint8".to_string(),
        DataType::UInt16 => "uint16".to_string(),
        DataType::UInt32 => "uint32".to_string(),
        DataType::UInt64 => "uint64".to_string(),
        DataType::Float32 => "float".to_string(),
        DataType::Float64 => "double".to_string(),
        DataType::Utf8 => "string".to_string(),
        DataType::LargeUtf8 => "large_string".to_string(),
        DataType::Binary => "binary".to_string(),
        DataType::LargeBinary => "large_binary".to_string(),
        DataType::Timestamp(unit, tz) => {
            let precision = match unit {
                TimeUnit::Second => "s",
                TimeUnit::Millisecond => "ms",
                TimeUnit::Microsecond => "us",
                TimeUnit::Nanosecond => "ns",
            };
            match tz {
                Some(tz) => format!("timestamp({}, '{}')", precision, tz),
                None => format!("timestamp({})", precision),
            }
        }
        DataType::Duration(unit) => {
            let precision = match unit {
                TimeUnit::Second => "s",
                TimeUnit::Millisecond => "ms",
                TimeUnit::Microsecond => "us",
                TimeUnit::Nanosecond => "ns",
            };
            format!("duration({})", precision)
        }
        // For complex types, fallback to string representation
        _ => format!("{:?}", dt),
    }
}

/// Metadata column name for log time (nanoseconds since epoch).
pub const LOG_TIME_COLUMN: &str = "_log_time";

/// Metadata column name for publish time (nanoseconds since epoch).
pub const PUBLISH_TIME_COLUMN: &str = "_publish_time";

/// Metadata column name for sequence number.
pub const SEQUENCE_COLUMN: &str = "_sequence";

/// Metadata column name for channel ID.
pub const CHANNEL_ID_COLUMN: &str = "_channel_id";

/// Metadata table name for channels.
pub const CHANNELS_TABLE: &str = "_channels";

/// Metadata table name for schemas.
pub const SCHEMAS_TABLE: &str = "_schemas";

/// Add metadata columns to a schema.
fn add_metadata_columns(schema: &Schema) -> Schema {
    let mut fields: Vec<Field> = vec![
        // Add _log_time as first column for efficient ordering
        Field::new(LOG_TIME_COLUMN, DataType::UInt64, false),
    ];
    fields.extend(schema.fields().iter().map(|f| f.as_ref().clone()));
    Schema::new(fields)
}

/// Writer for a single topic's data to a LanceDB table.
pub struct TopicWriter {
    table: Table,
    schema: Arc<Schema>,
    builder: RecordBatchBuilder,
    /// Buffered log times for pending messages
    log_times: Vec<u64>,
    batch_size: usize,
    pending_count: usize,
}

impl TopicWriter {
    /// Create a new topic writer.
    async fn new(
        db: &Connection,
        table_name: &str,
        incoming_schema: Arc<Schema>,
        write_mode: WriteMode,
        batch_size: usize,
    ) -> Result<Self, WriterError> {
        // The message schema is the schema of the ROS1 messages (without metadata columns)
        let message_schema = incoming_schema;

        // Add metadata columns (_log_time) to the schema
        let incoming_schema_with_meta = Arc::new(add_metadata_columns(&message_schema));

        // Check if table exists
        let table_names = db.table_names().execute().await?;
        let exists = table_names.contains(&table_name.to_string());

        let (table, schema) = if exists {
            match write_mode {
                WriteMode::ErrorIfExists => {
                    return Err(WriterError::TableExists(table_name.to_string()));
                }
                WriteMode::Overwrite => {
                    db.drop_table(table_name, &[]).await?;
                    let table = db
                        .create_empty_table(table_name, incoming_schema_with_meta.clone())
                        .execute()
                        .await?;
                    (table, incoming_schema_with_meta)
                }
                WriteMode::Append => {
                    // Open existing table and compare schemas
                    let table = db.open_table(table_name).execute().await?;
                    let existing_schema = table.schema().await?;

                    // Compare schemas and get evolution plan
                    let evolution = compare_schemas(&existing_schema, &incoming_schema_with_meta)?;

                    if evolution.needs_table_alteration() {
                        // Apply schema evolution to the table
                        Self::apply_evolution(&table, &evolution).await?;
                    }

                    // Use merged schema for writing
                    let merged = evolution
                        .merged_schema
                        .unwrap_or(existing_schema);
                    (table, merged)
                }
            }
        } else {
            let table = db
                .create_empty_table(table_name, incoming_schema_with_meta.clone())
                .execute()
                .await?;
            (table, incoming_schema_with_meta)
        };

        // Builder uses message schema (without metadata columns)
        let builder = RecordBatchBuilder::new(message_schema.clone())?;

        Ok(Self {
            table,
            schema,
            builder,
            log_times: Vec::with_capacity(batch_size),
            batch_size,
            pending_count: 0,
        })
    }

    /// Apply schema evolution to an existing table.
    async fn apply_evolution(
        table: &Table,
        evolution: &SchemaEvolution,
    ) -> Result<(), WriterError> {
        // Add new columns
        for new_field in &evolution.new_fields {
            let sql_type = datatype_to_sql(new_field.data_type());
            let sql_expr = format!("CAST(NULL AS {})", sql_type);
            table
                .add_columns(
                    lancedb::table::NewColumnTransform::SqlExpressions(vec![(
                        new_field.name().to_string(),
                        sql_expr,
                    )]),
                    None,
                )
                .await?;
        }

        // Apply type widening via column alterations
        for (field_name, new_type) in &evolution.widened_fields {
            table
                .alter_columns(&[ColumnAlteration::new(field_name.clone())
                    .cast_to(new_type.clone())])
                .await?;
        }

        Ok(())
    }

    /// Add a message to the writer.
    ///
    /// Messages are buffered and written in batches for efficiency.
    async fn add(&mut self, message: &Ros1Message) -> Result<(), WriterError> {
        self.builder.append(&message.value)?;
        self.log_times.push(message.log_time);
        self.pending_count += 1;

        if self.pending_count >= self.batch_size {
            self.flush().await?;
        }

        Ok(())
    }

    /// Flush any pending messages to the table.
    async fn flush(&mut self) -> Result<(), WriterError> {
        if self.pending_count == 0 {
            return Ok(());
        }

        // Take log times (reuse capacity)
        let log_times = std::mem::take(&mut self.log_times);
        self.log_times.reserve(self.batch_size);

        // Build message batch (builder resets itself after finish)
        let message_batch = self.builder.finish()?;

        // Create log_time array
        let log_time_array: arrow::array::ArrayRef = Arc::new(
            arrow::array::UInt64Array::from(log_times)
        );

        // Combine metadata columns with message columns
        let mut columns: Vec<arrow::array::ArrayRef> = vec![log_time_array];
        columns.extend((0..message_batch.num_columns()).map(|i| message_batch.column(i).clone()));

        // Use with_match_field_names(false) to allow metadata differences between
        // schema fields and array fields (Arrow builders don't preserve field metadata).
        let options = arrow::record_batch::RecordBatchOptions::new()
            .with_match_field_names(false);
        let batch = RecordBatch::try_new_with_options(self.schema.clone(), columns, &options)?;
        self.write_batch(batch).await?;
        self.pending_count = 0;

        Ok(())
    }

    /// Write a RecordBatch to the table.
    async fn write_batch(&self, batch: RecordBatch) -> Result<(), WriterError> {
        let batches: Box<dyn arrow::record_batch::RecordBatchReader + Send> =
            Box::new(arrow::record_batch::RecordBatchIterator::new(
                vec![Ok(batch)],
                self.schema.clone(),
            ));
        self.table.add(batches).execute().await?;
        Ok(())
    }
}

/// Convert an MCAP file to LanceDB tables.
///
/// Each topic in the MCAP file becomes a separate table in LanceDB.
///
/// # Example
///
/// ```ignore
/// use std::fs::File;
/// use lancedb::connect;
///
/// let db = connect("./my_database").execute().await?;
/// let file = File::open("recording.mcap")?;
/// let stats = convert_mcap_to_lance(file, &db, ConvertOptions::default()).await?;
/// println!("Converted {} messages", stats.total_messages);
/// ```
pub async fn convert_mcap_to_lance<R: Read>(
    source: R,
    db: &Connection,
    options: ConvertOptions,
) -> Result<ConvertStats, WriterError> {
    let mut reader = Ros1Reader::new(source);
    let mut writers: HashMap<String, TopicWriter> = HashMap::new();
    let mut stats = ConvertStats::default();

    // Read all messages
    while let Some(message) = reader.next_message()? {
        // Check topic filter
        if let Some(ref topics) = options.topics {
            if !topics.contains(&message.topic) {
                continue;
            }
        }

        // Get or create writer for this topic (avoid cloning topic unless needed)
        if !writers.contains_key(&message.topic) {
            let schema = reader
                .topic_schema(&message.topic)
                .expect("schema should exist after reading message")
                .clone();

            let table_name = sanitize_table_name(&message.topic);
            let writer = TopicWriter::new(
                db,
                &table_name,
                schema,
                options.write_mode,
                options.batch_size,
            )
            .await?;

            writers.insert(message.topic.clone(), writer);
            stats.tables_created += 1;
        }

        let writer = writers.get_mut(&message.topic).unwrap();
        writer.add(&message).await?;

        *stats.messages_per_topic.entry(message.topic).or_insert(0) += 1;
        stats.total_messages += 1;
    }

    // Flush all writers
    for writer in writers.values_mut() {
        writer.flush().await?;
    }

    Ok(stats)
}

/// Query helper to list all topics (tables) in a LanceDB database.
pub async fn list_topics(db: &Connection) -> Result<Vec<String>, WriterError> {
    let names = db.table_names().execute().await?;
    Ok(names)
}

/// Create the Arrow schema for the _channels metadata table.
fn channels_table_schema() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::UInt16, false),
        Field::new("topic", DataType::Utf8, false),
        Field::new("schema_id", DataType::UInt16, false),
        Field::new("message_encoding", DataType::Utf8, false),
        Field::new("metadata", DataType::Utf8, true),  // JSON-encoded metadata
    ])
}

/// Create the Arrow schema for the _schemas metadata table.
fn schemas_table_schema() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::UInt16, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("encoding", DataType::Utf8, false),
        Field::new("data", DataType::Binary, false),
    ])
}

/// Write channel metadata to the _channels table.
async fn write_channels_table(
    db: &Connection,
    channels: &HashMap<u16, crate::mcap::ChannelInfo>,
    write_mode: WriteMode,
) -> Result<(), WriterError> {
    let schema = Arc::new(channels_table_schema());

    // Check if table exists
    let table_names = db.table_names().execute().await?;
    let exists = table_names.contains(&CHANNELS_TABLE.to_string());

    let table = if exists {
        match write_mode {
            WriteMode::ErrorIfExists => {
                return Err(WriterError::TableExists(CHANNELS_TABLE.to_string()));
            }
            WriteMode::Overwrite => {
                db.drop_table(CHANNELS_TABLE, &[]).await?;
                db.create_empty_table(CHANNELS_TABLE, schema.clone())
                    .execute()
                    .await?
            }
            WriteMode::Append => {
                // For append mode, we still overwrite the metadata tables since they're
                // meant to reflect the full state after the conversion
                db.drop_table(CHANNELS_TABLE, &[]).await?;
                db.create_empty_table(CHANNELS_TABLE, schema.clone())
                    .execute()
                    .await?
            }
        }
    } else {
        db.create_empty_table(CHANNELS_TABLE, schema.clone())
            .execute()
            .await?
    };

    // Build arrays for each column
    let mut ids = Vec::with_capacity(channels.len());
    let mut topics = Vec::with_capacity(channels.len());
    let mut schema_ids = Vec::with_capacity(channels.len());
    let mut encodings = Vec::with_capacity(channels.len());
    let mut metadatas = Vec::with_capacity(channels.len());

    for channel in channels.values() {
        ids.push(channel.id);
        topics.push(channel.topic.clone());
        schema_ids.push(channel.schema_id);
        encodings.push(channel.message_encoding.clone());
        // Serialize metadata as JSON
        let metadata_json = serde_json_lite(&channel.metadata);
        metadatas.push(Some(metadata_json));
    }

    let id_array: arrow::array::ArrayRef = Arc::new(arrow::array::UInt16Array::from(ids));
    let topic_array: arrow::array::ArrayRef = Arc::new(arrow::array::StringArray::from(topics));
    let schema_id_array: arrow::array::ArrayRef = Arc::new(arrow::array::UInt16Array::from(schema_ids));
    let encoding_array: arrow::array::ArrayRef = Arc::new(arrow::array::StringArray::from(encodings));
    let metadata_array: arrow::array::ArrayRef = Arc::new(arrow::array::StringArray::from(metadatas));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![id_array, topic_array, schema_id_array, encoding_array, metadata_array],
    )?;

    let batches: Box<dyn arrow::record_batch::RecordBatchReader + Send> =
        Box::new(arrow::record_batch::RecordBatchIterator::new(
            vec![Ok(batch)],
            schema,
        ));
    table.add(batches).execute().await?;

    Ok(())
}

/// Write schema metadata to the _schemas table.
async fn write_schemas_table(
    db: &Connection,
    schemas: &HashMap<u16, crate::mcap::SchemaInfo>,
    write_mode: WriteMode,
) -> Result<(), WriterError> {
    let schema = Arc::new(schemas_table_schema());

    // Check if table exists
    let table_names = db.table_names().execute().await?;
    let exists = table_names.contains(&SCHEMAS_TABLE.to_string());

    let table = if exists {
        match write_mode {
            WriteMode::ErrorIfExists => {
                return Err(WriterError::TableExists(SCHEMAS_TABLE.to_string()));
            }
            WriteMode::Overwrite => {
                db.drop_table(SCHEMAS_TABLE, &[]).await?;
                db.create_empty_table(SCHEMAS_TABLE, schema.clone())
                    .execute()
                    .await?
            }
            WriteMode::Append => {
                // For append mode, we still overwrite the metadata tables
                db.drop_table(SCHEMAS_TABLE, &[]).await?;
                db.create_empty_table(SCHEMAS_TABLE, schema.clone())
                    .execute()
                    .await?
            }
        }
    } else {
        db.create_empty_table(SCHEMAS_TABLE, schema.clone())
            .execute()
            .await?
    };

    // Build arrays for each column
    let mut ids = Vec::with_capacity(schemas.len());
    let mut names = Vec::with_capacity(schemas.len());
    let mut encodings = Vec::with_capacity(schemas.len());
    let mut datas = Vec::with_capacity(schemas.len());

    for schema_info in schemas.values() {
        ids.push(schema_info.id);
        names.push(schema_info.name.clone());
        encodings.push(schema_info.encoding.clone());
        datas.push(schema_info.data.clone());
    }

    let id_array: arrow::array::ArrayRef = Arc::new(arrow::array::UInt16Array::from(ids));
    let name_array: arrow::array::ArrayRef = Arc::new(arrow::array::StringArray::from(names));
    let encoding_array: arrow::array::ArrayRef = Arc::new(arrow::array::StringArray::from(encodings));
    let data_array: arrow::array::ArrayRef = Arc::new(arrow::array::BinaryArray::from_vec(
        datas.iter().map(|d| d.as_slice()).collect(),
    ));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![id_array, name_array, encoding_array, data_array],
    )?;

    let batches: Box<dyn arrow::record_batch::RecordBatchReader + Send> =
        Box::new(arrow::record_batch::RecordBatchIterator::new(
            vec![Ok(batch)],
            schema,
        ));
    table.add(batches).execute().await?;

    Ok(())
}

/// Simple JSON serialization for metadata (avoids serde_json dependency).
fn serde_json_lite(metadata: &HashMap<String, String>) -> String {
    if metadata.is_empty() {
        return "{}".to_string();
    }

    let pairs: Vec<String> = metadata
        .iter()
        .map(|(k, v)| {
            format!(
                "\"{}\":\"{}\"",
                k.replace('\\', "\\\\").replace('"', "\\\""),
                v.replace('\\', "\\\\").replace('"', "\\\"")
            )
        })
        .collect();

    format!("{{{}}}", pairs.join(","))
}

/// Per-topic writer state for the fast transcoding path.
struct FastTopicWriter {
    table: Table,
    schema: Arc<Schema>,
    sink: ArrowRowSink,
    /// Pre-compiled transcoder for zero-lookup message transcoding (ROS1 only)
    transcoder: Option<CompiledTranscoder>,
    /// Buffered log times for pending messages
    log_times: Vec<u64>,
    /// Buffered publish times for pending messages
    publish_times: Vec<u64>,
    /// Buffered sequence numbers for pending messages
    sequences: Vec<u32>,
    /// Buffered channel IDs for pending messages
    channel_ids: Vec<u16>,
    pending_count: usize,
    batch_size: usize,
    /// Background write tasks for pipelining
    pending_writes: Vec<tokio::task::JoinHandle<Result<(), WriterError>>>,
    /// Maximum number of concurrent writes (for backpressure)
    max_pending_writes: usize,
}

impl FastTopicWriter {
    async fn new(
        db: &Connection,
        table_name: &str,
        message_schema: Arc<Schema>,
        transcoder: Option<CompiledTranscoder>,
        write_mode: WriteMode,
        batch_size: usize,
    ) -> Result<Self, WriterError> {
        // Create full schema with metadata columns
        let mut fields: Vec<Arc<Field>> = vec![
            Arc::new(Field::new(
                LOG_TIME_COLUMN,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            )),
            Arc::new(Field::new(
                PUBLISH_TIME_COLUMN,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            )),
            Arc::new(Field::new(
                SEQUENCE_COLUMN,
                DataType::UInt32,
                false,
            )),
            Arc::new(Field::new(
                CHANNEL_ID_COLUMN,
                DataType::UInt16,
                false,
            )),
        ];
        fields.extend(message_schema.fields().iter().cloned());
        let schema = Arc::new(Schema::new(fields));

        // Check if table exists and handle according to write mode
        let existing_tables = db.table_names().execute().await?;
        let table_exists = existing_tables.contains(&table_name.to_string());

        let table = if table_exists {
            match write_mode {
                WriteMode::ErrorIfExists => {
                    return Err(WriterError::TableExists(table_name.to_string()));
                }
                WriteMode::Overwrite => {
                    db.drop_table(table_name, &[]).await?;
                    db.create_empty_table(table_name, schema.clone())
                        .execute()
                        .await?
                }
                WriteMode::Append => db.open_table(table_name).execute().await?,
            }
        } else {
            db.create_empty_table(table_name, schema.clone())
                .execute()
                .await?
        };

        let sink = ArrowRowSink::new(message_schema.clone())?;

        Ok(Self {
            table,
            schema,
            sink,
            transcoder,
            log_times: Vec::with_capacity(batch_size),
            publish_times: Vec::with_capacity(batch_size),
            sequences: Vec::with_capacity(batch_size),
            channel_ids: Vec::with_capacity(batch_size),
            pending_count: 0,
            batch_size,
            pending_writes: Vec::new(),
            max_pending_writes: 2, // Allow 1 write in flight while transcoding next batch
        })
    }

    /// Flush current batch to Lance, spawning as background task for pipelining.
    async fn flush(&mut self) -> Result<(), WriterError> {
        if self.pending_count == 0 {
            return Ok(());
        }

        // Apply backpressure: if too many writes pending, wait for some to complete
        while self.pending_writes.len() >= self.max_pending_writes {
            self.wait_one().await?;
        }

        // Take metadata buffers and reserve capacity for next batch
        let log_times = std::mem::take(&mut self.log_times);
        self.log_times.reserve(self.batch_size);
        let publish_times = std::mem::take(&mut self.publish_times);
        self.publish_times.reserve(self.batch_size);
        let sequences = std::mem::take(&mut self.sequences);
        self.sequences.reserve(self.batch_size);
        let channel_ids = std::mem::take(&mut self.channel_ids);
        self.channel_ids.reserve(self.batch_size);

        // Finish the sink to get message columns
        let message_batch = self.sink.finish()?;

        // Create metadata arrays (convert u64 to i64 for timestamps)
        let log_times_i64: Vec<i64> = log_times.into_iter().map(|t| t as i64).collect();
        let log_time_array: arrow::array::ArrayRef = Arc::new(
            arrow::array::TimestampNanosecondArray::from(log_times_i64),
        );
        let publish_times_i64: Vec<i64> = publish_times.into_iter().map(|t| t as i64).collect();
        let publish_time_array: arrow::array::ArrayRef = Arc::new(
            arrow::array::TimestampNanosecondArray::from(publish_times_i64),
        );
        let sequence_array: arrow::array::ArrayRef = Arc::new(
            arrow::array::UInt32Array::from(sequences),
        );
        let channel_id_array: arrow::array::ArrayRef = Arc::new(
            arrow::array::UInt16Array::from(channel_ids),
        );

        // Combine metadata columns with message columns
        let mut columns: Vec<arrow::array::ArrayRef> = vec![
            log_time_array,
            publish_time_array,
            sequence_array,
            channel_id_array,
        ];
        columns.extend((0..message_batch.num_columns()).map(|i| message_batch.column(i).clone()));

        // Use with_match_field_names(false) to allow metadata differences between
        // schema fields and array fields (Arrow builders don't preserve field metadata).
        let options = arrow::record_batch::RecordBatchOptions::new()
            .with_match_field_names(false);
        let batch = RecordBatch::try_new_with_options(self.schema.clone(), columns, &options)?;
        let schema = self.schema.clone();
        let table = self.table.clone();

        // Spawn write as background task
        let handle = tokio::spawn(async move {
            let batches: Box<dyn arrow::record_batch::RecordBatchReader + Send> =
                Box::new(arrow::record_batch::RecordBatchIterator::new(
                    vec![Ok(batch)],
                    schema,
                ));
            table.add(batches).execute().await?;
            Ok(())
        });
        self.pending_writes.push(handle);
        self.pending_count = 0;

        Ok(())
    }

    /// Wait for one pending write to complete.
    async fn wait_one(&mut self) -> Result<(), WriterError> {
        if let Some(handle) = self.pending_writes.pop() {
            match handle.await {
                Ok(result) => result?,
                Err(e) => return Err(WriterError::Lance(lancedb::Error::Runtime {
                    message: format!("write task panicked: {}", e),
                })),
            }
        }
        Ok(())
    }

    /// Wait for all pending writes to complete.
    async fn finish(&mut self) -> Result<(), WriterError> {
        // Flush any remaining data
        self.flush().await?;

        // Wait for all pending writes
        for handle in self.pending_writes.drain(..) {
            match handle.await {
                Ok(result) => result?,
                Err(e) => return Err(WriterError::Lance(lancedb::Error::Runtime {
                    message: format!("write task panicked: {}", e),
                })),
            }
        }
        Ok(())
    }
}

/// Convert MCAP file to LanceDB using fast direct transcoding.
///
/// This is an optimized version that skips the intermediate Value representation,
/// transcoding ROS1 binary data directly to Arrow arrays.
pub async fn convert_mcap_to_lance_fast<R: Read>(
    source: R,
    db: &Connection,
    options: ConvertOptions,
) -> Result<ConvertStats, WriterError> {
    let mut reader = Ros1Reader::new(source);
    let mut writers: HashMap<String, FastTopicWriter> = HashMap::new();
    let mut stats = ConvertStats::default();

    // Read all messages using the raw message API
    while let Some(raw_msg) = reader.next_raw_message()? {
        // Check topic filter
        if let Some(ref topics) = options.topics {
            if !topics.contains(&raw_msg.topic) {
                continue;
            }
        }

        // Get or create writer for this topic
        if !writers.contains_key(&raw_msg.topic) {
            let schema = reader
                .topic_schema(&raw_msg.topic)
                .expect("schema should exist after reading message")
                .clone();

            // Compile the transcoder once per topic - resolves all nested types upfront
            let definition = reader.definition(raw_msg.schema_id).unwrap();
            let registry = reader.registry(raw_msg.schema_id).unwrap();
            let transcoder = CompiledTranscoder::compile(definition, registry)?;

            let table_name = sanitize_table_name(&raw_msg.topic);
            let writer = FastTopicWriter::new(
                db,
                &table_name,
                schema,
                Some(transcoder),
                options.write_mode,
                options.batch_size,
            )
            .await?;

            writers.insert(raw_msg.topic.clone(), writer);
            stats.tables_created += 1;
        }

        let writer = writers.get_mut(&raw_msg.topic).unwrap();

        // Transcode using pre-compiled transcoder (zero lookups per message)
        writer.transcoder.as_ref().unwrap().transcode(&raw_msg.data, &mut writer.sink)?;

        writer.log_times.push(raw_msg.log_time);
        writer.publish_times.push(raw_msg.publish_time);
        writer.sequences.push(raw_msg.sequence);
        writer.channel_ids.push(raw_msg.channel_id);
        writer.pending_count += 1;

        // Flush if batch is full OR if we're approaching i32 offset limit
        // (List arrays use i32 offsets, max ~2 billion bytes)
        const MAX_BATCH_BYTES: usize = 1_500_000_000; // 1.5GB, safe margin below i32::MAX
        let should_flush = writer.pending_count >= writer.batch_size
            || writer.sink.bytes_written() >= MAX_BATCH_BYTES;
        if should_flush {
            if let Err(e) = writer.flush().await {
                eprintln!("Error flushing topic {}: {:?}", raw_msg.topic, e);
                return Err(e);
            }
        }

        // Use get_mut to avoid cloning the topic string on every message
        if let Some(count) = stats.messages_per_topic.get_mut(&raw_msg.topic) {
            *count += 1;
        } else {
            stats.messages_per_topic.insert(raw_msg.topic, 1);
        }
        stats.total_messages += 1;
    }

    // Finish all writers (flush remaining + wait for pending writes)
    for (topic, writer) in writers.iter_mut() {
        if let Err(e) = writer.finish().await {
            eprintln!("Error finishing topic {}: {:?}", topic, e);
            return Err(e);
        }
    }

    // Write metadata tables
    write_channels_table(
        db,
        reader.channels(),
        options.write_mode,
    )
    .await?;

    write_schemas_table(
        db,
        reader.schemas(),
        options.write_mode,
    )
    .await?;

    Ok(stats)
}

/// Convert protobuf-encoded MCAP file to LanceDB using fast direct transcoding.
///
/// This is similar to `convert_mcap_to_lance_fast` but handles protobuf encoding
/// instead of ROS1.
pub async fn convert_protobuf_mcap_to_lance<R: Read>(
    source: R,
    db: &Connection,
    options: ConvertOptions,
) -> Result<ConvertStats, WriterError> {
    let mut reader = ProtobufReader::new(source);
    let mut writers: HashMap<String, FastTopicWriter> = HashMap::new();
    let mut topic_schema_ids: HashMap<String, u16> = HashMap::new();
    let mut stats = ConvertStats::default();

    // Read all messages using the raw message API
    while let Some(raw_msg) = reader.next_raw_message()? {
        // Check topic filter
        if let Some(ref topics) = options.topics {
            if !topics.contains(&raw_msg.topic) {
                continue;
            }
        }

        // Get or create writer for this topic
        if !writers.contains_key(&raw_msg.topic) {
            let schema = reader
                .topic_schema(&raw_msg.topic)
                .expect("schema should exist after reading message")
                .clone();

            let table_name = sanitize_table_name(&raw_msg.topic);
            let writer = FastTopicWriter::new(
                db,
                &table_name,
                schema,
                None, // Protobuf uses its own transcoding
                options.write_mode,
                options.batch_size,
            )
            .await?;

            writers.insert(raw_msg.topic.clone(), writer);
            topic_schema_ids.insert(raw_msg.topic.clone(), raw_msg.schema_id);
            stats.tables_created += 1;
        }

        let schema_id = topic_schema_ids[&raw_msg.topic];
        let message_desc = reader.message_descriptor(schema_id).unwrap();

        let writer = writers.get_mut(&raw_msg.topic).unwrap();

        // Transcode directly to Arrow sink
        protobuf_transcode(&raw_msg.data, message_desc, &mut writer.sink)?;

        writer.log_times.push(raw_msg.log_time);
        writer.publish_times.push(raw_msg.publish_time);
        writer.sequences.push(raw_msg.sequence);
        writer.channel_ids.push(raw_msg.channel_id);
        writer.pending_count += 1;

        // Flush if batch is full OR if we're approaching i32 offset limit
        // (List arrays use i32 offsets, max ~2 billion bytes)
        const MAX_BATCH_BYTES: usize = 1_500_000_000; // 1.5GB, safe margin below i32::MAX
        let should_flush = writer.pending_count >= writer.batch_size
            || writer.sink.bytes_written() >= MAX_BATCH_BYTES;
        if should_flush {
            if let Err(e) = writer.flush().await {
                eprintln!("Error flushing topic {}: {:?}", raw_msg.topic, e);
                return Err(e);
            }
        }

        // Track stats
        if let Some(count) = stats.messages_per_topic.get_mut(&raw_msg.topic) {
            *count += 1;
        } else {
            stats.messages_per_topic.insert(raw_msg.topic, 1);
        }
        stats.total_messages += 1;
    }

    // Finish all writers
    for (topic, writer) in writers.iter_mut() {
        if let Err(e) = writer.finish().await {
            eprintln!("Error finishing topic {}: {:?}", topic, e);
            return Err(e);
        }
    }

    // Write metadata tables
    write_channels_table(
        db,
        reader.channels(),
        options.write_mode,
    )
    .await?;

    write_schemas_table(
        db,
        reader.schemas(),
        options.write_mode,
    )
    .await?;

    Ok(stats)
}

/// Convert CDR-encoded (ROS2) MCAP file to LanceDB using fast direct transcoding.
///
/// This is similar to `convert_mcap_to_lance_fast` but handles CDR/ROS2 encoding.
pub async fn convert_cdr_mcap_to_lance<R: Read>(
    source: R,
    db: &Connection,
    options: ConvertOptions,
) -> Result<ConvertStats, WriterError> {
    let mut reader = CdrReader::new(source);
    let mut writers: HashMap<String, FastTopicWriter> = HashMap::new();
    let mut topic_schema_ids: HashMap<String, u16> = HashMap::new();
    let mut stats = ConvertStats::default();

    // Read all messages using the raw message API
    while let Some(raw_msg) = reader.next_raw_message()? {
        // Check topic filter
        if let Some(ref topics) = options.topics {
            if !topics.contains(&raw_msg.topic) {
                continue;
            }
        }

        // Get or create writer for this topic
        if !writers.contains_key(&raw_msg.topic) {
            let schema = reader
                .topic_schema(&raw_msg.topic)
                .expect("schema should exist after reading message")
                .clone();

            let table_name = sanitize_table_name(&raw_msg.topic);
            let writer = FastTopicWriter::new(
                db,
                &table_name,
                schema,
                None, // CDR uses its own transcoding
                options.write_mode,
                options.batch_size,
            )
            .await?;

            writers.insert(raw_msg.topic.clone(), writer);
            topic_schema_ids.insert(raw_msg.topic.clone(), raw_msg.schema_id);
            stats.tables_created += 1;
        }

        let schema_id = topic_schema_ids[&raw_msg.topic];
        let definition = reader.definition(schema_id).unwrap();
        let registry = reader.registry(schema_id).unwrap();

        let writer = writers.get_mut(&raw_msg.topic).unwrap();

        // Transcode CDR data directly to Arrow sink
        if let Err(e) = cdr_transcode(&raw_msg.data, definition, registry, &mut writer.sink) {
            // Provide helpful error message for malformed messages
            eprintln!(
                "Error: malformed CDR message on topic '{}' (seq {}): {}",
                raw_msg.topic, raw_msg.sequence, e
            );
            eprintln!(
                "  Hint: This topic may contain corrupted data. Try excluding it."
            );
            return Err(e.into());
        }

        writer.log_times.push(raw_msg.log_time);
        writer.publish_times.push(raw_msg.publish_time);
        writer.sequences.push(raw_msg.sequence);
        writer.channel_ids.push(raw_msg.channel_id);
        writer.pending_count += 1;

        // Flush if batch is full OR if we're approaching i32 offset limit
        const MAX_BATCH_BYTES: usize = 1_500_000_000;
        let should_flush = writer.pending_count >= writer.batch_size
            || writer.sink.bytes_written() >= MAX_BATCH_BYTES;
        if should_flush {
            if let Err(e) = writer.flush().await {
                eprintln!("Error flushing topic {}: {:?}", raw_msg.topic, e);
                return Err(e);
            }
        }

        // Track stats
        if let Some(count) = stats.messages_per_topic.get_mut(&raw_msg.topic) {
            *count += 1;
        } else {
            stats.messages_per_topic.insert(raw_msg.topic, 1);
        }
        stats.total_messages += 1;
    }

    // Finish all writers
    for (topic, writer) in writers.iter_mut() {
        if let Err(e) = writer.finish().await {
            eprintln!("Error finishing topic {}: {:?}", topic, e);
            return Err(e);
        }
    }

    // Write metadata tables
    write_channels_table(
        db,
        reader.channels(),
        options.write_mode,
    )
    .await?;

    write_schemas_table(
        db,
        reader.schemas(),
        options.write_mode,
    )
    .await?;

    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;
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

    mod sanitize {
        use super::*;

        #[test]
        fn test_sanitize_table_name() {
            assert_eq!(sanitize_table_name("/camera/image_raw"), "camera_image_raw");
            assert_eq!(sanitize_table_name("/robot/pose"), "robot_pose");
            assert_eq!(sanitize_table_name("simple"), "simple");
            assert_eq!(sanitize_table_name("/a/b/c/d"), "a_b_c_d");
            assert_eq!(sanitize_table_name("/my-topic"), "my_topic");
        }
    }

    mod conversion {
        use super::*;

        #[tokio::test]
        async fn test_convert_single_topic() {
            let dir = tempdir().unwrap();
            let db_path = dir.path().join("test.lance");

            let db = connect(db_path.to_str().unwrap())
                .execute()
                .await
                .unwrap();

            let mcap_data = create_test_mcap();
            let stats = convert_mcap_to_lance(Cursor::new(mcap_data), &db, ConvertOptions::default())
                .await
                .unwrap();

            assert_eq!(stats.total_messages, 5);
            assert_eq!(stats.tables_created, 1);
            assert_eq!(stats.messages_per_topic.get("/points"), Some(&5));

            // Verify table was created
            let tables = list_topics(&db).await.unwrap();
            assert!(tables.contains(&"points".to_string()));

            // Query the table
            let table = db.open_table("points").execute().await.unwrap();
            let count = table.count_rows(None).await.unwrap();
            assert_eq!(count, 5);
        }

        #[tokio::test]
        async fn test_convert_multi_topic() {
            let dir = tempdir().unwrap();
            let db_path = dir.path().join("test.lance");

            let db = connect(db_path.to_str().unwrap())
                .execute()
                .await
                .unwrap();

            let mcap_data = create_multi_topic_mcap();
            let stats = convert_mcap_to_lance(Cursor::new(mcap_data), &db, ConvertOptions::default())
                .await
                .unwrap();

            assert_eq!(stats.total_messages, 6);
            assert_eq!(stats.tables_created, 2);
            assert_eq!(stats.messages_per_topic.get("/robot/position"), Some(&3));
            assert_eq!(stats.messages_per_topic.get("/robot/status"), Some(&3));

            // Verify tables
            let tables = list_topics(&db).await.unwrap();
            assert!(tables.contains(&"robot_position".to_string()));
            assert!(tables.contains(&"robot_status".to_string()));
        }

        #[tokio::test]
        async fn test_convert_with_topic_filter() {
            let dir = tempdir().unwrap();
            let db_path = dir.path().join("test.lance");

            let db = connect(db_path.to_str().unwrap())
                .execute()
                .await
                .unwrap();

            let mcap_data = create_multi_topic_mcap();
            let options = ConvertOptions {
                topics: Some(vec!["/robot/position".to_string()]),
                ..Default::default()
            };

            let stats = convert_mcap_to_lance(Cursor::new(mcap_data), &db, options)
                .await
                .unwrap();

            assert_eq!(stats.total_messages, 3);
            assert_eq!(stats.tables_created, 1);

            let tables = list_topics(&db).await.unwrap();
            assert!(tables.contains(&"robot_position".to_string()));
            assert!(!tables.contains(&"robot_status".to_string()));
        }

        #[tokio::test]
        async fn test_overwrite_existing() {
            let dir = tempdir().unwrap();
            let db_path = dir.path().join("test.lance");

            let db = connect(db_path.to_str().unwrap())
                .execute()
                .await
                .unwrap();

            let mcap_data = create_test_mcap();

            // First conversion
            convert_mcap_to_lance(
                Cursor::new(mcap_data.clone()),
                &db,
                ConvertOptions::overwrite(),
            )
            .await
            .unwrap();

            // Second conversion with overwrite
            let stats = convert_mcap_to_lance(
                Cursor::new(mcap_data),
                &db,
                ConvertOptions::overwrite(),
            )
            .await
            .unwrap();

            assert_eq!(stats.total_messages, 5);

            // Table should have 5 rows (not 10)
            let table = db.open_table("points").execute().await.unwrap();
            let count = table.count_rows(None).await.unwrap();
            assert_eq!(count, 5);
        }

        #[tokio::test]
        async fn test_error_on_existing_table() {
            let dir = tempdir().unwrap();
            let db_path = dir.path().join("test.lance");

            let db = connect(db_path.to_str().unwrap())
                .execute()
                .await
                .unwrap();

            let mcap_data = create_test_mcap();

            // First conversion
            convert_mcap_to_lance(
                Cursor::new(mcap_data.clone()),
                &db,
                ConvertOptions::default(),
            )
            .await
            .unwrap();

            // Second conversion should fail
            let result = convert_mcap_to_lance(
                Cursor::new(mcap_data),
                &db,
                ConvertOptions::default(),
            )
            .await;

            assert!(matches!(result, Err(WriterError::TableExists(_))));
        }

        #[tokio::test]
        async fn test_batch_size() {
            let dir = tempdir().unwrap();
            let db_path = dir.path().join("test.lance");

            let db = connect(db_path.to_str().unwrap())
                .execute()
                .await
                .unwrap();

            let mcap_data = create_test_mcap();
            let options = ConvertOptions {
                batch_size: 2, // Small batch size
                ..Default::default()
            };

            let stats = convert_mcap_to_lance(Cursor::new(mcap_data), &db, options)
                .await
                .unwrap();

            assert_eq!(stats.total_messages, 5);

            // Verify all rows made it
            let table = db.open_table("points").execute().await.unwrap();
            let count = table.count_rows(None).await.unwrap();
            assert_eq!(count, 5);
        }

        #[tokio::test]
        async fn test_fast_convert_simple() {
            let dir = tempdir().unwrap();
            let db_path = dir.path().join("test.lance");

            let db = connect(db_path.to_str().unwrap())
                .execute()
                .await
                .unwrap();

            let mcap_data = create_test_mcap();
            let stats = convert_mcap_to_lance_fast(Cursor::new(mcap_data), &db, ConvertOptions::default())
                .await
                .unwrap();

            assert_eq!(stats.total_messages, 5);
            assert_eq!(stats.tables_created, 1);
            assert_eq!(stats.messages_per_topic.get("/points"), Some(&5));

            // Verify table was created
            let tables = list_topics(&db).await.unwrap();
            assert!(tables.contains(&"points".to_string()));

            // Query the table
            let table = db.open_table("points").execute().await.unwrap();
            let count = table.count_rows(None).await.unwrap();
            assert_eq!(count, 5);
        }

        #[tokio::test]
        async fn test_fast_convert_with_bool() {
            // Test with a message that has bool fields (like PointCloud2's is_bigendian)
            let dir = tempdir().unwrap();
            let db_path = dir.path().join("test.lance");

            let db = connect(db_path.to_str().unwrap())
                .execute()
                .await
                .unwrap();

            // Create MCAP with a simple bool-containing message
            let mut buf = Vec::new();
            {
                let mut writer = Writer::new(Cursor::new(&mut buf)).unwrap();

                // Simplified PointCloud2-like message with bool
                let schema = Arc::new(Schema {
                    id: 1,
                    name: "test_msgs/BoolMessage".into(),
                    encoding: "ros1msg".into(),
                    data: Cow::Borrowed(b"uint32 height\nuint32 width\nbool is_bigendian\nbool is_dense"),
                });

                let channel = Arc::new(Channel {
                    id: 1,
                    topic: "/cloud".into(),
                    schema: Some(schema.clone()),
                    message_encoding: "ros1".into(),
                    metadata: BTreeMap::new(),
                });

                // Build message data: height(4) + width(4) + is_bigendian(1) + is_dense(1)
                let mut data = Vec::new();
                data.extend_from_slice(&100u32.to_le_bytes()); // height
                data.extend_from_slice(&200u32.to_le_bytes()); // width
                data.push(0); // is_bigendian = false
                data.push(1); // is_dense = true

                let message = Message {
                    channel: channel.clone(),
                    sequence: 0,
                    log_time: 1_000_000_000,
                    publish_time: 1_000_000_000,
                    data: Cow::Owned(data),
                };
                writer.write(&message).unwrap();
                writer.finish().unwrap();
            }

            let stats = convert_mcap_to_lance_fast(Cursor::new(buf), &db, ConvertOptions::default())
                .await
                .unwrap();

            assert_eq!(stats.total_messages, 1);
        }

        #[tokio::test]
        async fn test_fast_convert_with_nested_list() {
            // Test with PointCloud2-like message with PointField[]
            let dir = tempdir().unwrap();
            let db_path = dir.path().join("test.lance");

            let db = connect(db_path.to_str().unwrap())
                .execute()
                .await
                .unwrap();

            let mut buf = Vec::new();
            {
                let mut writer = Writer::new(Cursor::new(&mut buf)).unwrap();

                // PointCloud2-like message with fields array
                let schema_data = b"uint32 height\nuint32 width\nPointField[] fields\nbool is_bigendian\n================================================================================\nMSG: sensor_msgs/PointField\nstring name\nuint32 offset\nuint8 datatype\nuint32 count";
                let schema = Arc::new(Schema {
                    id: 1,
                    name: "sensor_msgs/PointCloud2".into(),
                    encoding: "ros1msg".into(),
                    data: Cow::Borrowed(schema_data),
                });

                let channel = Arc::new(Channel {
                    id: 1,
                    topic: "/cloud".into(),
                    schema: Some(schema.clone()),
                    message_encoding: "ros1".into(),
                    metadata: BTreeMap::new(),
                });

                // Build message data
                let mut data = Vec::new();
                data.extend_from_slice(&1u32.to_le_bytes()); // height
                data.extend_from_slice(&100u32.to_le_bytes()); // width

                // fields array with 2 PointFields
                data.extend_from_slice(&2u32.to_le_bytes()); // array length

                // PointField 1: name="x", offset=0, datatype=7 (FLOAT32), count=1
                data.extend_from_slice(&1u32.to_le_bytes()); // name length
                data.push(b'x');
                data.extend_from_slice(&0u32.to_le_bytes()); // offset
                data.push(7); // datatype
                data.extend_from_slice(&1u32.to_le_bytes()); // count

                // PointField 2: name="y", offset=4, datatype=7, count=1
                data.extend_from_slice(&1u32.to_le_bytes()); // name length
                data.push(b'y');
                data.extend_from_slice(&4u32.to_le_bytes()); // offset
                data.push(7); // datatype
                data.extend_from_slice(&1u32.to_le_bytes()); // count

                data.push(0); // is_bigendian = false

                let message = Message {
                    channel: channel.clone(),
                    sequence: 0,
                    log_time: 1_000_000_000,
                    publish_time: 1_000_000_000,
                    data: Cow::Owned(data),
                };
                writer.write(&message).unwrap();
                writer.finish().unwrap();
            }

            let stats = convert_mcap_to_lance_fast(Cursor::new(buf), &db, ConvertOptions::default())
                .await
                .unwrap();

            assert_eq!(stats.total_messages, 1);
        }
    }

    mod query {
        use super::*;
        use futures::TryStreamExt;
        use lancedb::query::ExecutableQuery;

        #[tokio::test]
        async fn test_query_data() {
            let dir = tempdir().unwrap();
            let db_path = dir.path().join("test.lance");

            let db = connect(db_path.to_str().unwrap())
                .execute()
                .await
                .unwrap();

            let mcap_data = create_test_mcap();
            convert_mcap_to_lance(Cursor::new(mcap_data), &db, ConvertOptions::default())
                .await
                .unwrap();

            let table = db.open_table("points").execute().await.unwrap();

            // Query all data
            let batches: Vec<RecordBatch> = table
                .query()
                .execute()
                .await
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap();

            assert!(!batches.is_empty());

            let total_rows: usize = batches.iter().map(|b: &RecordBatch| b.num_rows()).sum();
            assert_eq!(total_rows, 5);
        }

        #[tokio::test]
        async fn test_schema_preserved() {
            let dir = tempdir().unwrap();
            let db_path = dir.path().join("test.lance");

            let db = connect(db_path.to_str().unwrap())
                .execute()
                .await
                .unwrap();

            let mcap_data = create_test_mcap();
            convert_mcap_to_lance(Cursor::new(mcap_data), &db, ConvertOptions::default())
                .await
                .unwrap();

            let table = db.open_table("points").execute().await.unwrap();
            let schema = table.schema().await.unwrap();

            // Check schema has the expected fields
            assert!(schema.field_with_name("x").is_ok());
            assert!(schema.field_with_name("y").is_ok());
            assert!(schema.field_with_name("z").is_ok());
        }
    }

    mod append {
        use super::*;

        #[tokio::test]
        async fn test_append_same_schema() {
            let dir = tempdir().unwrap();
            let db_path = dir.path().join("test.lance");

            let db = connect(db_path.to_str().unwrap())
                .execute()
                .await
                .unwrap();

            let mcap_data = create_test_mcap();

            // First conversion
            convert_mcap_to_lance(
                Cursor::new(mcap_data.clone()),
                &db,
                ConvertOptions::default(),
            )
            .await
            .unwrap();

            // Second conversion with append
            let stats = convert_mcap_to_lance(
                Cursor::new(mcap_data),
                &db,
                ConvertOptions::append(),
            )
            .await
            .unwrap();

            assert_eq!(stats.total_messages, 5);

            // Table should have 10 rows (5 + 5)
            let table = db.open_table("points").execute().await.unwrap();
            let count = table.count_rows(None).await.unwrap();
            assert_eq!(count, 10);
        }

        #[tokio::test]
        async fn test_append_to_nonexistent_table() {
            let dir = tempdir().unwrap();
            let db_path = dir.path().join("test.lance");

            let db = connect(db_path.to_str().unwrap())
                .execute()
                .await
                .unwrap();

            let mcap_data = create_test_mcap();

            // Append to a table that doesn't exist should create it
            let stats = convert_mcap_to_lance(
                Cursor::new(mcap_data),
                &db,
                ConvertOptions::append(),
            )
            .await
            .unwrap();

            assert_eq!(stats.total_messages, 5);
            assert_eq!(stats.tables_created, 1);

            let table = db.open_table("points").execute().await.unwrap();
            let count = table.count_rows(None).await.unwrap();
            assert_eq!(count, 5);
        }

        fn create_point_with_w_mcap() -> Vec<u8> {
            // Creates points with x, y, z, w (new field)
            let mut buf = Vec::new();
            {
                let mut writer = Writer::new(Cursor::new(&mut buf)).unwrap();

                let schema = Arc::new(Schema {
                    id: 1,
                    name: "geometry_msgs/Point4".into(),
                    encoding: "ros1msg".into(),
                    data: Cow::Borrowed(b"float64 x\nfloat64 y\nfloat64 z\nfloat64 w"),
                });

                let channel = Arc::new(Channel {
                    id: 1,
                    topic: "/points".into(),
                    schema: Some(schema.clone()),
                    message_encoding: "ros1".into(),
                    metadata: BTreeMap::new(),
                });

                // Write 3 point messages with w field
                for i in 0..3u32 {
                    let point_data: Vec<u8> = [i as f64, 0.0, 0.0, 1.0]
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

        #[tokio::test]
        async fn test_append_with_new_field() {
            let dir = tempdir().unwrap();
            let db_path = dir.path().join("test.lance");

            let db = connect(db_path.to_str().unwrap())
                .execute()
                .await
                .unwrap();

            // First: create table with x, y, z
            let mcap_xyz = create_test_mcap();
            convert_mcap_to_lance(
                Cursor::new(mcap_xyz),
                &db,
                ConvertOptions::default(),
            )
            .await
            .unwrap();

            // Verify initial schema
            let table = db.open_table("points").execute().await.unwrap();
            let schema = table.schema().await.unwrap();
            assert!(schema.field_with_name("w").is_err()); // No w field yet

            // Second: append data with x, y, z, w
            let mcap_xyzw = create_point_with_w_mcap();
            convert_mcap_to_lance(
                Cursor::new(mcap_xyzw),
                &db,
                ConvertOptions::append(),
            )
            .await
            .unwrap();

            // Verify schema evolved
            let table = db.open_table("points").execute().await.unwrap();
            let schema = table.schema().await.unwrap();
            assert!(schema.field_with_name("w").is_ok()); // Now has w field
            assert!(schema.field_with_name("w").unwrap().is_nullable()); // Must be nullable

            // Verify row count
            let count = table.count_rows(None).await.unwrap();
            assert_eq!(count, 8); // 5 + 3
        }

        fn create_int32_value_mcap() -> Vec<u8> {
            let mut buf = Vec::new();
            {
                let mut writer = Writer::new(Cursor::new(&mut buf)).unwrap();

                let schema = Arc::new(Schema {
                    id: 1,
                    name: "test_msgs/Value".into(),
                    encoding: "ros1msg".into(),
                    data: Cow::Borrowed(b"int32 value"),
                });

                let channel = Arc::new(Channel {
                    id: 1,
                    topic: "/values".into(),
                    schema: Some(schema.clone()),
                    message_encoding: "ros1".into(),
                    metadata: BTreeMap::new(),
                });

                for i in 0..3i32 {
                    let message = Message {
                        channel: channel.clone(),
                        sequence: i as u32,
                        log_time: (i as u64) * 1_000_000_000,
                        publish_time: (i as u64) * 1_000_000_000,
                        data: Cow::Owned(i.to_le_bytes().to_vec()),
                    };
                    writer.write(&message).unwrap();
                }

                writer.finish().unwrap();
            }
            buf
        }

        fn create_int64_value_mcap() -> Vec<u8> {
            let mut buf = Vec::new();
            {
                let mut writer = Writer::new(Cursor::new(&mut buf)).unwrap();

                let schema = Arc::new(Schema {
                    id: 1,
                    name: "test_msgs/Value".into(),
                    encoding: "ros1msg".into(),
                    data: Cow::Borrowed(b"int64 value"),
                });

                let channel = Arc::new(Channel {
                    id: 1,
                    topic: "/values".into(),
                    schema: Some(schema.clone()),
                    message_encoding: "ros1".into(),
                    metadata: BTreeMap::new(),
                });

                for i in 0..2i64 {
                    let message = Message {
                        channel: channel.clone(),
                        sequence: i as u32,
                        log_time: (i as u64) * 1_000_000_000,
                        publish_time: (i as u64) * 1_000_000_000,
                        data: Cow::Owned(i.to_le_bytes().to_vec()),
                    };
                    writer.write(&message).unwrap();
                }

                writer.finish().unwrap();
            }
            buf
        }

        #[tokio::test]
        async fn test_append_with_type_widening() {
            let dir = tempdir().unwrap();
            let db_path = dir.path().join("test.lance");

            let db = connect(db_path.to_str().unwrap())
                .execute()
                .await
                .unwrap();

            // First: create table with int32 value
            let mcap_i32 = create_int32_value_mcap();
            convert_mcap_to_lance(
                Cursor::new(mcap_i32),
                &db,
                ConvertOptions::default(),
            )
            .await
            .unwrap();

            // Verify initial schema
            let table = db.open_table("values").execute().await.unwrap();
            let schema = table.schema().await.unwrap();
            assert_eq!(
                schema.field_with_name("value").unwrap().data_type(),
                &arrow::datatypes::DataType::Int32
            );

            // Second: append data with int64 value
            let mcap_i64 = create_int64_value_mcap();
            convert_mcap_to_lance(
                Cursor::new(mcap_i64),
                &db,
                ConvertOptions::append(),
            )
            .await
            .unwrap();

            // Verify schema was widened
            let table = db.open_table("values").execute().await.unwrap();
            let schema = table.schema().await.unwrap();
            assert_eq!(
                schema.field_with_name("value").unwrap().data_type(),
                &arrow::datatypes::DataType::Int64
            );

            // Verify row count
            let count = table.count_rows(None).await.unwrap();
            assert_eq!(count, 5); // 3 + 2
        }

        fn create_string_value_mcap() -> Vec<u8> {
            let mut buf = Vec::new();
            {
                let mut writer = Writer::new(Cursor::new(&mut buf)).unwrap();

                let schema = Arc::new(Schema {
                    id: 1,
                    name: "test_msgs/Value".into(),
                    encoding: "ros1msg".into(),
                    data: Cow::Borrowed(b"string value"),
                });

                let channel = Arc::new(Channel {
                    id: 1,
                    topic: "/values".into(),
                    schema: Some(schema.clone()),
                    message_encoding: "ros1".into(),
                    metadata: BTreeMap::new(),
                });

                let text = "hello";
                let mut data = (text.len() as u32).to_le_bytes().to_vec();
                data.extend(text.as_bytes());

                let message = Message {
                    channel: channel.clone(),
                    sequence: 0,
                    log_time: 0,
                    publish_time: 0,
                    data: Cow::Owned(data),
                };
                writer.write(&message).unwrap();
                writer.finish().unwrap();
            }
            buf
        }

        #[tokio::test]
        async fn test_append_with_incompatible_types() {
            let dir = tempdir().unwrap();
            let db_path = dir.path().join("test.lance");

            let db = connect(db_path.to_str().unwrap())
                .execute()
                .await
                .unwrap();

            // First: create table with int32 value
            let mcap_i32 = create_int32_value_mcap();
            convert_mcap_to_lance(
                Cursor::new(mcap_i32),
                &db,
                ConvertOptions::default(),
            )
            .await
            .unwrap();

            // Second: try to append data with string value (incompatible)
            let mcap_str = create_string_value_mcap();
            let result = convert_mcap_to_lance(
                Cursor::new(mcap_str),
                &db,
                ConvertOptions::append(),
            )
            .await;

            assert!(matches!(result, Err(WriterError::Evolution(_))));
        }
    }
}
