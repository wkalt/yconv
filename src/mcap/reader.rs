//! Sans-io MCAP reader for streaming MCAP files from any byte source.

use std::collections::HashMap;
use std::io::Read;

use mcap::parse_record;
use mcap::records::{op, Record};
use mcap::sans_io::{LinearReadEvent, LinearReader};
use thiserror::Error;

/// Errors that can occur while reading an MCAP file.
#[derive(Debug, Error)]
pub enum ReaderError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("MCAP parse error: {0}")]
    Mcap(#[from] mcap::McapError),

    #[error("channel {channel_id} references unknown schema {schema_id}")]
    UnknownSchema { channel_id: u16, schema_id: u16 },

    #[error("message references unknown channel {0}")]
    UnknownChannel(u16),

    #[error("schema encoding '{0}' is not supported (expected 'ros1msg')")]
    UnsupportedEncoding(String),
}

/// Information about an MCAP schema.
#[derive(Debug, Clone)]
pub struct SchemaInfo {
    /// Unique schema ID within the file.
    pub id: u16,
    /// Full message type name (e.g., "sensor_msgs/Image").
    pub name: String,
    /// Schema encoding (e.g., "ros1msg").
    pub encoding: String,
    /// Raw schema data (message definition text for ROS1).
    pub data: Vec<u8>,
}

/// Information about an MCAP channel.
#[derive(Debug, Clone)]
pub struct ChannelInfo {
    /// Unique channel ID within the file.
    pub id: u16,
    /// Schema ID this channel uses.
    pub schema_id: u16,
    /// Topic name (e.g., "/camera/image_raw").
    pub topic: String,
    /// Message encoding (e.g., "ros1").
    pub message_encoding: String,
    /// Channel metadata.
    pub metadata: HashMap<String, String>,
}

/// A message read from the MCAP file.
#[derive(Debug, Clone)]
pub struct RawMessage {
    /// Channel ID this message was published on.
    pub channel_id: u16,
    /// Sequence number.
    pub sequence: u32,
    /// Log time in nanoseconds since epoch.
    pub log_time: u64,
    /// Publish time in nanoseconds since epoch.
    pub publish_time: u64,
    /// Raw message data.
    pub data: Vec<u8>,
}

impl RawMessage {
    /// Get the topic name for this message.
    pub fn topic<'a>(&self, reader: &'a StreamReader<impl Read>) -> Option<&'a str> {
        reader.channels.get(&self.channel_id).map(|c| c.topic.as_str())
    }
}

/// A streaming MCAP reader that processes files from any `Read` source.
///
/// This reader uses mcap's sans-io `LinearReader` internally, feeding it bytes
/// as needed and parsing records incrementally.
pub struct StreamReader<R: Read> {
    source: R,
    linear: LinearReader,
    schemas: HashMap<u16, SchemaInfo>,
    channels: HashMap<u16, ChannelInfo>,
    finished: bool,
}

impl<R: Read> StreamReader<R> {
    /// Create a new streaming MCAP reader from a byte source.
    pub fn new(source: R) -> Self {
        Self {
            source,
            linear: LinearReader::new(),
            schemas: HashMap::new(),
            channels: HashMap::new(),
            finished: false,
        }
    }

    /// Get all schemas encountered so far.
    pub fn schemas(&self) -> &HashMap<u16, SchemaInfo> {
        &self.schemas
    }

    /// Get a schema by ID.
    pub fn schema(&self, id: u16) -> Option<&SchemaInfo> {
        self.schemas.get(&id)
    }

    /// Get all channels encountered so far.
    pub fn channels(&self) -> &HashMap<u16, ChannelInfo> {
        &self.channels
    }

    /// Get a channel by ID.
    pub fn channel(&self, id: u16) -> Option<&ChannelInfo> {
        self.channels.get(&id)
    }

    /// Get the schema for a channel.
    pub fn channel_schema(&self, channel_id: u16) -> Option<&SchemaInfo> {
        self.channels
            .get(&channel_id)
            .and_then(|c| self.schemas.get(&c.schema_id))
    }

    /// Read the next message from the MCAP file.
    ///
    /// Returns `None` when the file is fully read.
    /// Schemas and channels are automatically tracked and can be queried via
    /// `schemas()`, `channels()`, etc.
    pub fn next_message(&mut self) -> Result<Option<RawMessage>, ReaderError> {
        if self.finished {
            return Ok(None);
        }

        loop {
            match self.linear.next_event() {
                None => {
                    self.finished = true;
                    return Ok(None);
                }
                Some(Err(e)) => return Err(e.into()),
                Some(Ok(event)) => match event {
                    LinearReadEvent::ReadRequest(need) => {
                        let buf = self.linear.insert(need);
                        let read = self.source.read(buf)?;
                        if read == 0 {
                            // EOF reached
                            self.finished = true;
                            return Ok(None);
                        }
                        self.linear.notify_read(read);
                    }
                    LinearReadEvent::Record { opcode, data } => {
                        match opcode {
                            op::SCHEMA => {
                                let record = parse_record(opcode, data)?;
                                if let Record::Schema { header, data } = record {
                                    self.schemas.insert(
                                        header.id,
                                        SchemaInfo {
                                            id: header.id,
                                            name: header.name,
                                            encoding: header.encoding,
                                            data: data.into_owned(),
                                        },
                                    );
                                }
                            }
                            op::CHANNEL => {
                                let record = parse_record(opcode, data)?;
                                if let Record::Channel(channel) = record {
                                    self.channels.insert(
                                        channel.id,
                                        ChannelInfo {
                                            id: channel.id,
                                            schema_id: channel.schema_id,
                                            topic: channel.topic,
                                            message_encoding: channel.message_encoding,
                                            metadata: channel.metadata.into_iter().collect(),
                                        },
                                    );
                                }
                            }
                            op::MESSAGE => {
                                let record = parse_record(opcode, data)?;
                                if let Record::Message { header, data } = record {
                                    return Ok(Some(RawMessage {
                                        channel_id: header.channel_id,
                                        sequence: header.sequence,
                                        log_time: header.log_time,
                                        publish_time: header.publish_time,
                                        data: data.into_owned(),
                                    }));
                                }
                            }
                            // Skip other record types (Header, Footer, Chunk, etc.)
                            _ => {}
                        }
                    }
                },
            }
        }
    }
}

/// Iterator adapter for StreamReader.
impl<R: Read> Iterator for StreamReader<R> {
    type Item = Result<RawMessage, ReaderError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.next_message() {
            Ok(Some(msg)) => Some(Ok(msg)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    mod reader_creation {
        use super::*;

        #[test]
        fn test_new_reader() {
            let data: Vec<u8> = vec![];
            let reader = StreamReader::new(Cursor::new(data));
            assert!(reader.schemas().is_empty());
            assert!(reader.channels().is_empty());
        }

        #[test]
        fn test_empty_file() {
            let data: Vec<u8> = vec![];
            let mut reader = StreamReader::new(Cursor::new(data));
            // Empty file should return None (EOF)
            let result = reader.next_message();
            assert!(result.is_ok());
            assert!(result.unwrap().is_none());
        }
    }

    mod valid_mcap {
        use super::*;
        use mcap::{Channel, Message, Schema, Writer};
        use std::borrow::Cow;
        use std::collections::BTreeMap;
        use std::sync::Arc;

        fn create_test_mcap() -> Vec<u8> {
            let mut buf = Vec::new();
            {
                let mut writer = Writer::new(Cursor::new(&mut buf)).unwrap();

                let schema = Arc::new(Schema {
                    id: 1,
                    name: "test_msgs/Point".into(),
                    encoding: "ros1msg".into(),
                    data: Cow::Borrowed(b"float64 x\nfloat64 y\nfloat64 z"),
                });

                let channel = Arc::new(Channel {
                    id: 1,
                    topic: "/test/point".into(),
                    schema: Some(schema.clone()),
                    message_encoding: "ros1".into(),
                    metadata: BTreeMap::new(),
                });

                // Create test message data (3 float64s = 24 bytes)
                let point_data: Vec<u8> = [1.0f64, 2.0f64, 3.0f64]
                    .iter()
                    .flat_map(|f| f.to_le_bytes())
                    .collect();

                let message = Message {
                    channel: channel.clone(),
                    sequence: 1,
                    log_time: 1000000000,
                    publish_time: 1000000000,
                    data: Cow::Owned(point_data),
                };

                writer.write(&message).unwrap();
                writer.finish().unwrap();
            }
            buf
        }

        #[test]
        fn test_read_schema() {
            let data = create_test_mcap();
            let mut reader = StreamReader::new(Cursor::new(data));

            // Read all messages to populate schemas
            while let Ok(Some(_)) = reader.next_message() {}

            assert_eq!(reader.schemas().len(), 1);
            let schema = reader.schemas().values().next().unwrap();
            assert_eq!(schema.name, "test_msgs/Point");
            assert_eq!(schema.encoding, "ros1msg");
        }

        #[test]
        fn test_read_channel() {
            let data = create_test_mcap();
            let mut reader = StreamReader::new(Cursor::new(data));

            // Read all messages to populate channels
            while let Ok(Some(_)) = reader.next_message() {}

            assert_eq!(reader.channels().len(), 1);
            let channel = reader.channels().values().next().unwrap();
            assert_eq!(channel.topic, "/test/point");
            assert_eq!(channel.message_encoding, "ros1");
        }

        #[test]
        fn test_read_message() {
            let data = create_test_mcap();
            let mut reader = StreamReader::new(Cursor::new(data));

            let msg = reader.next_message().unwrap().unwrap();
            assert_eq!(msg.sequence, 1);
            assert_eq!(msg.log_time, 1000000000);
            assert_eq!(msg.data.len(), 24); // 3 float64s

            // Parse the floats
            let x = f64::from_le_bytes(msg.data[0..8].try_into().unwrap());
            let y = f64::from_le_bytes(msg.data[8..16].try_into().unwrap());
            let z = f64::from_le_bytes(msg.data[16..24].try_into().unwrap());
            assert_eq!(x, 1.0);
            assert_eq!(y, 2.0);
            assert_eq!(z, 3.0);
        }

        #[test]
        fn test_iterator() {
            let data = create_test_mcap();
            let reader = StreamReader::new(Cursor::new(data));

            let messages: Vec<_> = reader.collect();
            assert_eq!(messages.len(), 1);
            assert!(messages[0].is_ok());
        }

        #[test]
        fn test_channel_schema_lookup() {
            let data = create_test_mcap();
            let mut reader = StreamReader::new(Cursor::new(data));

            let msg = reader.next_message().unwrap().unwrap();
            let schema = reader.channel_schema(msg.channel_id);
            assert!(schema.is_some());
            assert_eq!(schema.unwrap().name, "test_msgs/Point");
        }
    }

    mod multiple_messages {
        use super::*;
        use mcap::{Channel, Message, Schema, Writer};
        use std::borrow::Cow;
        use std::collections::BTreeMap;
        use std::sync::Arc;

        fn create_multi_message_mcap() -> Vec<u8> {
            let mut buf = Vec::new();
            {
                let mut writer = Writer::new(Cursor::new(&mut buf)).unwrap();

                let schema = Arc::new(Schema {
                    id: 1,
                    name: "std_msgs/UInt32".into(),
                    encoding: "ros1msg".into(),
                    data: Cow::Borrowed(b"uint32 data"),
                });

                let channel = Arc::new(Channel {
                    id: 1,
                    topic: "/counter".into(),
                    schema: Some(schema.clone()),
                    message_encoding: "ros1".into(),
                    metadata: BTreeMap::new(),
                });

                for i in 0..5u32 {
                    let data = i.to_le_bytes().to_vec();
                    let message = Message {
                        channel: channel.clone(),
                        sequence: i,
                        log_time: (i as u64) * 1_000_000_000,
                        publish_time: (i as u64) * 1_000_000_000,
                        data: Cow::Owned(data),
                    };
                    writer.write(&message).unwrap();
                }

                writer.finish().unwrap();
            }
            buf
        }

        #[test]
        fn test_read_multiple_messages() {
            let data = create_multi_message_mcap();
            let mut reader = StreamReader::new(Cursor::new(data));

            let mut count = 0;
            while let Ok(Some(msg)) = reader.next_message() {
                let value = u32::from_le_bytes(msg.data[0..4].try_into().unwrap());
                assert_eq!(value, msg.sequence);
                count += 1;
            }
            assert_eq!(count, 5);
        }

        #[test]
        fn test_iterator_count() {
            let data = create_multi_message_mcap();
            let reader = StreamReader::new(Cursor::new(data));
            assert_eq!(reader.count(), 5);
        }
    }

    mod multiple_topics {
        use super::*;
        use mcap::{Channel, Message, Schema, Writer};
        use std::borrow::Cow;
        use std::collections::BTreeMap;
        use std::sync::Arc;

        fn create_multi_topic_mcap() -> Vec<u8> {
            let mut buf = Vec::new();
            {
                let mut writer = Writer::new(Cursor::new(&mut buf)).unwrap();

                let schema1 = Arc::new(Schema {
                    id: 1,
                    name: "std_msgs/String".into(),
                    encoding: "ros1msg".into(),
                    data: Cow::Borrowed(b"string data"),
                });

                let schema2 = Arc::new(Schema {
                    id: 2,
                    name: "std_msgs/Int32".into(),
                    encoding: "ros1msg".into(),
                    data: Cow::Borrowed(b"int32 data"),
                });

                let channel1 = Arc::new(Channel {
                    id: 1,
                    topic: "/topic_a".into(),
                    schema: Some(schema1.clone()),
                    message_encoding: "ros1".into(),
                    metadata: BTreeMap::new(),
                });

                let channel2 = Arc::new(Channel {
                    id: 2,
                    topic: "/topic_b".into(),
                    schema: Some(schema2.clone()),
                    message_encoding: "ros1".into(),
                    metadata: BTreeMap::new(),
                });

                // Write alternating messages
                for i in 0..4u32 {
                    if i % 2 == 0 {
                        // String message: length prefix + bytes
                        let s = format!("msg{}", i);
                        let mut data = (s.len() as u32).to_le_bytes().to_vec();
                        data.extend(s.as_bytes());
                        let message = Message {
                            channel: channel1.clone(),
                            sequence: i,
                            log_time: (i as u64) * 1_000_000_000,
                            publish_time: (i as u64) * 1_000_000_000,
                            data: Cow::Owned(data),
                        };
                        writer.write(&message).unwrap();
                    } else {
                        let data = (i as i32).to_le_bytes().to_vec();
                        let message = Message {
                            channel: channel2.clone(),
                            sequence: i,
                            log_time: (i as u64) * 1_000_000_000,
                            publish_time: (i as u64) * 1_000_000_000,
                            data: Cow::Owned(data),
                        };
                        writer.write(&message).unwrap();
                    }
                }

                writer.finish().unwrap();
            }
            buf
        }

        #[test]
        fn test_multiple_schemas() {
            let data = create_multi_topic_mcap();
            let mut reader = StreamReader::new(Cursor::new(data));

            while let Ok(Some(_)) = reader.next_message() {}

            assert_eq!(reader.schemas().len(), 2);
            let names: Vec<_> = reader.schemas().values().map(|s| s.name.as_str()).collect();
            assert!(names.contains(&"std_msgs/String"));
            assert!(names.contains(&"std_msgs/Int32"));
        }

        #[test]
        fn test_multiple_channels() {
            let data = create_multi_topic_mcap();
            let mut reader = StreamReader::new(Cursor::new(data));

            while let Ok(Some(_)) = reader.next_message() {}

            assert_eq!(reader.channels().len(), 2);
            let topics: Vec<_> = reader.channels().values().map(|c| c.topic.as_str()).collect();
            assert!(topics.contains(&"/topic_a"));
            assert!(topics.contains(&"/topic_b"));
        }

        #[test]
        fn test_message_channel_association() {
            let data = create_multi_topic_mcap();
            let mut reader = StreamReader::new(Cursor::new(data));

            let mut topic_a_count = 0;
            let mut topic_b_count = 0;

            while let Ok(Some(msg)) = reader.next_message() {
                let channel = reader.channel(msg.channel_id).unwrap();
                match channel.topic.as_str() {
                    "/topic_a" => topic_a_count += 1,
                    "/topic_b" => topic_b_count += 1,
                    _ => panic!("unexpected topic"),
                }
            }

            assert_eq!(topic_a_count, 2);
            assert_eq!(topic_b_count, 2);
        }
    }
}
