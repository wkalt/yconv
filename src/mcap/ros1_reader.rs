//! High-level ROS1-aware MCAP reader that deserializes messages.

use std::collections::HashMap;
use std::io::Read;
use std::sync::Arc;

use arrow::datatypes::Schema;
use thiserror::Error;

use crate::arrow::RecordBatchBuilder;
use crate::mcap::{ChannelInfo, ReaderError, StreamReader};
use crate::ros1::{
    deserialize, message_to_arrow_schema, ConversionError, DeserializeError, MessageDefinition,
    MessageRegistry, ParseError, Value,
};

/// Errors that can occur while reading ROS1 messages from MCAP.
#[derive(Debug, Error)]
pub enum Ros1ReaderError {
    #[error("MCAP reader error: {0}")]
    Reader(#[from] ReaderError),

    #[error("schema parsing error: {0}")]
    Parse(#[from] ParseError),

    #[error("deserialization error: {0}")]
    Deserialize(#[from] DeserializeError),

    #[error("schema conversion error: {0}")]
    Conversion(#[from] ConversionError),

    #[error("schema encoding '{0}' is not ros1msg")]
    UnsupportedEncoding(String),

    #[error("channel {channel_id} references unknown schema {schema_id}")]
    UnknownSchema { channel_id: u16, schema_id: u16 },

    #[error("message references unknown channel {0}")]
    UnknownChannel(u16),

    #[error("schema data is not valid UTF-8: {0}")]
    InvalidSchemaUtf8(std::string::FromUtf8Error),
}

/// A deserialized ROS1 message with metadata.
#[derive(Debug, Clone)]
pub struct Ros1Message {
    /// Topic name (e.g., "/camera/image_raw").
    pub topic: String,
    /// Message type name (e.g., "sensor_msgs/Image").
    pub message_type: String,
    /// Log time in nanoseconds since epoch.
    pub log_time: u64,
    /// Publish time in nanoseconds since epoch.
    pub publish_time: u64,
    /// Sequence number.
    pub sequence: u32,
    /// Deserialized message value.
    pub value: Value,
}

/// A raw ROS1 message with schema info for direct transcoding.
///
/// This struct provides access to the raw message bytes along with
/// the parsed schema information needed for transcoding.
pub struct RawRos1Message {
    /// Topic name.
    pub topic: String,
    /// Channel ID this message was published on.
    pub channel_id: u16,
    /// Log time in nanoseconds since epoch.
    pub log_time: u64,
    /// Publish time in nanoseconds since epoch.
    pub publish_time: u64,
    /// Sequence number.
    pub sequence: u32,
    /// Raw message bytes (owned to avoid lifetime issues).
    pub data: Vec<u8>,
    /// Schema ID for looking up definition/registry.
    pub schema_id: u16,
}

/// Cached information about a parsed schema.
struct ParsedSchema {
    /// The message type name (e.g., "sensor_msgs/Image").
    message_type: String,
    /// The message registry containing this schema and its dependencies.
    registry: MessageRegistry,
    /// The root message definition.
    definition: MessageDefinition,
    /// The Arrow schema for this message type.
    arrow_schema: Arc<Schema>,
}

/// High-level reader for ROS1-encoded MCAP files.
///
/// This reader automatically parses ROS1 message definitions from MCAP schemas
/// and deserializes messages as they are read.
pub struct Ros1Reader<R: Read> {
    inner: StreamReader<R>,
    /// Parsed schemas by schema ID.
    parsed_schemas: HashMap<u16, ParsedSchema>,
    /// Arrow schemas by topic name.
    topic_schemas: HashMap<String, Arc<Schema>>,
}

impl<R: Read> Ros1Reader<R> {
    /// Create a new ROS1 MCAP reader.
    pub fn new(source: R) -> Self {
        Self {
            inner: StreamReader::new(source),
            parsed_schemas: HashMap::new(),
            topic_schemas: HashMap::new(),
        }
    }

    /// Get the Arrow schema for a topic.
    ///
    /// Returns `None` if no messages have been read for this topic yet.
    pub fn topic_schema(&self, topic: &str) -> Option<&Arc<Schema>> {
        self.topic_schemas.get(topic)
    }

    /// Get all topic schemas discovered so far.
    pub fn topic_schemas(&self) -> &HashMap<String, Arc<Schema>> {
        &self.topic_schemas
    }

    /// Get all topics discovered so far.
    pub fn topics(&self) -> Vec<&str> {
        self.inner
            .channels()
            .values()
            .map(|c| c.topic.as_str())
            .collect()
    }

    /// Get channel info by topic name.
    pub fn channel_by_topic(&self, topic: &str) -> Option<&ChannelInfo> {
        self.inner.channels().values().find(|c| c.topic == topic)
    }

    /// Get all channels discovered so far.
    pub fn channels(&self) -> &HashMap<u16, ChannelInfo> {
        self.inner.channels()
    }

    /// Get all schemas discovered so far.
    pub fn schemas(&self) -> &HashMap<u16, crate::mcap::SchemaInfo> {
        self.inner.schemas()
    }

    /// Read the next message from the MCAP file.
    ///
    /// Returns `None` when the file is fully read.
    pub fn next_message(&mut self) -> Result<Option<Ros1Message>, Ros1ReaderError> {
        loop {
            let raw_msg = match self.inner.next_message()? {
                Some(msg) => msg,
                None => return Ok(None),
            };

            // Get channel info and extract what we need before any mutable borrows
            let (message_encoding, schema_id, topic) = {
                let channel = self
                    .inner
                    .channel(raw_msg.channel_id)
                    .ok_or(Ros1ReaderError::UnknownChannel(raw_msg.channel_id))?;
                (
                    channel.message_encoding.clone(),
                    channel.schema_id,
                    channel.topic.clone(),
                )
            };

            // Only handle ros1-encoded messages
            if message_encoding != "ros1" {
                continue;
            }

            // Get or parse the schema
            if !self.parsed_schemas.contains_key(&schema_id) {
                self.parse_schema(schema_id)?;
            }

            let parsed = self.parsed_schemas.get(&schema_id).unwrap();

            // Cache Arrow schema for this topic
            if !self.topic_schemas.contains_key(&topic) {
                self.topic_schemas
                    .insert(topic.clone(), parsed.arrow_schema.clone());
            }

            // Deserialize the message
            let value = deserialize(&raw_msg.data, &parsed.definition, &parsed.registry)?;

            return Ok(Some(Ros1Message {
                topic,
                message_type: parsed.message_type.clone(),
                log_time: raw_msg.log_time,
                publish_time: raw_msg.publish_time,
                sequence: raw_msg.sequence,
                value,
            }));
        }
    }

    /// Parse a schema from the MCAP file and cache it.
    fn parse_schema(&mut self, schema_id: u16) -> Result<(), Ros1ReaderError> {
        let schema_info = self
            .inner
            .schema(schema_id)
            .ok_or(Ros1ReaderError::UnknownSchema {
                channel_id: 0,
                schema_id,
            })?;

        // Check encoding
        if schema_info.encoding != "ros1msg" {
            return Err(Ros1ReaderError::UnsupportedEncoding(
                schema_info.encoding.clone(),
            ));
        }

        // Parse schema text
        let schema_text = String::from_utf8(schema_info.data.clone())
            .map_err(Ros1ReaderError::InvalidSchemaUtf8)?;

        // Store the message type name from schema info
        let message_type = schema_info.name.clone();

        // Parse into message registry
        let mut registry = MessageRegistry::new();
        registry.parse_mcap_schema(&message_type, &schema_text)?;

        // Get the root definition from the registry
        let definition = registry
            .get(&message_type)
            .ok_or(Ros1ReaderError::UnknownSchema {
                channel_id: 0,
                schema_id,
            })?
            .clone();

        // Create Arrow schema
        let arrow_schema = Arc::new(message_to_arrow_schema(&definition, &registry)?);

        self.parsed_schemas.insert(
            schema_id,
            ParsedSchema {
                message_type,
                registry,
                definition,
                arrow_schema,
            },
        );

        Ok(())
    }

    /// Read the next raw message without deserializing.
    ///
    /// This is more efficient than `next_message()` when you want to transcode
    /// directly to Arrow without creating intermediate Value objects.
    /// Use `definition()` and `registry()` to get schema info for transcoding.
    pub fn next_raw_message(&mut self) -> Result<Option<RawRos1Message>, Ros1ReaderError> {
        loop {
            let raw_msg = match self.inner.next_message()? {
                Some(msg) => msg,
                None => return Ok(None),
            };

            // Get channel info
            let (message_encoding, schema_id, topic) = {
                let channel = self
                    .inner
                    .channel(raw_msg.channel_id)
                    .ok_or(Ros1ReaderError::UnknownChannel(raw_msg.channel_id))?;
                (
                    channel.message_encoding.clone(),
                    channel.schema_id,
                    channel.topic.clone(),
                )
            };

            // Only handle ros1-encoded messages
            if message_encoding != "ros1" {
                continue;
            }

            // Ensure schema is parsed
            if !self.parsed_schemas.contains_key(&schema_id) {
                self.parse_schema(schema_id)?;
            }

            // Cache Arrow schema for this topic
            let parsed = self.parsed_schemas.get(&schema_id).unwrap();
            if !self.topic_schemas.contains_key(&topic) {
                self.topic_schemas
                    .insert(topic.clone(), parsed.arrow_schema.clone());
            }

            return Ok(Some(RawRos1Message {
                topic,
                channel_id: raw_msg.channel_id,
                log_time: raw_msg.log_time,
                publish_time: raw_msg.publish_time,
                sequence: raw_msg.sequence,
                data: raw_msg.data,
                schema_id,
            }));
        }
    }

    /// Get the message definition for a schema ID.
    ///
    /// Returns `None` if the schema hasn't been parsed yet.
    pub fn definition(&self, schema_id: u16) -> Option<&MessageDefinition> {
        self.parsed_schemas.get(&schema_id).map(|p| &p.definition)
    }

    /// Get the message registry for a schema ID.
    ///
    /// Returns `None` if the schema hasn't been parsed yet.
    pub fn registry(&self, schema_id: u16) -> Option<&MessageRegistry> {
        self.parsed_schemas.get(&schema_id).map(|p| &p.registry)
    }

    /// Create a RecordBatchBuilder for a topic.
    ///
    /// Returns `None` if no messages have been read for this topic yet.
    pub fn builder_for_topic(
        &self,
        topic: &str,
    ) -> Option<Result<RecordBatchBuilder, crate::arrow::BuilderError>> {
        self.topic_schemas
            .get(topic)
            .map(|schema| RecordBatchBuilder::new(schema.clone()))
    }
}

/// Iterator adapter for Ros1Reader.
impl<R: Read> Iterator for Ros1Reader<R> {
    type Item = Result<Ros1Message, Ros1ReaderError>;

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
    use mcap::{Channel, Message, Schema, Writer};
    use std::borrow::Cow;
    use std::collections::BTreeMap;
    use std::io::Cursor;
    use std::sync::Arc;

    fn create_ros1_mcap() -> Vec<u8> {
        let mut buf = Vec::new();
        {
            let mut writer = Writer::new(Cursor::new(&mut buf)).unwrap();

            // ROS1 schema for a Point message
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

            // Create ROS1 serialized message (3 float64s = 24 bytes, little-endian)
            let point_data: Vec<u8> = [1.5f64, 2.5f64, 3.5f64]
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

    fn create_multi_topic_ros1_mcap() -> Vec<u8> {
        let mut buf = Vec::new();
        {
            let mut writer = Writer::new(Cursor::new(&mut buf)).unwrap();

            // Schema for Point
            let point_schema = Arc::new(Schema {
                id: 1,
                name: "geometry_msgs/Point".into(),
                encoding: "ros1msg".into(),
                data: Cow::Borrowed(b"float64 x\nfloat64 y\nfloat64 z"),
            });

            // Schema for String
            let string_schema = Arc::new(Schema {
                id: 2,
                name: "std_msgs/String".into(),
                encoding: "ros1msg".into(),
                data: Cow::Borrowed(b"string data"),
            });

            let point_channel = Arc::new(Channel {
                id: 1,
                topic: "/points".into(),
                schema: Some(point_schema.clone()),
                message_encoding: "ros1".into(),
                metadata: BTreeMap::new(),
            });

            let string_channel = Arc::new(Channel {
                id: 2,
                topic: "/status".into(),
                schema: Some(string_schema.clone()),
                message_encoding: "ros1".into(),
                metadata: BTreeMap::new(),
            });

            // Write a point message
            let point_data: Vec<u8> = [1.0f64, 2.0f64, 3.0f64]
                .iter()
                .flat_map(|f| f.to_le_bytes())
                .collect();

            writer
                .write(&Message {
                    channel: point_channel.clone(),
                    sequence: 1,
                    log_time: 1000000000,
                    publish_time: 1000000000,
                    data: Cow::Owned(point_data),
                })
                .unwrap();

            // Write a string message (length-prefixed)
            let text = "hello";
            let mut string_data = (text.len() as u32).to_le_bytes().to_vec();
            string_data.extend(text.as_bytes());

            writer
                .write(&Message {
                    channel: string_channel.clone(),
                    sequence: 2,
                    log_time: 2000000000,
                    publish_time: 2000000000,
                    data: Cow::Owned(string_data),
                })
                .unwrap();

            // Write another point
            let point_data2: Vec<u8> = [4.0f64, 5.0f64, 6.0f64]
                .iter()
                .flat_map(|f| f.to_le_bytes())
                .collect();

            writer
                .write(&Message {
                    channel: point_channel.clone(),
                    sequence: 3,
                    log_time: 3000000000,
                    publish_time: 3000000000,
                    data: Cow::Owned(point_data2),
                })
                .unwrap();

            writer.finish().unwrap();
        }
        buf
    }

    mod basic_reading {
        use super::*;

        #[test]
        fn test_read_single_message() {
            let data = create_ros1_mcap();
            let mut reader = Ros1Reader::new(Cursor::new(data));

            let msg = reader.next_message().unwrap().unwrap();
            assert_eq!(msg.topic, "/points");
            assert_eq!(msg.message_type, "geometry_msgs/Point");
            assert_eq!(msg.sequence, 1);

            // Check the deserialized value
            if let Value::Message(fields) = &msg.value {
                assert_eq!(fields.get("x").unwrap().as_f64(), Some(1.5));
                assert_eq!(fields.get("y").unwrap().as_f64(), Some(2.5));
                assert_eq!(fields.get("z").unwrap().as_f64(), Some(3.5));
            } else {
                panic!("expected Message value");
            }
        }

        #[test]
        fn test_iterator() {
            let data = create_ros1_mcap();
            let reader = Ros1Reader::new(Cursor::new(data));
            let messages: Vec<_> = reader.collect();
            assert_eq!(messages.len(), 1);
            assert!(messages[0].is_ok());
        }

        #[test]
        fn test_topic_schema() {
            let data = create_ros1_mcap();
            let mut reader = Ros1Reader::new(Cursor::new(data));

            // Schema not available before reading
            assert!(reader.topic_schema("/points").is_none());

            // Read message
            let _ = reader.next_message().unwrap();

            // Now schema should be available
            let schema = reader.topic_schema("/points").unwrap();
            assert_eq!(schema.fields().len(), 3);
            assert!(schema.field_with_name("x").is_ok());
            assert!(schema.field_with_name("y").is_ok());
            assert!(schema.field_with_name("z").is_ok());
        }
    }

    mod multi_topic {
        use super::*;

        #[test]
        fn test_multiple_topics() {
            let data = create_multi_topic_ros1_mcap();
            let mut reader = Ros1Reader::new(Cursor::new(data));

            let mut points = Vec::new();
            let mut strings = Vec::new();

            while let Ok(Some(msg)) = reader.next_message() {
                match msg.topic.as_str() {
                    "/points" => points.push(msg),
                    "/status" => strings.push(msg),
                    _ => panic!("unexpected topic"),
                }
            }

            assert_eq!(points.len(), 2);
            assert_eq!(strings.len(), 1);

            // Check string message
            if let Value::Message(fields) = &strings[0].value {
                assert_eq!(
                    fields.get("data").unwrap().as_str(),
                    Some("hello".to_string()).as_deref()
                );
            }
        }

        #[test]
        fn test_topic_schemas() {
            let data = create_multi_topic_ros1_mcap();
            let mut reader = Ros1Reader::new(Cursor::new(data));

            // Read all messages
            while let Ok(Some(_)) = reader.next_message() {}

            let schemas = reader.topic_schemas();
            assert_eq!(schemas.len(), 2);
            assert!(schemas.contains_key("/points"));
            assert!(schemas.contains_key("/status"));
        }

        #[test]
        fn test_topics() {
            let data = create_multi_topic_ros1_mcap();
            let mut reader = Ros1Reader::new(Cursor::new(data));

            // Read all messages
            while let Ok(Some(_)) = reader.next_message() {}

            let topics = reader.topics();
            assert_eq!(topics.len(), 2);
            assert!(topics.contains(&"/points"));
            assert!(topics.contains(&"/status"));
        }
    }

    mod arrow_integration {
        use super::*;

        #[test]
        fn test_builder_for_topic() {
            let data = create_ros1_mcap();
            let mut reader = Ros1Reader::new(Cursor::new(data));

            // Not available before reading
            assert!(reader.builder_for_topic("/points").is_none());

            // Read message to trigger schema parsing
            let _msg = reader.next_message().unwrap().unwrap();

            // Now we can create a builder
            let builder = reader.builder_for_topic("/points").unwrap().unwrap();
            assert_eq!(builder.len(), 0);
        }

        #[test]
        fn test_build_record_batch() {
            let data = create_multi_topic_ros1_mcap();
            let mut reader = Ros1Reader::new(Cursor::new(data));

            // Collect messages by topic
            let mut point_messages = Vec::new();
            while let Ok(Some(msg)) = reader.next_message() {
                if msg.topic == "/points" {
                    point_messages.push(msg);
                }
            }

            // Create builder and add messages
            let mut builder = reader.builder_for_topic("/points").unwrap().unwrap();
            for msg in &point_messages {
                builder.append(&msg.value).unwrap();
            }

            // Build the record batch
            let batch = builder.finish().unwrap();
            assert_eq!(batch.num_rows(), 2);
            assert_eq!(batch.num_columns(), 3);
        }
    }

    mod nested_messages {
        use super::*;

        fn create_nested_ros1_mcap() -> Vec<u8> {
            let mut buf = Vec::new();
            {
                let mut writer = Writer::new(Cursor::new(&mut buf)).unwrap();

                // Schema for PoseStamped (nested message with Header and Pose)
                let schema_text = b"\
std_msgs/Header header
geometry_msgs/Pose pose
================================================================================
MSG: std_msgs/Header
uint32 seq
time stamp
string frame_id
================================================================================
MSG: geometry_msgs/Pose
geometry_msgs/Point position
geometry_msgs/Quaternion orientation
================================================================================
MSG: geometry_msgs/Point
float64 x
float64 y
float64 z
================================================================================
MSG: geometry_msgs/Quaternion
float64 x
float64 y
float64 z
float64 w";

                let schema = Arc::new(Schema {
                    id: 1,
                    name: "geometry_msgs/PoseStamped".into(),
                    encoding: "ros1msg".into(),
                    data: Cow::Borrowed(schema_text),
                });

                let channel = Arc::new(Channel {
                    id: 1,
                    topic: "/pose".into(),
                    schema: Some(schema.clone()),
                    message_encoding: "ros1".into(),
                    metadata: BTreeMap::new(),
                });

                // Create serialized message:
                // Header: seq(u32) + stamp(time: u32+u32) + frame_id(string: len+bytes)
                // Pose: Point(3*f64) + Quaternion(4*f64)
                let mut msg_data = Vec::new();

                // Header.seq
                msg_data.extend(1u32.to_le_bytes());
                // Header.stamp (sec, nsec)
                msg_data.extend(1000u32.to_le_bytes());
                msg_data.extend(500u32.to_le_bytes());
                // Header.frame_id
                let frame_id = "map";
                msg_data.extend((frame_id.len() as u32).to_le_bytes());
                msg_data.extend(frame_id.as_bytes());

                // Pose.position (x, y, z)
                msg_data.extend(1.0f64.to_le_bytes());
                msg_data.extend(2.0f64.to_le_bytes());
                msg_data.extend(3.0f64.to_le_bytes());

                // Pose.orientation (x, y, z, w)
                msg_data.extend(0.0f64.to_le_bytes());
                msg_data.extend(0.0f64.to_le_bytes());
                msg_data.extend(0.0f64.to_le_bytes());
                msg_data.extend(1.0f64.to_le_bytes());

                writer
                    .write(&Message {
                        channel: channel.clone(),
                        sequence: 1,
                        log_time: 1000000000,
                        publish_time: 1000000000,
                        data: Cow::Owned(msg_data),
                    })
                    .unwrap();

                writer.finish().unwrap();
            }
            buf
        }

        #[test]
        fn test_nested_message() {
            let data = create_nested_ros1_mcap();
            let mut reader = Ros1Reader::new(Cursor::new(data));

            let msg = reader.next_message().unwrap().unwrap();
            assert_eq!(msg.topic, "/pose");
            assert_eq!(msg.message_type, "geometry_msgs/PoseStamped");

            // Navigate the nested structure
            if let Value::Message(fields) = &msg.value {
                // Check header
                if let Some(Value::Message(header)) = fields.get("header") {
                    assert_eq!(header.get("seq").unwrap().as_u64(), Some(1));
                    assert_eq!(
                        header.get("frame_id").unwrap().as_str(),
                        Some("map".to_string()).as_deref()
                    );
                } else {
                    panic!("expected header message");
                }

                // Check pose
                if let Some(Value::Message(pose)) = fields.get("pose") {
                    if let Some(Value::Message(position)) = pose.get("position") {
                        assert_eq!(position.get("x").unwrap().as_f64(), Some(1.0));
                        assert_eq!(position.get("y").unwrap().as_f64(), Some(2.0));
                        assert_eq!(position.get("z").unwrap().as_f64(), Some(3.0));
                    } else {
                        panic!("expected position message");
                    }

                    if let Some(Value::Message(orientation)) = pose.get("orientation") {
                        assert_eq!(orientation.get("w").unwrap().as_f64(), Some(1.0));
                    } else {
                        panic!("expected orientation message");
                    }
                } else {
                    panic!("expected pose message");
                }
            } else {
                panic!("expected Message value");
            }
        }

        #[test]
        fn test_nested_arrow_schema() {
            let data = create_nested_ros1_mcap();
            let mut reader = Ros1Reader::new(Cursor::new(data));

            // Read to trigger schema parsing
            let _ = reader.next_message().unwrap();

            let schema = reader.topic_schema("/pose").unwrap();
            // Should have header and pose fields
            assert!(schema.field_with_name("header").is_ok());
            assert!(schema.field_with_name("pose").is_ok());
        }
    }
}
