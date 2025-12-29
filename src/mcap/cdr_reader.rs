//! High-level CDR/ROS2-aware MCAP reader that deserializes messages.

use std::collections::HashMap;
use std::io::Read;
use std::sync::Arc;

use arrow::datatypes::Schema;
use thiserror::Error;

use crate::arrow::RecordBatchBuilder;
use crate::mcap::{ChannelInfo, ReaderError, StreamReader};
use crate::ros1::{
    message_to_arrow_schema, ConversionError, MessageDefinition, MessageRegistry, ParseError,
};

/// Errors that can occur while reading CDR messages from MCAP.
#[derive(Debug, Error)]
pub enum CdrReaderError {
    #[error("MCAP reader error: {0}")]
    Reader(#[from] ReaderError),

    #[error("schema parsing error: {0}")]
    Parse(#[from] ParseError),

    #[error("schema conversion error: {0}")]
    Conversion(#[from] ConversionError),

    #[error("schema encoding '{0}' is not ros2msg")]
    UnsupportedEncoding(String),

    #[error("channel {channel_id} references unknown schema {schema_id}")]
    UnknownSchema { channel_id: u16, schema_id: u16 },

    #[error("message references unknown channel {0}")]
    UnknownChannel(u16),

    #[error("schema data is not valid UTF-8: {0}")]
    InvalidSchemaUtf8(std::string::FromUtf8Error),
}

/// A raw CDR message with schema info for direct transcoding.
pub struct RawCdrMessage {
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
    /// Raw message bytes (includes CDR encapsulation header).
    pub data: Vec<u8>,
    /// Schema ID for looking up definition/registry.
    pub schema_id: u16,
}

/// Cached information about a parsed schema.
struct ParsedSchema {
    /// The message registry containing this schema and its dependencies.
    registry: MessageRegistry,
    /// The root message definition.
    definition: MessageDefinition,
    /// The Arrow schema for this message type.
    arrow_schema: Arc<Schema>,
}

/// High-level reader for CDR-encoded (ROS2) MCAP files.
///
/// This reader automatically parses ROS2 message definitions from MCAP schemas
/// and provides access to raw messages for transcoding.
pub struct CdrReader<R: Read> {
    inner: StreamReader<R>,
    /// Parsed schemas by schema ID.
    parsed_schemas: HashMap<u16, ParsedSchema>,
    /// Arrow schemas by topic name.
    topic_schemas: HashMap<String, Arc<Schema>>,
}

impl<R: Read> CdrReader<R> {
    /// Create a new CDR MCAP reader.
    pub fn new(source: R) -> Self {
        Self {
            inner: StreamReader::new(source),
            parsed_schemas: HashMap::new(),
            topic_schemas: HashMap::new(),
        }
    }

    /// Get the Arrow schema for a topic.
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

    /// Read the next raw message without deserializing.
    ///
    /// This is efficient for transcoding directly to Arrow.
    /// Use `definition()` and `registry()` to get schema info for transcoding.
    pub fn next_raw_message(&mut self) -> Result<Option<RawCdrMessage>, CdrReaderError> {
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
                    .ok_or(CdrReaderError::UnknownChannel(raw_msg.channel_id))?;
                (
                    channel.message_encoding.clone(),
                    channel.schema_id,
                    channel.topic.clone(),
                )
            };

            // Only handle cdr-encoded messages
            if message_encoding != "cdr" {
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

            return Ok(Some(RawCdrMessage {
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

    /// Parse a schema from the MCAP file and cache it.
    fn parse_schema(&mut self, schema_id: u16) -> Result<(), CdrReaderError> {
        let schema_info = self
            .inner
            .schema(schema_id)
            .ok_or(CdrReaderError::UnknownSchema {
                channel_id: 0,
                schema_id,
            })?;

        // Check encoding - accept ros2msg or empty (some MCAP files don't set it)
        if !schema_info.encoding.is_empty() && schema_info.encoding != "ros2msg" {
            return Err(CdrReaderError::UnsupportedEncoding(
                schema_info.encoding.clone(),
            ));
        }

        // Parse schema text
        let schema_text = String::from_utf8(schema_info.data.clone())
            .map_err(CdrReaderError::InvalidSchemaUtf8)?;

        // Store the message type name from schema info
        // ROS2 uses format like "sensor_msgs/msg/Imu"
        let message_type = schema_info.name.clone();

        // Parse into message registry
        // ros2msg format is compatible with ros1msg parser
        let mut registry = MessageRegistry::new();
        registry.parse_mcap_schema(&message_type, &schema_text)?;

        // Get the root definition from the registry
        let definition = registry
            .get(&message_type)
            .ok_or(CdrReaderError::UnknownSchema {
                channel_id: 0,
                schema_id,
            })?
            .clone();

        // Create Arrow schema
        let arrow_schema = Arc::new(message_to_arrow_schema(&definition, &registry)?);

        self.parsed_schemas.insert(
            schema_id,
            ParsedSchema {
                registry,
                definition,
                arrow_schema,
            },
        );

        Ok(())
    }

    /// Get the message definition for a schema ID.
    pub fn definition(&self, schema_id: u16) -> Option<&MessageDefinition> {
        self.parsed_schemas.get(&schema_id).map(|p| &p.definition)
    }

    /// Get the message registry for a schema ID.
    pub fn registry(&self, schema_id: u16) -> Option<&MessageRegistry> {
        self.parsed_schemas.get(&schema_id).map(|p| &p.registry)
    }

    /// Create a RecordBatchBuilder for a topic.
    pub fn builder_for_topic(
        &self,
        topic: &str,
    ) -> Option<Result<RecordBatchBuilder, crate::arrow::BuilderError>> {
        self.topic_schemas
            .get(topic)
            .map(|schema| RecordBatchBuilder::new(schema.clone()))
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

    /// Helper to create CDR data with encapsulation header
    fn with_encapsulation(data: &[u8]) -> Vec<u8> {
        let mut result = vec![0x00, 0x01, 0x00, 0x00]; // CDR_LE header
        result.extend_from_slice(data);
        result
    }

    fn create_cdr_mcap() -> Vec<u8> {
        let mut buf = Vec::new();
        {
            let mut writer = Writer::new(Cursor::new(&mut buf)).unwrap();

            // ROS2 schema for a Point message
            let schema = Arc::new(Schema {
                id: 1,
                name: "geometry_msgs/msg/Point".into(),
                encoding: "ros2msg".into(),
                data: Cow::Borrowed(b"float64 x\nfloat64 y\nfloat64 z"),
            });

            let channel = Arc::new(Channel {
                id: 1,
                topic: "/points".into(),
                schema: Some(schema.clone()),
                message_encoding: "cdr".into(),
                metadata: BTreeMap::new(),
            });

            // Create CDR serialized message (3 float64s = 24 bytes)
            let mut point_data = Vec::new();
            point_data.extend_from_slice(&1.5f64.to_le_bytes());
            point_data.extend_from_slice(&2.5f64.to_le_bytes());
            point_data.extend_from_slice(&3.5f64.to_le_bytes());

            let message = Message {
                channel: channel.clone(),
                sequence: 1,
                log_time: 1000000000,
                publish_time: 1000000000,
                data: Cow::Owned(with_encapsulation(&point_data)),
            };

            writer.write(&message).unwrap();
            writer.finish().unwrap();
        }
        buf
    }

    #[test]
    fn test_read_cdr_message() {
        let data = create_cdr_mcap();
        let mut reader = CdrReader::new(Cursor::new(data));

        let msg = reader.next_raw_message().unwrap().unwrap();
        assert_eq!(msg.topic, "/points");
        assert_eq!(msg.sequence, 1);
        // 4-byte encapsulation header + 24 bytes of data
        assert_eq!(msg.data.len(), 28);
    }

    #[test]
    fn test_topic_schema() {
        let data = create_cdr_mcap();
        let mut reader = CdrReader::new(Cursor::new(data));

        // Schema not available before reading
        assert!(reader.topic_schema("/points").is_none());

        // Read message
        let _ = reader.next_raw_message().unwrap();

        // Now schema should be available
        let schema = reader.topic_schema("/points").unwrap();
        assert_eq!(schema.fields().len(), 3);
        assert!(schema.field_with_name("x").is_ok());
        assert!(schema.field_with_name("y").is_ok());
        assert!(schema.field_with_name("z").is_ok());
    }

    #[test]
    fn test_definition_and_registry() {
        let data = create_cdr_mcap();
        let mut reader = CdrReader::new(Cursor::new(data));

        let msg = reader.next_raw_message().unwrap().unwrap();

        let def = reader.definition(msg.schema_id).unwrap();
        assert_eq!(def.fields.len(), 3);

        let _registry = reader.registry(msg.schema_id).unwrap();
    }
}
