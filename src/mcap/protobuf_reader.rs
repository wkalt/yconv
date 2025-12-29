//! High-level protobuf-aware MCAP reader that deserializes messages.

use std::collections::HashMap;
use std::io::Read;
use std::sync::Arc;

use arrow::datatypes::Schema;
use prost_reflect::MessageDescriptor;
use thiserror::Error;

use crate::mcap::{ChannelInfo, ReaderError, StreamReader};
use crate::protobuf::{ProtobufSchema, ProtobufSchemaError};

/// Errors that can occur while reading protobuf messages from MCAP.
#[derive(Debug, Error)]
pub enum ProtobufReaderError {
    #[error("MCAP reader error: {0}")]
    Reader(#[from] ReaderError),

    #[error("schema parsing error: {0}")]
    Schema(#[from] ProtobufSchemaError),

    #[error("schema encoding '{0}' is not protobuf")]
    UnsupportedEncoding(String),

    #[error("channel {channel_id} references unknown schema {schema_id}")]
    UnknownSchema { channel_id: u16, schema_id: u16 },

    #[error("message references unknown channel {0}")]
    UnknownChannel(u16),
}

/// A raw protobuf message with schema info for direct transcoding.
pub struct RawProtobufMessage {
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
    /// Raw message bytes.
    pub data: Vec<u8>,
    /// Schema ID for looking up definition.
    pub schema_id: u16,
}

/// Cached information about a parsed protobuf schema.
struct ParsedSchema {
    /// The parsed protobuf schema with descriptor pool.
    schema: ProtobufSchema,
    /// The Arrow schema for this message type.
    arrow_schema: Arc<Schema>,
}

/// High-level reader for protobuf-encoded MCAP files.
pub struct ProtobufReader<R: Read> {
    inner: StreamReader<R>,
    /// Parsed schemas by schema ID.
    parsed_schemas: HashMap<u16, ParsedSchema>,
    /// Arrow schemas by topic name.
    topic_schemas: HashMap<String, Arc<Schema>>,
}

impl<R: Read> ProtobufReader<R> {
    /// Create a new protobuf MCAP reader.
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
    pub fn channels(&self) -> &std::collections::HashMap<u16, ChannelInfo> {
        self.inner.channels()
    }

    /// Get all schemas discovered so far.
    pub fn schemas(&self) -> &std::collections::HashMap<u16, crate::mcap::SchemaInfo> {
        self.inner.schemas()
    }

    /// Read the next raw message without deserializing.
    ///
    /// Use `message_descriptor()` to get schema info for transcoding.
    pub fn next_raw_message(&mut self) -> Result<Option<RawProtobufMessage>, ProtobufReaderError> {
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
                    .ok_or(ProtobufReaderError::UnknownChannel(raw_msg.channel_id))?;
                (
                    channel.message_encoding.clone(),
                    channel.schema_id,
                    channel.topic.clone(),
                )
            };

            // Only handle protobuf-encoded messages
            if message_encoding != "protobuf" {
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

            return Ok(Some(RawProtobufMessage {
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
    fn parse_schema(&mut self, schema_id: u16) -> Result<(), ProtobufReaderError> {
        let schema_info = self
            .inner
            .schema(schema_id)
            .ok_or(ProtobufReaderError::UnknownSchema {
                channel_id: 0,
                schema_id,
            })?;

        // Check encoding
        if schema_info.encoding != "protobuf" {
            return Err(ProtobufReaderError::UnsupportedEncoding(
                schema_info.encoding.clone(),
            ));
        }

        // Parse the FileDescriptorSet from schema data
        // The schema name is the fully-qualified message type
        let message_type = schema_info.name.clone();
        let schema = ProtobufSchema::parse(&schema_info.data, &message_type)?;
        let arrow_schema = schema.arrow_schema();

        self.parsed_schemas.insert(
            schema_id,
            ParsedSchema {
                schema,
                arrow_schema,
            },
        );

        Ok(())
    }

    /// Get the message descriptor for a schema ID.
    pub fn message_descriptor(&self, schema_id: u16) -> Option<&MessageDescriptor> {
        self.parsed_schemas
            .get(&schema_id)
            .map(|p| p.schema.message_descriptor())
    }
}
