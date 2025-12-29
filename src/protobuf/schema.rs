//! Protobuf schema parsing and Arrow schema generation.
//!
//! MCAP stores protobuf schemas as serialized FileDescriptorSet (binary protobuf).
//! This module parses them and converts to Arrow schemas.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Fields, Schema, TimeUnit};
use prost::Message;
use prost_reflect::{DescriptorPool, FieldDescriptor, Kind, MessageDescriptor};
use prost_types::FileDescriptorSet;
use thiserror::Error;

/// Errors that can occur during protobuf schema parsing.
#[derive(Debug, Error)]
pub enum ProtobufSchemaError {
    #[error("failed to decode FileDescriptorSet: {0}")]
    DecodeError(#[from] prost::DecodeError),

    #[error("failed to create descriptor pool: {0}")]
    DescriptorPoolError(String),

    #[error("message type not found: {0}")]
    MessageNotFound(String),

    #[error("unsupported protobuf type: {0}")]
    UnsupportedType(String),
}

/// Parsed protobuf schema with type registry.
pub struct ProtobufSchema {
    /// The descriptor pool containing all types.
    pool: DescriptorPool,
    /// The root message descriptor.
    message: MessageDescriptor,
    /// Cached Arrow schema.
    arrow_schema: Arc<Schema>,
}

impl ProtobufSchema {
    /// Parse a protobuf schema from MCAP schema data.
    ///
    /// The schema_data should be a serialized FileDescriptorSet.
    /// The message_name should be the fully-qualified message type.
    pub fn parse(schema_data: &[u8], message_name: &str) -> Result<Self, ProtobufSchemaError> {
        // Decode the FileDescriptorSet
        let fds = FileDescriptorSet::decode(schema_data)?;

        // Create descriptor pool
        let pool = DescriptorPool::from_file_descriptor_set(fds)
            .map_err(|e| ProtobufSchemaError::DescriptorPoolError(e.to_string()))?;

        // Find the message descriptor
        let message = pool
            .get_message_by_name(message_name)
            .ok_or_else(|| ProtobufSchemaError::MessageNotFound(message_name.to_string()))?;

        // Build Arrow schema
        let arrow_schema = Arc::new(Self::message_to_arrow_schema(&message)?);

        Ok(Self {
            pool,
            message,
            arrow_schema,
        })
    }

    /// Get the Arrow schema for this message type.
    pub fn arrow_schema(&self) -> Arc<Schema> {
        Arc::clone(&self.arrow_schema)
    }

    /// Get the message descriptor.
    pub fn message_descriptor(&self) -> &MessageDescriptor {
        &self.message
    }

    /// Get the descriptor pool for resolving nested types.
    pub fn pool(&self) -> &DescriptorPool {
        &self.pool
    }

    /// Convert a protobuf message descriptor to an Arrow schema.
    fn message_to_arrow_schema(msg: &MessageDescriptor) -> Result<Schema, ProtobufSchemaError> {
        let fields: Vec<Field> = msg
            .fields()
            .map(|f| Self::field_to_arrow(&f))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Schema::new(fields))
    }

    /// Convert a protobuf field descriptor to an Arrow field.
    fn field_to_arrow(field: &FieldDescriptor) -> Result<Field, ProtobufSchemaError> {
        let data_type = Self::kind_to_arrow(field.kind(), field)?;

        // Handle repeated fields (lists)
        let data_type = if field.is_map() {
            // Maps are stored as list of key-value structs
            // The underlying message has key (field 1) and value (field 2)
            if let Kind::Message(entry_desc) = field.kind() {
                let key_field = entry_desc.get_field(1).expect("map entry has key field");
                let value_field = entry_desc.get_field(2).expect("map entry has value field");
                let key_type = Self::kind_to_arrow(key_field.kind(), &key_field)?;
                let value_type = Self::kind_to_arrow(value_field.kind(), &value_field)?;
                let entry_fields = Fields::from(vec![
                    Field::new("key", key_type, false),
                    Field::new("value", value_type, true),
                ]);
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::Struct(entry_fields),
                    true,
                )))
            } else {
                return Err(ProtobufSchemaError::UnsupportedType("map without message kind".to_string()));
            }
        } else if field.is_list() {
            DataType::List(Arc::new(Field::new("item", data_type, true)))
        } else {
            data_type
        };

        // Protobuf fields are nullable by default in proto3
        Ok(Field::new(field.name(), data_type, true))
    }

    /// Convert a protobuf kind to an Arrow data type.
    fn kind_to_arrow(
        kind: Kind,
        _field: &FieldDescriptor,
    ) -> Result<DataType, ProtobufSchemaError> {
        match kind {
            Kind::Bool => Ok(DataType::Boolean),
            Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 => Ok(DataType::Int32),
            Kind::Int64 | Kind::Sint64 | Kind::Sfixed64 => Ok(DataType::Int64),
            Kind::Uint32 | Kind::Fixed32 => Ok(DataType::UInt32),
            Kind::Uint64 | Kind::Fixed64 => Ok(DataType::UInt64),
            Kind::Float => Ok(DataType::Float32),
            Kind::Double => Ok(DataType::Float64),
            Kind::String => Ok(DataType::Utf8),
            Kind::Bytes => Ok(DataType::Binary),
            Kind::Enum(_) => {
                // Store enums as int32 (their wire representation)
                Ok(DataType::Int32)
            }
            Kind::Message(msg_desc) => {
                // Handle well-known types
                match msg_desc.full_name() {
                    "google.protobuf.Timestamp" => {
                        Ok(DataType::Timestamp(TimeUnit::Nanosecond, None))
                    }
                    "google.protobuf.Duration" => {
                        Ok(DataType::Duration(TimeUnit::Nanosecond))
                    }
                    _ => {
                        // Regular nested message
                        let fields: Vec<Field> = msg_desc
                            .fields()
                            .map(|f| Self::field_to_arrow(&f))
                            .collect::<Result<Vec<_>, _>>()?;
                        Ok(DataType::Struct(Fields::from(fields)))
                    }
                }
            }
        }
    }

    /// Generate protobuf schema text from Arrow schema (for writing MCAP).
    pub fn arrow_to_proto_schema(
        schema: &Schema,
        message_name: &str,
    ) -> Result<String, ProtobufSchemaError> {
        let mut lines = Vec::new();
        lines.push(format!("syntax = \"proto3\";"));
        lines.push(String::new());
        lines.push(format!("message {} {{", message_name));

        for (i, field) in schema.fields().iter().enumerate() {
            let proto_type = Self::arrow_to_proto_type(field.data_type())?;
            let field_num = i + 1;
            if matches!(field.data_type(), DataType::List(_)) {
                lines.push(format!("  repeated {} {} = {};", proto_type, field.name(), field_num));
            } else {
                lines.push(format!("  {} {} = {};", proto_type, field.name(), field_num));
            }
        }

        lines.push("}".to_string());
        Ok(lines.join("\n"))
    }

    /// Convert Arrow data type to protobuf type string.
    fn arrow_to_proto_type(dt: &DataType) -> Result<&'static str, ProtobufSchemaError> {
        match dt {
            DataType::Boolean => Ok("bool"),
            DataType::Int8 | DataType::Int16 | DataType::Int32 => Ok("int32"),
            DataType::Int64 => Ok("int64"),
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 => Ok("uint32"),
            DataType::UInt64 => Ok("uint64"),
            DataType::Float32 => Ok("float"),
            DataType::Float64 => Ok("double"),
            DataType::Utf8 | DataType::LargeUtf8 => Ok("string"),
            DataType::Binary | DataType::LargeBinary => Ok("bytes"),
            DataType::Timestamp(_, _) => Ok("google.protobuf.Timestamp"),
            DataType::Duration(_) => Ok("google.protobuf.Duration"),
            DataType::List(inner) => Self::arrow_to_proto_type(inner.data_type()),
            DataType::Struct(_) => Err(ProtobufSchemaError::UnsupportedType(
                "nested structs require separate message definitions".to_string(),
            )),
            other => Err(ProtobufSchemaError::UnsupportedType(format!("{:?}", other))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_schema_generation() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]);

        let proto = ProtobufSchema::arrow_to_proto_schema(&schema, "TestMessage").unwrap();
        assert!(proto.contains("message TestMessage"));
        assert!(proto.contains("int32 id = 1"));
        assert!(proto.contains("string name = 2"));
        assert!(proto.contains("double value = 3"));
    }
}
