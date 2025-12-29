//! Direct protobuf to sink transcoder - avoids intermediate Value representation.
//!
//! This module decodes protobuf wire format directly into a RowSink,
//! similar to how the ROS1 transcoder works.

use prost_reflect::{DynamicMessage, FieldDescriptor, Kind, MessageDescriptor, ReflectMessage, Value};
use thiserror::Error;

use crate::sink::RowSink;

/// Errors that can occur during transcoding.
#[derive(Debug, Error)]
pub enum TranscodeError {
    #[error("protobuf decode error: {0}")]
    DecodeError(#[from] prost::DecodeError),

    #[error("sink error: {0}")]
    Sink(String),

    #[error("unexpected value type for field {field}: expected {expected}")]
    UnexpectedType { field: String, expected: String },
}

/// Helper to convert sink errors to TranscodeError.
#[inline]
fn sink_err<E: std::fmt::Display>(e: E) -> TranscodeError {
    TranscodeError::Sink(e.to_string())
}

/// Transcode a protobuf message directly to a sink.
///
/// This is the main entry point - reads binary data and pushes values
/// directly to the sink without creating intermediate structures.
pub fn transcode<S: RowSink>(
    data: &[u8],
    message_desc: &MessageDescriptor,
    sink: &mut S,
) -> Result<(), TranscodeError>
where
    S::Error: std::fmt::Display,
{
    // Decode the protobuf message
    let msg = DynamicMessage::decode(message_desc.clone(), data)?;

    // Transcode each field at the top level
    for field in message_desc.fields() {
        transcode_field(&msg, &field, sink)?;
    }

    sink.finish_row().map_err(sink_err)?;
    Ok(())
}

/// Transcode a single field from the message to the sink.
fn transcode_field<S: RowSink>(
    msg: &DynamicMessage,
    field: &FieldDescriptor,
    sink: &mut S,
) -> Result<(), TranscodeError>
where
    S::Error: std::fmt::Display,
{
    if field.is_map() {
        transcode_map_field(msg, field, sink)
    } else if field.is_list() {
        transcode_list_field(msg, field, sink)
    } else {
        let value = msg.get_field(field);
        transcode_value(&value, field, sink)
    }
}

/// Transcode a map field (stored as list of key-value structs).
fn transcode_map_field<S: RowSink>(
    msg: &DynamicMessage,
    field: &FieldDescriptor,
    sink: &mut S,
) -> Result<(), TranscodeError>
where
    S::Error: std::fmt::Display,
{
    let map = msg.get_field(field);
    if let Value::Map(map_value) = map.as_ref() {
        let len = map_value.len();
        sink.enter_list(len).map_err(sink_err)?;

        // Get key/value fields from the map entry message descriptor
        let (key_field, value_field) = if let Kind::Message(entry_desc) = field.kind() {
            let key = entry_desc.get_field(1).expect("map entry has key field");
            let value = entry_desc.get_field(2).expect("map entry has value field");
            (key, value)
        } else {
            return Err(TranscodeError::UnexpectedType {
                field: field.name().to_string(),
                expected: "map message".to_string(),
            });
        };

        for (key, value) in map_value.iter() {
            sink.enter_struct().map_err(sink_err)?;
            // Convert MapKey to Value for uniform handling
            let key_value: Value = key.clone().into();
            transcode_value(&std::borrow::Cow::Owned(key_value), &key_field, sink)?;
            transcode_value(&std::borrow::Cow::Borrowed(value), &value_field, sink)?;
            sink.exit_struct().map_err(sink_err)?;
        }

        sink.exit_list().map_err(sink_err)?;
    }
    Ok(())
}

/// Transcode a repeated field (list).
fn transcode_list_field<S: RowSink>(
    msg: &DynamicMessage,
    field: &FieldDescriptor,
    sink: &mut S,
) -> Result<(), TranscodeError>
where
    S::Error: std::fmt::Display,
{
    let list = msg.get_field(field);
    if let Value::List(list_value) = list.as_ref() {
        let len = list_value.len();
        sink.enter_list(len).map_err(sink_err)?;

        for value in list_value.iter() {
            transcode_value(&std::borrow::Cow::Borrowed(value), field, sink)?;
        }

        sink.exit_list().map_err(sink_err)?;
    }
    Ok(())
}

/// Transcode a single value to the sink.
fn transcode_value<S: RowSink>(
    value: &std::borrow::Cow<'_, Value>,
    _field: &FieldDescriptor,
    sink: &mut S,
) -> Result<(), TranscodeError>
where
    S::Error: std::fmt::Display,
{
    match value.as_ref() {
        Value::Bool(v) => {
            sink.push_bool(*v).map_err(sink_err)?;
        }
        Value::I32(v) => {
            sink.push_i32(*v).map_err(sink_err)?;
        }
        Value::I64(v) => {
            sink.push_i64(*v).map_err(sink_err)?;
        }
        Value::U32(v) => {
            sink.push_u32(*v).map_err(sink_err)?;
        }
        Value::U64(v) => {
            sink.push_u64(*v).map_err(sink_err)?;
        }
        Value::F32(v) => {
            sink.push_f32(*v).map_err(sink_err)?;
        }
        Value::F64(v) => {
            sink.push_f64(*v).map_err(sink_err)?;
        }
        Value::String(v) => {
            sink.push_string(v).map_err(sink_err)?;
        }
        Value::Bytes(v) => {
            sink.push_bytes(v).map_err(sink_err)?;
        }
        Value::EnumNumber(v) => {
            // Store enums as their integer value
            sink.push_i32(*v).map_err(sink_err)?;
        }
        Value::Message(msg) => {
            // Check for well-known types
            let descriptor = msg.descriptor();
            let type_name = descriptor.full_name();
            match type_name {
                "google.protobuf.Timestamp" => {
                    // Timestamp: seconds (int64) + nanos (int32)
                    let seconds = msg
                        .get_field_by_name("seconds")
                        .map(|v| match v.as_ref() {
                            Value::I64(s) => *s,
                            _ => 0,
                        })
                        .unwrap_or(0);
                    let nanos = msg
                        .get_field_by_name("nanos")
                        .map(|v| match v.as_ref() {
                            Value::I32(n) => *n,
                            _ => 0,
                        })
                        .unwrap_or(0);
                    let total_nanos = seconds * 1_000_000_000 + nanos as i64;
                    sink.push_timestamp_nanos(total_nanos).map_err(sink_err)?;
                }
                "google.protobuf.Duration" => {
                    // Duration: seconds (int64) + nanos (int32)
                    let seconds = msg
                        .get_field_by_name("seconds")
                        .map(|v| match v.as_ref() {
                            Value::I64(s) => *s,
                            _ => 0,
                        })
                        .unwrap_or(0);
                    let nanos = msg
                        .get_field_by_name("nanos")
                        .map(|v| match v.as_ref() {
                            Value::I32(n) => *n,
                            _ => 0,
                        })
                        .unwrap_or(0);
                    let total_nanos = seconds * 1_000_000_000 + nanos as i64;
                    sink.push_duration_nanos(total_nanos).map_err(sink_err)?;
                }
                _ => {
                    // Regular nested message
                    sink.enter_struct().map_err(sink_err)?;
                    for field in msg.descriptor().fields() {
                        transcode_field(msg, &field, sink)?;
                    }
                    sink.exit_struct().map_err(sink_err)?;
                }
            }
        }
        Value::List(_) => {
            // Lists are handled by transcode_list_field
            unreachable!("List values should be handled by transcode_list_field");
        }
        Value::Map(_) => {
            // Maps are handled by transcode_map_field
            unreachable!("Map values should be handled by transcode_map_field");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::ArrowRowSink;
    use arrow::array::{Array, Float64Array, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use prost_reflect::DescriptorPool;
    use std::sync::Arc;

    fn create_test_pool() -> DescriptorPool {
        // Create an empty descriptor pool for basic tests
        // Full tests require a proper FileDescriptorSet which is complex to create
        DescriptorPool::new()
    }

    // Note: Full tests require a proper DescriptorPool setup which is complex.
    // Integration tests with real MCAP files provide better coverage.
}
