//! Protobuf serialization using the generic RowSource trait.
//!
//! This module provides `ProtobufWriter`, which serializes data from any `RowSource`
//! implementation to protobuf binary format.

use std::fmt;

use prost::encoding::{encode_varint, WireType};
use prost_reflect::{FieldDescriptor, Kind, MessageDescriptor};
use thiserror::Error;

use crate::source::RowSource;

/// Errors that can occur during protobuf serialization via RowSource.
#[derive(Debug, Error)]
pub enum WriteError {
    #[error("source error: {0}")]
    Source(String),

    #[error("unsupported protobuf type: {0}")]
    UnsupportedType(String),

    #[error("missing field descriptor for {0}")]
    MissingField(String),
}

/// A writer that serializes data from a `RowSource` to protobuf binary format.
pub struct ProtobufWriter {
    buffer: Vec<u8>,
}

impl ProtobufWriter {
    /// Create a new writer with default buffer capacity.
    pub fn new() -> Self {
        Self {
            buffer: Vec::with_capacity(1024),
        }
    }

    /// Create a new writer with specified buffer capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            buffer: Vec::with_capacity(capacity),
        }
    }

    /// Clear the internal buffer for reuse.
    pub fn clear(&mut self) {
        self.buffer.clear();
    }

    /// Get the serialized bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.buffer
    }

    /// Take ownership of the serialized bytes.
    pub fn into_bytes(self) -> Vec<u8> {
        self.buffer
    }

    /// Serialize a single row from a `RowSource` using a message descriptor.
    pub fn write_row<S: RowSource>(
        &mut self,
        source: &mut S,
        message_desc: &MessageDescriptor,
    ) -> Result<(), WriteError>
    where
        S::Error: fmt::Display,
    {
        for field in message_desc.fields() {
            self.write_field(source, &field)?;
        }
        Ok(())
    }

    /// Serialize a single field from the source.
    fn write_field<S: RowSource>(
        &mut self,
        source: &mut S,
        field: &FieldDescriptor,
    ) -> Result<(), WriteError>
    where
        S::Error: fmt::Display,
    {
        let field_number = field.number();

        if field.is_list() && !field.is_map() {
            return self.write_repeated_field(source, field);
        }

        if field.is_map() {
            return self.write_map_field(source, field);
        }

        // Write tag
        let wire_type = kind_to_wire_type(field.kind());
        self.write_tag(field_number, wire_type);

        // Write value
        self.write_value(source, field.kind(), field)
    }

    /// Write a repeated field.
    fn write_repeated_field<S: RowSource>(
        &mut self,
        source: &mut S,
        field: &FieldDescriptor,
    ) -> Result<(), WriteError>
    where
        S::Error: fmt::Display,
    {
        let len = source
            .enter_list()
            .map_err(|e| WriteError::Source(e.to_string()))?;

        let wire_type = kind_to_wire_type(field.kind());

        for _ in 0..len {
            self.write_tag(field.number(), wire_type);
            self.write_value(source, field.kind(), field)?;
        }

        source
            .exit_list()
            .map_err(|e| WriteError::Source(e.to_string()))?;
        Ok(())
    }

    /// Write a map field (as repeated key-value entries).
    fn write_map_field<S: RowSource>(
        &mut self,
        source: &mut S,
        field: &FieldDescriptor,
    ) -> Result<(), WriteError>
    where
        S::Error: fmt::Display,
    {
        let len = source
            .enter_list()
            .map_err(|e| WriteError::Source(e.to_string()))?;

        // Get key/value fields from the map entry message descriptor
        let (key_field, value_field) = if let Kind::Message(entry_desc) = field.kind() {
            let key = entry_desc
                .get_field(1)
                .ok_or_else(|| WriteError::MissingField("map key".to_string()))?;
            let value = entry_desc
                .get_field(2)
                .ok_or_else(|| WriteError::MissingField("map value".to_string()))?;
            (key, value)
        } else {
            return Err(WriteError::UnsupportedType(
                "map without message kind".to_string(),
            ));
        };

        for _ in 0..len {
            // Each map entry is a message with key (field 1) and value (field 2)
            source
                .enter_struct()
                .map_err(|e| WriteError::Source(e.to_string()))?;

            // Write the entry as a length-delimited message
            self.write_tag(field.number(), WireType::LengthDelimited);

            // Serialize entry to temp buffer to get length
            let mut entry_buf = Vec::new();
            std::mem::swap(&mut self.buffer, &mut entry_buf);

            // Key (field 1)
            self.write_tag(1, kind_to_wire_type(key_field.kind()));
            self.write_value(source, key_field.kind(), &key_field)?;

            // Value (field 2)
            self.write_tag(2, kind_to_wire_type(value_field.kind()));
            self.write_value(source, value_field.kind(), &value_field)?;

            let entry_data = std::mem::take(&mut self.buffer);
            self.buffer = entry_buf;

            // Write length prefix and data
            encode_varint(entry_data.len() as u64, &mut self.buffer);
            self.buffer.extend_from_slice(&entry_data);

            source
                .exit_struct()
                .map_err(|e| WriteError::Source(e.to_string()))?;
        }

        source
            .exit_list()
            .map_err(|e| WriteError::Source(e.to_string()))?;
        Ok(())
    }

    /// Write a value based on its kind.
    fn write_value<S: RowSource>(
        &mut self,
        source: &mut S,
        kind: Kind,
        _field: &FieldDescriptor,
    ) -> Result<(), WriteError>
    where
        S::Error: fmt::Display,
    {
        match kind {
            Kind::Bool => {
                let v = source
                    .read_bool()
                    .map_err(|e| WriteError::Source(e.to_string()))?;
                self.write_bool(v);
            }
            Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 => {
                let v = source
                    .read_i32()
                    .map_err(|e| WriteError::Source(e.to_string()))?;
                self.write_i32(v, kind);
            }
            Kind::Int64 | Kind::Sint64 | Kind::Sfixed64 => {
                let v = source
                    .read_i64()
                    .map_err(|e| WriteError::Source(e.to_string()))?;
                self.write_i64(v, kind);
            }
            Kind::Uint32 | Kind::Fixed32 => {
                let v = source
                    .read_u32()
                    .map_err(|e| WriteError::Source(e.to_string()))?;
                self.write_u32(v, kind);
            }
            Kind::Uint64 | Kind::Fixed64 => {
                let v = source
                    .read_u64()
                    .map_err(|e| WriteError::Source(e.to_string()))?;
                self.write_u64(v, kind);
            }
            Kind::Float => {
                let v = source
                    .read_f32()
                    .map_err(|e| WriteError::Source(e.to_string()))?;
                self.write_f32(v);
            }
            Kind::Double => {
                let v = source
                    .read_f64()
                    .map_err(|e| WriteError::Source(e.to_string()))?;
                self.write_f64(v);
            }
            Kind::String => {
                let v = source
                    .read_string()
                    .map_err(|e| WriteError::Source(e.to_string()))?;
                self.write_string(&v);
            }
            Kind::Bytes => {
                let v = source
                    .read_bytes()
                    .map_err(|e| WriteError::Source(e.to_string()))?;
                self.write_bytes(&v);
            }
            Kind::Enum(_) => {
                let v = source
                    .read_i32()
                    .map_err(|e| WriteError::Source(e.to_string()))?;
                encode_varint(v as u64, &mut self.buffer);
            }
            Kind::Message(msg_desc) => {
                match msg_desc.full_name() {
                    "google.protobuf.Timestamp" => {
                        let nanos = source
                            .read_timestamp_nanos()
                            .map_err(|e| WriteError::Source(e.to_string()))?;
                        let seconds = nanos / 1_000_000_000;
                        let nano_part = (nanos % 1_000_000_000) as i32;

                        // Serialize timestamp message
                        let mut msg_buf = Vec::new();
                        // seconds (field 1, varint)
                        if seconds != 0 {
                            msg_buf.push((1 << 3) | WireType::Varint as u8);
                            encode_varint(seconds as u64, &mut msg_buf);
                        }
                        // nanos (field 2, varint)
                        if nano_part != 0 {
                            msg_buf.push((2 << 3) | WireType::Varint as u8);
                            encode_varint(nano_part as u64, &mut msg_buf);
                        }

                        encode_varint(msg_buf.len() as u64, &mut self.buffer);
                        self.buffer.extend_from_slice(&msg_buf);
                    }
                    "google.protobuf.Duration" => {
                        let nanos = source
                            .read_duration_nanos()
                            .map_err(|e| WriteError::Source(e.to_string()))?;
                        let seconds = nanos / 1_000_000_000;
                        let nano_part = (nanos % 1_000_000_000) as i32;

                        let mut msg_buf = Vec::new();
                        if seconds != 0 {
                            msg_buf.push((1 << 3) | WireType::Varint as u8);
                            encode_varint(seconds as u64, &mut msg_buf);
                        }
                        if nano_part != 0 {
                            msg_buf.push((2 << 3) | WireType::Varint as u8);
                            encode_varint(nano_part as u64, &mut msg_buf);
                        }

                        encode_varint(msg_buf.len() as u64, &mut self.buffer);
                        self.buffer.extend_from_slice(&msg_buf);
                    }
                    _ => {
                        // Regular nested message - serialize to temp buffer first
                        source
                            .enter_struct()
                            .map_err(|e| WriteError::Source(e.to_string()))?;

                        let mut msg_buf = Vec::new();
                        std::mem::swap(&mut self.buffer, &mut msg_buf);

                        for nested_field in msg_desc.fields() {
                            self.write_field(source, &nested_field)?;
                        }

                        let nested_data = std::mem::take(&mut self.buffer);
                        self.buffer = msg_buf;

                        // Write length prefix and data
                        encode_varint(nested_data.len() as u64, &mut self.buffer);
                        self.buffer.extend_from_slice(&nested_data);

                        source
                            .exit_struct()
                            .map_err(|e| WriteError::Source(e.to_string()))?;
                    }
                }
            }
        }
        Ok(())
    }

    /// Write a tag (field number + wire type).
    fn write_tag(&mut self, field_number: u32, wire_type: WireType) {
        let tag = (field_number << 3) | (wire_type as u32);
        encode_varint(tag as u64, &mut self.buffer);
    }

    fn write_bool(&mut self, value: bool) {
        self.buffer.push(if value { 1 } else { 0 });
    }

    fn write_i32(&mut self, value: i32, kind: Kind) {
        match kind {
            Kind::Sint32 => {
                // ZigZag encoding
                let encoded = ((value << 1) ^ (value >> 31)) as u32;
                encode_varint(encoded as u64, &mut self.buffer);
            }
            Kind::Sfixed32 => {
                self.buffer.extend_from_slice(&value.to_le_bytes());
            }
            _ => {
                encode_varint(value as u64, &mut self.buffer);
            }
        }
    }

    fn write_i64(&mut self, value: i64, kind: Kind) {
        match kind {
            Kind::Sint64 => {
                // ZigZag encoding
                let encoded = ((value << 1) ^ (value >> 63)) as u64;
                encode_varint(encoded, &mut self.buffer);
            }
            Kind::Sfixed64 => {
                self.buffer.extend_from_slice(&value.to_le_bytes());
            }
            _ => {
                encode_varint(value as u64, &mut self.buffer);
            }
        }
    }

    fn write_u32(&mut self, value: u32, kind: Kind) {
        match kind {
            Kind::Fixed32 => {
                self.buffer.extend_from_slice(&value.to_le_bytes());
            }
            _ => {
                encode_varint(value as u64, &mut self.buffer);
            }
        }
    }

    fn write_u64(&mut self, value: u64, kind: Kind) {
        match kind {
            Kind::Fixed64 => {
                self.buffer.extend_from_slice(&value.to_le_bytes());
            }
            _ => {
                encode_varint(value, &mut self.buffer);
            }
        }
    }

    fn write_f32(&mut self, value: f32) {
        self.buffer.extend_from_slice(&value.to_le_bytes());
    }

    fn write_f64(&mut self, value: f64) {
        self.buffer.extend_from_slice(&value.to_le_bytes());
    }

    fn write_string(&mut self, value: &str) {
        encode_varint(value.len() as u64, &mut self.buffer);
        self.buffer.extend_from_slice(value.as_bytes());
    }

    fn write_bytes(&mut self, value: &[u8]) {
        encode_varint(value.len() as u64, &mut self.buffer);
        self.buffer.extend_from_slice(value);
    }
}

impl Default for ProtobufWriter {
    fn default() -> Self {
        Self::new()
    }
}

/// Get the wire type for a protobuf kind.
fn kind_to_wire_type(kind: Kind) -> WireType {
    match kind {
        Kind::Bool
        | Kind::Int32
        | Kind::Int64
        | Kind::Uint32
        | Kind::Uint64
        | Kind::Sint32
        | Kind::Sint64
        | Kind::Enum(_) => WireType::Varint,
        Kind::Fixed32 | Kind::Sfixed32 | Kind::Float => WireType::ThirtyTwoBit,
        Kind::Fixed64 | Kind::Sfixed64 | Kind::Double => WireType::SixtyFourBit,
        Kind::String | Kind::Bytes | Kind::Message(_) => WireType::LengthDelimited,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_varint_encoding() {
        let mut buf = Vec::new();
        encode_varint(300, &mut buf);
        assert_eq!(buf, vec![0xAC, 0x02]);
    }
}
