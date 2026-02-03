//! Direct CDR to sink transcoder - avoids intermediate Value representation.
//!
//! This module provides high-performance transcoding of CDR (ROS2) binary data
//! directly into Arrow format via the RowSink trait, bypassing intermediate
//! Value objects.

use thiserror::Error;

use crate::ros1::{FieldType, MessageDefinition, Primitive};
use crate::sink::RowSink;

/// Errors that can occur during CDR transcoding.
#[derive(Debug, Error)]
pub enum CdrTranscodeError {
    #[error("unexpected end of data: needed {needed} bytes at offset {offset}, had {available}")]
    UnexpectedEof {
        needed: usize,
        offset: usize,
        available: usize,
    },

    #[error("invalid UTF-8 string: {0}")]
    InvalidUtf8(String),

    #[error("unresolved nested type: {0}")]
    UnresolvedType(String),

    #[error("sink error: {0}")]
    Sink(String),

    #[error("invalid encapsulation header")]
    InvalidEncapsulation,
}

impl From<crate::arrow::SinkError> for CdrTranscodeError {
    fn from(e: crate::arrow::SinkError) -> Self {
        CdrTranscodeError::Sink(e.to_string())
    }
}

/// Registry of message definitions for resolving nested types.
pub trait CdrTypeRegistry {
    fn get(&self, name: &str) -> Option<&MessageDefinition>;
}

impl CdrTypeRegistry for std::collections::HashMap<String, MessageDefinition> {
    fn get(&self, name: &str) -> Option<&MessageDefinition> {
        std::collections::HashMap::get(self, name)
    }
}

impl CdrTypeRegistry for crate::ros1::MessageRegistry {
    fn get(&self, name: &str) -> Option<&MessageDefinition> {
        crate::ros1::MessageRegistry::get(self, name)
    }
}

/// Check if a field type is an empty struct (recursively).
fn is_empty_struct<R: CdrTypeRegistry>(field_type: &FieldType, registry: &R) -> bool {
    match field_type {
        FieldType::Primitive(_) => false,
        FieldType::Array { .. } => false,
        FieldType::FixedArray { .. } => false,
        FieldType::Nested { package, name } => {
            let full_name = match package {
                Some(pkg) => format!("{}/{}", pkg, name),
                None => name.clone(),
            };
            match registry.get(&full_name) {
                Some(def) => def
                    .fields
                    .iter()
                    .all(|f| is_empty_struct(&f.field_type, registry)),
                None => false,
            }
        }
    }
}

/// A cursor for reading CDR bytes with alignment support during transcoding.
pub struct CdrCursor<'a> {
    data: &'a [u8],
    pos: usize,
    /// Starting offset for alignment calculations (after encapsulation header)
    origin: usize,
}

impl<'a> CdrCursor<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self {
            data,
            pos: 0,
            origin: 0,
        }
    }

    #[inline]
    fn remaining(&self) -> usize {
        self.data.len().saturating_sub(self.pos)
    }

    /// Align to N-byte boundary relative to origin.
    #[inline]
    fn align_to(&mut self, alignment: usize) -> Result<(), CdrTranscodeError> {
        if alignment <= 1 {
            return Ok(());
        }
        let offset_from_origin = self.pos + self.origin;
        let remainder = offset_from_origin % alignment;
        if remainder != 0 {
            let padding = alignment - remainder;
            if self.remaining() < padding {
                return Err(CdrTranscodeError::UnexpectedEof {
                    needed: padding,
                    offset: self.pos,
                    available: self.remaining(),
                });
            }
            self.pos += padding;
        }
        Ok(())
    }

    #[inline]
    fn read_bytes(&mut self, n: usize) -> Result<&'a [u8], CdrTranscodeError> {
        if self.remaining() < n {
            return Err(CdrTranscodeError::UnexpectedEof {
                needed: n,
                offset: self.pos,
                available: self.remaining(),
            });
        }
        let bytes = &self.data[self.pos..self.pos + n];
        self.pos += n;
        Ok(bytes)
    }

    #[inline]
    fn read_u8(&mut self) -> Result<u8, CdrTranscodeError> {
        let bytes = self.read_bytes(1)?;
        Ok(bytes[0])
    }

    #[inline]
    fn read_i8(&mut self) -> Result<i8, CdrTranscodeError> {
        let bytes = self.read_bytes(1)?;
        Ok(bytes[0] as i8)
    }

    #[inline]
    fn read_u16(&mut self) -> Result<u16, CdrTranscodeError> {
        self.align_to(2)?;
        let bytes = self.read_bytes(2)?;
        Ok(u16::from_le_bytes([bytes[0], bytes[1]]))
    }

    #[inline]
    fn read_i16(&mut self) -> Result<i16, CdrTranscodeError> {
        self.align_to(2)?;
        let bytes = self.read_bytes(2)?;
        Ok(i16::from_le_bytes([bytes[0], bytes[1]]))
    }

    #[inline]
    fn read_u32(&mut self) -> Result<u32, CdrTranscodeError> {
        self.align_to(4)?;
        let bytes = self.read_bytes(4)?;
        Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    #[inline]
    fn read_i32(&mut self) -> Result<i32, CdrTranscodeError> {
        self.align_to(4)?;
        let bytes = self.read_bytes(4)?;
        Ok(i32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    #[inline]
    fn read_u64(&mut self) -> Result<u64, CdrTranscodeError> {
        self.align_to(8)?;
        let bytes = self.read_bytes(8)?;
        Ok(u64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    #[inline]
    fn read_i64(&mut self) -> Result<i64, CdrTranscodeError> {
        self.align_to(8)?;
        let bytes = self.read_bytes(8)?;
        Ok(i64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    #[inline]
    fn read_f32(&mut self) -> Result<f32, CdrTranscodeError> {
        self.align_to(4)?;
        let bytes = self.read_bytes(4)?;
        Ok(f32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    #[inline]
    fn read_f64(&mut self) -> Result<f64, CdrTranscodeError> {
        self.align_to(8)?;
        let bytes = self.read_bytes(8)?;
        Ok(f64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    /// Read a CDR string (length includes null terminator).
    fn read_string(&mut self) -> Result<&'a str, CdrTranscodeError> {
        let len = self.read_u32()? as usize;
        if len == 0 {
            return Ok("");
        }
        let bytes = self.read_bytes(len)?;
        // CDR strings include null terminator in length, so strip it
        let string_bytes = if !bytes.is_empty() && bytes[bytes.len() - 1] == 0 {
            &bytes[..bytes.len() - 1]
        } else {
            bytes
        };
        std::str::from_utf8(string_bytes).map_err(|e| CdrTranscodeError::InvalidUtf8(e.to_string()))
    }

    /// Skip the 4-byte CDR encapsulation header and set origin for alignment.
    fn skip_encapsulation(&mut self) -> Result<(), CdrTranscodeError> {
        if self.remaining() < 4 {
            return Err(CdrTranscodeError::InvalidEncapsulation);
        }
        // Skip 2 bytes encoding + 2 bytes options
        self.read_bytes(4)?;
        // Set origin after encapsulation header
        self.origin = self.pos;
        Ok(())
    }
}

/// Helper to convert sink errors to CdrTranscodeError.
#[inline]
fn sink_err<E: std::fmt::Display>(e: E) -> CdrTranscodeError {
    CdrTranscodeError::Sink(e.to_string())
}

/// Get the alignment requirement for a field type.
#[inline]
fn field_alignment(field_type: &FieldType) -> usize {
    match field_type {
        FieldType::Primitive(p) => match p {
            Primitive::Bool | Primitive::Int8 | Primitive::UInt8 => 1,
            Primitive::Int16 | Primitive::UInt16 => 2,
            Primitive::Int32 | Primitive::UInt32 | Primitive::Float32 => 4,
            Primitive::Int64 | Primitive::UInt64 | Primitive::Float64 => 8,
            Primitive::String => 4, // Length prefix is u32
            Primitive::Time | Primitive::Duration => 4, // First field is i32
        },
        FieldType::Array { .. } | FieldType::FixedArray { .. } => 4, // Length prefix is u32
        FieldType::Nested { .. } => 1, // Nested structs align to their first field
    }
}

/// Transcode a primitive value from cursor to sink.
#[inline]
fn transcode_primitive<S: RowSink>(
    cursor: &mut CdrCursor,
    primitive: Primitive,
    sink: &mut S,
) -> Result<(), CdrTranscodeError>
where
    S::Error: std::fmt::Display,
{
    match primitive {
        Primitive::Bool => {
            let byte = cursor.read_u8()?;
            sink.push_bool(byte != 0).map_err(sink_err)?;
        }
        Primitive::Int8 => {
            sink.push_i8(cursor.read_i8()?).map_err(sink_err)?;
        }
        Primitive::Int16 => {
            sink.push_i16(cursor.read_i16()?).map_err(sink_err)?;
        }
        Primitive::Int32 => {
            sink.push_i32(cursor.read_i32()?).map_err(sink_err)?;
        }
        Primitive::Int64 => {
            sink.push_i64(cursor.read_i64()?).map_err(sink_err)?;
        }
        Primitive::UInt8 => {
            sink.push_u8(cursor.read_u8()?).map_err(sink_err)?;
        }
        Primitive::UInt16 => {
            sink.push_u16(cursor.read_u16()?).map_err(sink_err)?;
        }
        Primitive::UInt32 => {
            sink.push_u32(cursor.read_u32()?).map_err(sink_err)?;
        }
        Primitive::UInt64 => {
            sink.push_u64(cursor.read_u64()?).map_err(sink_err)?;
        }
        Primitive::Float32 => {
            sink.push_f32(cursor.read_f32()?).map_err(sink_err)?;
        }
        Primitive::Float64 => {
            sink.push_f64(cursor.read_f64()?).map_err(sink_err)?;
        }
        Primitive::String => {
            let s = cursor.read_string()?;
            sink.push_string(s).map_err(sink_err)?;
        }
        Primitive::Time => {
            // ROS2 Time: sec (i32) + nanosec (u32)
            let sec = cursor.read_i32()?;
            let nsec = cursor.read_u32()?;
            let nanos = (sec as i64) * 1_000_000_000 + (nsec as i64);
            sink.push_timestamp_nanos(nanos).map_err(sink_err)?;
        }
        Primitive::Duration => {
            // ROS2 Duration: sec (i32) + nanosec (u32)
            let sec = cursor.read_i32()?;
            let nsec = cursor.read_u32()?;
            let nanos = (sec as i64) * 1_000_000_000 + (nsec as i64);
            sink.push_duration_nanos(nanos).map_err(sink_err)?;
        }
    }
    Ok(())
}

/// Transcode a field type from cursor to sink.
fn transcode_field_type<S: RowSink, R: CdrTypeRegistry>(
    cursor: &mut CdrCursor,
    field_type: &FieldType,
    registry: &R,
    sink: &mut S,
) -> Result<(), CdrTranscodeError>
where
    S::Error: std::fmt::Display,
{
    match field_type {
        FieldType::Primitive(p) => transcode_primitive(cursor, *p, sink),

        FieldType::Array { element } => {
            let count = cursor.read_u32()? as usize;
            // Optimize for primitive byte arrays (no alignment needed between elements)
            match element.as_ref() {
                FieldType::Primitive(Primitive::UInt8) => {
                    let bytes = cursor.read_bytes(count)?;
                    sink.push_u8_array(bytes).map_err(sink_err)?;
                }
                FieldType::Primitive(Primitive::Int8) => {
                    let bytes = cursor.read_bytes(count)?;
                    sink.push_u8_array(bytes).map_err(sink_err)?;
                }
                _ => {
                    sink.enter_list(count).map_err(sink_err)?;
                    for _ in 0..count {
                        // Align before each element
                        cursor.align_to(field_alignment(element))?;
                        transcode_field_type(cursor, element, registry, sink)?;
                    }
                    sink.exit_list().map_err(sink_err)?;
                }
            }
            Ok(())
        }

        FieldType::FixedArray { element, length } => {
            // Optimize for byte arrays
            match element.as_ref() {
                FieldType::Primitive(Primitive::UInt8) => {
                    let bytes = cursor.read_bytes(*length)?;
                    sink.push_u8_array(bytes).map_err(sink_err)?;
                }
                FieldType::Primitive(Primitive::Int8) => {
                    let bytes = cursor.read_bytes(*length)?;
                    sink.push_u8_array(bytes).map_err(sink_err)?;
                }
                _ => {
                    sink.enter_list(*length).map_err(sink_err)?;
                    for i in 0..*length {
                        // Align before each element (except first)
                        if i > 0 {
                            cursor.align_to(field_alignment(element))?;
                        }
                        transcode_field_type(cursor, element, registry, sink)?;
                    }
                    sink.exit_list().map_err(sink_err)?;
                }
            }
            Ok(())
        }

        FieldType::Nested { package, name } => {
            let full_name = match package {
                Some(pkg) => format!("{}/{}", pkg, name),
                None => name.clone(),
            };

            let nested_def = registry
                .get(&full_name)
                .ok_or(CdrTranscodeError::UnresolvedType(full_name))?;

            transcode_message(cursor, nested_def, registry, sink)
        }
    }
}

/// Transcode a complete message from cursor to sink.
fn transcode_message<S: RowSink, R: CdrTypeRegistry>(
    cursor: &mut CdrCursor,
    definition: &MessageDefinition,
    registry: &R,
    sink: &mut S,
) -> Result<(), CdrTranscodeError>
where
    S::Error: std::fmt::Display,
{
    sink.enter_struct().map_err(sink_err)?;
    for field in &definition.fields {
        // Skip empty struct fields
        if is_empty_struct(&field.field_type, registry) {
            continue;
        }
        // Align before each field
        cursor.align_to(field_alignment(&field.field_type))?;
        transcode_field_type(cursor, &field.field_type, registry, sink)?;
    }
    sink.exit_struct().map_err(sink_err)?;
    Ok(())
}

/// Transcode a CDR message directly to a sink.
///
/// This is the main entry point - reads CDR binary data (with encapsulation header)
/// and pushes values directly to the sink without creating intermediate Value objects.
pub fn transcode<S: RowSink, R: CdrTypeRegistry>(
    data: &[u8],
    definition: &MessageDefinition,
    registry: &R,
    sink: &mut S,
) -> Result<(), CdrTranscodeError>
where
    S::Error: std::fmt::Display,
{
    let mut cursor = CdrCursor::new(data);
    cursor.skip_encapsulation()?;

    // Transcode each field at the top level (no enter_struct for root)
    for field in &definition.fields {
        // Skip empty struct fields
        if is_empty_struct(&field.field_type, registry) {
            continue;
        }
        // Align before each field
        cursor.align_to(field_alignment(&field.field_type))?;
        transcode_field_type(&mut cursor, &field.field_type, registry, sink)?;
    }

    sink.finish_row().map_err(sink_err)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::ArrowRowSink;
    use crate::ros1::MessageRegistry;
    use arrow::array::{Array, Float64Array, Int32Array, StringArray, StructArray};
    use arrow::datatypes::{DataType, Field, Fields, Schema, TimeUnit};
    use std::sync::Arc;

    /// Helper to create CDR data with encapsulation header
    fn with_encapsulation(data: &[u8]) -> Vec<u8> {
        let mut result = vec![0x00, 0x01, 0x00, 0x00]; // CDR_LE header
        result.extend_from_slice(data);
        result
    }

    #[test]
    fn test_transcode_simple_point() {
        let def = crate::ros1::MessageDefinition::parse("float64 x\nfloat64 y\nfloat64 z").unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Float64, false),
            Field::new("y", DataType::Float64, false),
            Field::new("z", DataType::Float64, false),
        ]));

        let mut sink = ArrowRowSink::new(schema).unwrap();
        let registry = MessageRegistry::new();

        let mut data = Vec::new();
        data.extend_from_slice(&1.5f64.to_le_bytes());
        data.extend_from_slice(&2.5f64.to_le_bytes());
        data.extend_from_slice(&3.5f64.to_le_bytes());

        transcode(&with_encapsulation(&data), &def, &registry, &mut sink).unwrap();

        let batch = sink.finish().unwrap();
        assert_eq!(batch.num_rows(), 1);

        let x = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let y = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let z = batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        assert_eq!(x.value(0), 1.5);
        assert_eq!(y.value(0), 2.5);
        assert_eq!(z.value(0), 3.5);
    }

    #[test]
    fn test_transcode_with_alignment() {
        // bool (1 byte) + padding (3 bytes) + int32 (4 bytes)
        let def = crate::ros1::MessageDefinition::parse("bool a\nint32 b").unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Boolean, false),
            Field::new("b", DataType::Int32, false),
        ]));

        let mut sink = ArrowRowSink::new(schema).unwrap();
        let registry = MessageRegistry::new();

        let mut data = Vec::new();
        data.push(1); // bool
        data.extend_from_slice(&[0, 0, 0]); // padding
        data.extend_from_slice(&42i32.to_le_bytes());

        transcode(&with_encapsulation(&data), &def, &registry, &mut sink).unwrap();

        let batch = sink.finish().unwrap();
        let b = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(b.value(0), 42);
    }

    #[test]
    fn test_transcode_with_string() {
        let def = crate::ros1::MessageDefinition::parse("int32 seq\nstring frame_id").unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("seq", DataType::Int32, false),
            Field::new("frame_id", DataType::Utf8, false),
        ]));

        let mut sink = ArrowRowSink::new(schema).unwrap();
        let registry = MessageRegistry::new();

        let mut data = Vec::new();
        data.extend_from_slice(&42i32.to_le_bytes());
        data.extend_from_slice(&10u32.to_le_bytes()); // CDR length includes null
        data.extend_from_slice(b"base_link\0");

        transcode(&with_encapsulation(&data), &def, &registry, &mut sink).unwrap();

        let batch = sink.finish().unwrap();

        let seq = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let frame = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert_eq!(seq.value(0), 42);
        assert_eq!(frame.value(0), "base_link");
    }

    #[test]
    fn test_transcode_nested_message() {
        let mut registry = MessageRegistry::new();

        let point_def =
            crate::ros1::MessageDefinition::parse("float64 x\nfloat64 y\nfloat64 z").unwrap();
        registry.register("geometry_msgs/Point", point_def);

        let def = crate::ros1::MessageDefinition::parse("geometry_msgs/Point position").unwrap();

        let point_fields = Fields::from(vec![
            Field::new("x", DataType::Float64, false),
            Field::new("y", DataType::Float64, false),
            Field::new("z", DataType::Float64, false),
        ]);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "position",
            DataType::Struct(point_fields),
            false,
        )]));

        let mut sink = ArrowRowSink::new(schema).unwrap();

        let mut data = Vec::new();
        data.extend_from_slice(&1.0f64.to_le_bytes());
        data.extend_from_slice(&2.0f64.to_le_bytes());
        data.extend_from_slice(&3.0f64.to_le_bytes());

        transcode(&with_encapsulation(&data), &def, &registry, &mut sink).unwrap();

        let batch = sink.finish().unwrap();
        assert_eq!(batch.num_rows(), 1);

        let position = batch
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let x = position
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        assert_eq!(x.value(0), 1.0);
    }

    #[test]
    fn test_transcode_timestamp() {
        let def = crate::ros1::MessageDefinition::parse("time stamp").unwrap();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "stamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        )]));

        let mut sink = ArrowRowSink::new(schema).unwrap();
        let registry = MessageRegistry::new();

        let mut data = Vec::new();
        data.extend_from_slice(&1000i32.to_le_bytes()); // sec (i32 in ROS2)
        data.extend_from_slice(&500u32.to_le_bytes()); // nsec

        transcode(&with_encapsulation(&data), &def, &registry, &mut sink).unwrap();

        let batch = sink.finish().unwrap();
        let stamp = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::TimestampNanosecondArray>()
            .unwrap();

        assert_eq!(stamp.value(0), 1_000_000_000_500i64);
    }
}
