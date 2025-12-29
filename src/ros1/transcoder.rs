//! Direct ROS1 to sink transcoder - avoids intermediate Value representation.
//!
//! This module provides two transcoding approaches:
//! 1. `transcode()` - Simple API that does type lookups per message (slower)
//! 2. `CompiledTranscoder` - Pre-compiles the execution plan for zero-lookup transcoding (faster)

use thiserror::Error;

use crate::ros1::{FieldType, MessageDefinition, Primitive};
use crate::sink::RowSink;

/// Errors that can occur during transcoding.
#[derive(Debug, Error)]
pub enum TranscodeError {
    #[error("unexpected end of data: needed {needed} bytes, had {available}")]
    UnexpectedEof { needed: usize, available: usize },

    #[error("invalid UTF-8 string: {0}")]
    InvalidUtf8(String),

    #[error("unresolved nested type: {0}")]
    UnresolvedType(String),

    #[error("sink error: {0}")]
    Sink(String),
}

impl From<crate::arrow::SinkError> for TranscodeError {
    fn from(e: crate::arrow::SinkError) -> Self {
        TranscodeError::Sink(e.to_string())
    }
}

/// Registry of message definitions for resolving nested types.
pub trait TypeRegistry {
    fn get(&self, name: &str) -> Option<&MessageDefinition>;
}

impl TypeRegistry for std::collections::HashMap<String, MessageDefinition> {
    fn get(&self, name: &str) -> Option<&MessageDefinition> {
        std::collections::HashMap::get(self, name)
    }
}

impl TypeRegistry for crate::ros1::MessageRegistry {
    fn get(&self, name: &str) -> Option<&MessageDefinition> {
        crate::ros1::MessageRegistry::get(self, name)
    }
}

/// Check if a field type is an empty struct (recursively).
/// Empty structs are skipped in the Arrow schema, so we must skip them during transcoding too.
fn is_empty_struct<R: TypeRegistry>(field_type: &FieldType, registry: &R) -> bool {
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
                Some(def) => def.fields.iter().all(|f| is_empty_struct(&f.field_type, registry)),
                None => false, // Can't determine, assume not empty
            }
        }
    }
}

/// A cursor for reading bytes during transcoding.
struct Cursor<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    fn remaining(&self) -> usize {
        self.data.len() - self.pos
    }

    fn read_bytes(&mut self, n: usize) -> Result<&'a [u8], TranscodeError> {
        if self.remaining() < n {
            return Err(TranscodeError::UnexpectedEof {
                needed: n,
                available: self.remaining(),
            });
        }
        let bytes = &self.data[self.pos..self.pos + n];
        self.pos += n;
        Ok(bytes)
    }

    #[inline]
    fn read_u8(&mut self) -> Result<u8, TranscodeError> {
        let bytes = self.read_bytes(1)?;
        Ok(bytes[0])
    }

    #[inline]
    fn read_i8(&mut self) -> Result<i8, TranscodeError> {
        let bytes = self.read_bytes(1)?;
        Ok(bytes[0] as i8)
    }

    #[inline]
    fn read_u16(&mut self) -> Result<u16, TranscodeError> {
        let bytes = self.read_bytes(2)?;
        Ok(u16::from_le_bytes([bytes[0], bytes[1]]))
    }

    #[inline]
    fn read_i16(&mut self) -> Result<i16, TranscodeError> {
        let bytes = self.read_bytes(2)?;
        Ok(i16::from_le_bytes([bytes[0], bytes[1]]))
    }

    #[inline]
    fn read_u32(&mut self) -> Result<u32, TranscodeError> {
        let bytes = self.read_bytes(4)?;
        Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    #[inline]
    fn read_i32(&mut self) -> Result<i32, TranscodeError> {
        let bytes = self.read_bytes(4)?;
        Ok(i32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    #[inline]
    fn read_u64(&mut self) -> Result<u64, TranscodeError> {
        let bytes = self.read_bytes(8)?;
        Ok(u64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    #[inline]
    fn read_i64(&mut self) -> Result<i64, TranscodeError> {
        let bytes = self.read_bytes(8)?;
        Ok(i64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    #[inline]
    fn read_f32(&mut self) -> Result<f32, TranscodeError> {
        let bytes = self.read_bytes(4)?;
        Ok(f32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    #[inline]
    fn read_f64(&mut self) -> Result<f64, TranscodeError> {
        let bytes = self.read_bytes(8)?;
        Ok(f64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    fn read_string(&mut self) -> Result<&'a str, TranscodeError> {
        let len = self.read_u32()? as usize;
        let bytes = self.read_bytes(len)?;
        std::str::from_utf8(bytes).map_err(|e| TranscodeError::InvalidUtf8(e.to_string()))
    }
}

/// Helper to convert sink errors to TranscodeError.
#[inline]
fn sink_err<E: std::fmt::Display>(e: E) -> TranscodeError {
    TranscodeError::Sink(e.to_string())
}

/// Transcode a primitive value from cursor to sink.
#[inline]
fn transcode_primitive<S: RowSink>(
    cursor: &mut Cursor,
    primitive: Primitive,
    sink: &mut S,
) -> Result<(), TranscodeError>
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
            let sec = cursor.read_u32()?;
            let nsec = cursor.read_u32()?;
            let nanos = (sec as i64) * 1_000_000_000 + (nsec as i64);
            sink.push_timestamp_nanos(nanos).map_err(sink_err)?;
        }
        Primitive::Duration => {
            let sec = cursor.read_i32()?;
            let nsec = cursor.read_i32()?;
            let nanos = (sec as i64) * 1_000_000_000 + (nsec as i64);
            sink.push_duration_nanos(nanos).map_err(sink_err)?;
        }
    }
    Ok(())
}

/// Transcode a field type from cursor to sink.
fn transcode_field_type<S: RowSink, R: TypeRegistry>(
    cursor: &mut Cursor,
    field_type: &FieldType,
    registry: &R,
    sink: &mut S,
) -> Result<(), TranscodeError>
where
    S::Error: std::fmt::Display,
{
    match field_type {
        FieldType::Primitive(p) => transcode_primitive(cursor, *p, sink),

        FieldType::Array { element } => {
            let count = cursor.read_u32()? as usize;
            // Optimize for primitive arrays - use batch operations
            match element.as_ref() {
                FieldType::Primitive(Primitive::UInt8) => {
                    let bytes = cursor.read_bytes(count)?;
                    sink.push_u8_array(bytes).map_err(sink_err)?;
                }
                FieldType::Primitive(Primitive::Float32) => {
                    let bytes = cursor.read_bytes(count * 4)?;
                    // SAFETY: f32 is 4 bytes, and we're reading from little-endian ROS1 data
                    let values: Vec<f32> = bytes
                        .chunks_exact(4)
                        .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
                        .collect();
                    sink.push_f32_array(&values).map_err(sink_err)?;
                }
                FieldType::Primitive(Primitive::Float64) => {
                    let bytes = cursor.read_bytes(count * 8)?;
                    let values: Vec<f64> = bytes
                        .chunks_exact(8)
                        .map(|c| f64::from_le_bytes([c[0], c[1], c[2], c[3], c[4], c[5], c[6], c[7]]))
                        .collect();
                    sink.push_f64_array(&values).map_err(sink_err)?;
                }
                _ => {
                    // Fall back to element-by-element for other types
                    sink.enter_list(count).map_err(sink_err)?;
                    for _ in 0..count {
                        transcode_field_type(cursor, element, registry, sink)?;
                    }
                    sink.exit_list().map_err(sink_err)?;
                }
            }
            Ok(())
        }

        FieldType::FixedArray { element, length } => {
            // Optimize for primitive fixed arrays
            match element.as_ref() {
                FieldType::Primitive(Primitive::UInt8) => {
                    let bytes = cursor.read_bytes(*length)?;
                    sink.push_u8_array(bytes).map_err(sink_err)?;
                }
                FieldType::Primitive(Primitive::Float32) => {
                    let bytes = cursor.read_bytes(*length * 4)?;
                    let values: Vec<f32> = bytes
                        .chunks_exact(4)
                        .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
                        .collect();
                    sink.push_f32_array(&values).map_err(sink_err)?;
                }
                FieldType::Primitive(Primitive::Float64) => {
                    let bytes = cursor.read_bytes(*length * 8)?;
                    let values: Vec<f64> = bytes
                        .chunks_exact(8)
                        .map(|c| f64::from_le_bytes([c[0], c[1], c[2], c[3], c[4], c[5], c[6], c[7]]))
                        .collect();
                    sink.push_f64_array(&values).map_err(sink_err)?;
                }
                _ => {
                    sink.enter_list(*length).map_err(sink_err)?;
                    for _ in 0..*length {
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
                .ok_or_else(|| TranscodeError::UnresolvedType(full_name))?;

            transcode_message(cursor, nested_def, registry, sink)
        }
    }
}

/// Transcode a complete message from cursor to sink.
fn transcode_message<S: RowSink, R: TypeRegistry>(
    cursor: &mut Cursor,
    definition: &MessageDefinition,
    registry: &R,
    sink: &mut S,
) -> Result<(), TranscodeError>
where
    S::Error: std::fmt::Display,
{
    sink.enter_struct().map_err(sink_err)?;
    for field in &definition.fields {
        // Skip empty struct fields - they're not in the Arrow schema
        // (Lance doesn't support nullable empty structs)
        // Empty structs have 0 bytes in ROS1 format, so nothing to read from cursor
        if is_empty_struct(&field.field_type, registry) {
            continue;
        }
        transcode_field_type(cursor, &field.field_type, registry, sink)?;
    }
    sink.exit_struct().map_err(sink_err)?;
    Ok(())
}

/// Transcode a ROS1 message directly to a sink.
///
/// This is the main entry point - reads binary data and pushes values
/// directly to the sink without creating intermediate Value objects.
pub fn transcode<S: RowSink, R: TypeRegistry>(
    data: &[u8],
    definition: &MessageDefinition,
    registry: &R,
    sink: &mut S,
) -> Result<(), TranscodeError>
where
    S::Error: std::fmt::Display,
{
    let mut cursor = Cursor::new(data);

    // Transcode each field at the top level (no enter_struct for root)
    for field in &definition.fields {
        // Skip empty struct fields - they're not in the Arrow schema
        // (Lance doesn't support nullable empty structs)
        if is_empty_struct(&field.field_type, registry) {
            continue;
        }
        transcode_field_type(&mut cursor, &field.field_type, registry, sink)?;
    }

    sink.finish_row().map_err(sink_err)?;
    Ok(())
}

// ============================================================================
// Compiled Transcoder - Zero-lookup transcoding via pre-compiled execution plan
// ============================================================================

/// A pre-compiled field that can be transcoded without registry lookups.
/// All nested types are resolved at compile time.
#[derive(Debug, Clone)]
pub enum CompiledField {
    /// A primitive field
    Primitive(Primitive),

    /// A variable-length array with pre-compiled element type
    Array(Box<CompiledField>),

    /// A fixed-length array with pre-compiled element type
    FixedArray {
        element: Box<CompiledField>,
        length: usize,
    },

    /// A nested struct with pre-compiled fields (non-empty fields only)
    Struct(Vec<CompiledField>),

    // Optimized variants for common array types - avoid per-element dispatch
    /// Optimized: variable-length u8 array
    U8Array,
    /// Optimized: variable-length f32 array
    F32Array,
    /// Optimized: variable-length f64 array
    F64Array,
    /// Optimized: fixed-length u8 array
    FixedU8Array(usize),
    /// Optimized: fixed-length f32 array
    FixedF32Array(usize),
    /// Optimized: fixed-length f64 array
    FixedF64Array(usize),
}

/// A pre-compiled transcoder that can transcode messages without any registry lookups.
///
/// Create once per schema, then reuse for all messages of that type.
///
/// # Example
/// ```ignore
/// let transcoder = CompiledTranscoder::compile(&definition, &registry)?;
/// for message in messages {
///     transcoder.transcode(message.data, &mut sink)?;
/// }
/// ```
#[derive(Debug, Clone)]
pub struct CompiledTranscoder {
    /// The compiled fields for the root message (non-empty fields only)
    fields: Vec<CompiledField>,
}

/// Errors that can occur during transcoder compilation.
#[derive(Debug, Error)]
pub enum CompileError {
    #[error("unresolved nested type: {0}")]
    UnresolvedType(String),
}

impl CompiledTranscoder {
    /// Compile a transcoder for the given message definition.
    ///
    /// This resolves all nested types at compile time, so transcoding
    /// requires zero registry lookups.
    pub fn compile<R: TypeRegistry>(
        definition: &MessageDefinition,
        registry: &R,
    ) -> Result<Self, CompileError> {
        let fields = definition
            .fields
            .iter()
            .filter_map(|field| compile_field(&field.field_type, registry).transpose())
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self { fields })
    }

    /// Transcode a message using the pre-compiled execution plan.
    ///
    /// This is the fast path - no registry lookups, no string formatting,
    /// no is_empty_struct checks.
    #[inline]
    pub fn transcode<S: RowSink>(&self, data: &[u8], sink: &mut S) -> Result<(), TranscodeError>
    where
        S::Error: std::fmt::Display,
    {
        let mut cursor = Cursor::new(data);

        for field in &self.fields {
            transcode_compiled(&mut cursor, field, sink)?;
        }

        sink.finish_row().map_err(sink_err)?;
        Ok(())
    }
}

/// Compile a field type into a CompiledField.
/// Returns None for empty structs (they should be skipped).
fn compile_field<R: TypeRegistry>(
    field_type: &FieldType,
    registry: &R,
) -> Result<Option<CompiledField>, CompileError> {
    match field_type {
        FieldType::Primitive(p) => Ok(Some(CompiledField::Primitive(*p))),

        FieldType::Array { element } => {
            // Check for optimized array types
            match element.as_ref() {
                FieldType::Primitive(Primitive::UInt8) => Ok(Some(CompiledField::U8Array)),
                FieldType::Primitive(Primitive::Float32) => Ok(Some(CompiledField::F32Array)),
                FieldType::Primitive(Primitive::Float64) => Ok(Some(CompiledField::F64Array)),
                _ => {
                    match compile_field(element, registry)? {
                        Some(compiled) => Ok(Some(CompiledField::Array(Box::new(compiled)))),
                        None => {
                            // Array of empty structs - still need to read count but elements are empty
                            // This is unusual but we handle it
                            Ok(Some(CompiledField::Array(Box::new(CompiledField::Struct(
                                vec![],
                            )))))
                        }
                    }
                }
            }
        }

        FieldType::FixedArray { element, length } => {
            // Check for optimized fixed array types
            match element.as_ref() {
                FieldType::Primitive(Primitive::UInt8) => {
                    Ok(Some(CompiledField::FixedU8Array(*length)))
                }
                FieldType::Primitive(Primitive::Float32) => {
                    Ok(Some(CompiledField::FixedF32Array(*length)))
                }
                FieldType::Primitive(Primitive::Float64) => {
                    Ok(Some(CompiledField::FixedF64Array(*length)))
                }
                _ => match compile_field(element, registry)? {
                    Some(compiled) => Ok(Some(CompiledField::FixedArray {
                        element: Box::new(compiled),
                        length: *length,
                    })),
                    None => {
                        // Fixed array of empty structs - nothing to read
                        Ok(None)
                    }
                },
            }
        }

        FieldType::Nested { package, name } => {
            let full_name = match package {
                Some(pkg) => format!("{}/{}", pkg, name),
                None => name.clone(),
            };

            let nested_def = registry
                .get(&full_name)
                .ok_or_else(|| CompileError::UnresolvedType(full_name))?;

            // Compile nested fields, filtering out empty structs
            let compiled_fields: Vec<CompiledField> = nested_def
                .fields
                .iter()
                .filter_map(|field| compile_field(&field.field_type, registry).transpose())
                .collect::<Result<Vec<_>, _>>()?;

            // If all fields are empty, this is an empty struct - skip it
            if compiled_fields.is_empty() {
                Ok(None)
            } else {
                Ok(Some(CompiledField::Struct(compiled_fields)))
            }
        }
    }
}

/// Transcode a compiled field from cursor to sink.
#[inline]
fn transcode_compiled<S: RowSink>(
    cursor: &mut Cursor,
    field: &CompiledField,
    sink: &mut S,
) -> Result<(), TranscodeError>
where
    S::Error: std::fmt::Display,
{
    match field {
        CompiledField::Primitive(p) => transcode_primitive(cursor, *p, sink),

        // Optimized array variants - no per-element dispatch
        CompiledField::U8Array => {
            let count = cursor.read_u32()? as usize;
            let bytes = cursor.read_bytes(count)?;
            sink.push_u8_array(bytes).map_err(sink_err)?;
            Ok(())
        }
        CompiledField::F32Array => {
            let count = cursor.read_u32()? as usize;
            let bytes = cursor.read_bytes(count * 4)?;
            let values: Vec<f32> = bytes
                .chunks_exact(4)
                .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
                .collect();
            sink.push_f32_array(&values).map_err(sink_err)?;
            Ok(())
        }
        CompiledField::F64Array => {
            let count = cursor.read_u32()? as usize;
            let bytes = cursor.read_bytes(count * 8)?;
            let values: Vec<f64> = bytes
                .chunks_exact(8)
                .map(|c| f64::from_le_bytes([c[0], c[1], c[2], c[3], c[4], c[5], c[6], c[7]]))
                .collect();
            sink.push_f64_array(&values).map_err(sink_err)?;
            Ok(())
        }
        CompiledField::FixedU8Array(length) => {
            let bytes = cursor.read_bytes(*length)?;
            sink.push_u8_array(bytes).map_err(sink_err)?;
            Ok(())
        }
        CompiledField::FixedF32Array(length) => {
            let bytes = cursor.read_bytes(*length * 4)?;
            let values: Vec<f32> = bytes
                .chunks_exact(4)
                .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
                .collect();
            sink.push_f32_array(&values).map_err(sink_err)?;
            Ok(())
        }
        CompiledField::FixedF64Array(length) => {
            let bytes = cursor.read_bytes(*length * 8)?;
            let values: Vec<f64> = bytes
                .chunks_exact(8)
                .map(|c| f64::from_le_bytes([c[0], c[1], c[2], c[3], c[4], c[5], c[6], c[7]]))
                .collect();
            sink.push_f64_array(&values).map_err(sink_err)?;
            Ok(())
        }

        CompiledField::Array(element) => {
            let count = cursor.read_u32()? as usize;
            sink.enter_list(count).map_err(sink_err)?;
            for _ in 0..count {
                transcode_compiled(cursor, element, sink)?;
            }
            sink.exit_list().map_err(sink_err)?;
            Ok(())
        }

        CompiledField::FixedArray { element, length } => {
            sink.enter_list(*length).map_err(sink_err)?;
            for _ in 0..*length {
                transcode_compiled(cursor, element, sink)?;
            }
            sink.exit_list().map_err(sink_err)?;
            Ok(())
        }

        CompiledField::Struct(fields) => {
            sink.enter_struct().map_err(sink_err)?;
            for field in fields {
                transcode_compiled(cursor, field, sink)?;
            }
            sink.exit_struct().map_err(sink_err)?;
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::ArrowRowSink;
    use crate::ros1::MessageRegistry;
    use arrow::array::{Array, Float64Array, Int32Array, StringArray, StructArray};
    use arrow::datatypes::{DataType, Field, Fields, Schema, TimeUnit};
    use std::sync::Arc;

    #[test]
    fn test_transcode_simple_point() {
        // Schema: float64 x, float64 y, float64 z
        let def = crate::ros1::MessageDefinition::parse("float64 x\nfloat64 y\nfloat64 z").unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Float64, false),
            Field::new("y", DataType::Float64, false),
            Field::new("z", DataType::Float64, false),
        ]));

        let mut sink = ArrowRowSink::new(schema).unwrap();
        let registry = MessageRegistry::new();

        // Create ROS1 binary: 3 float64s
        let mut data = Vec::new();
        data.extend_from_slice(&1.5f64.to_le_bytes());
        data.extend_from_slice(&2.5f64.to_le_bytes());
        data.extend_from_slice(&3.5f64.to_le_bytes());

        transcode(&data, &def, &registry, &mut sink).unwrap();

        let batch = sink.finish().unwrap();
        assert_eq!(batch.num_rows(), 1);

        let x = batch.column(0).as_any().downcast_ref::<Float64Array>().unwrap();
        let y = batch.column(1).as_any().downcast_ref::<Float64Array>().unwrap();
        let z = batch.column(2).as_any().downcast_ref::<Float64Array>().unwrap();

        assert_eq!(x.value(0), 1.5);
        assert_eq!(y.value(0), 2.5);
        assert_eq!(z.value(0), 3.5);
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
        data.extend_from_slice(&9u32.to_le_bytes()); // string length
        data.extend_from_slice(b"base_link");

        transcode(&data, &def, &registry, &mut sink).unwrap();

        let batch = sink.finish().unwrap();

        let seq = batch.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        let frame = batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();

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

        transcode(&data, &def, &registry, &mut sink).unwrap();

        let batch = sink.finish().unwrap();
        assert_eq!(batch.num_rows(), 1);

        let position = batch.column(0).as_any().downcast_ref::<StructArray>().unwrap();
        let x = position.column(0).as_any().downcast_ref::<Float64Array>().unwrap();

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
        data.extend_from_slice(&1000u32.to_le_bytes()); // sec
        data.extend_from_slice(&500u32.to_le_bytes()); // nsec

        transcode(&data, &def, &registry, &mut sink).unwrap();

        let batch = sink.finish().unwrap();
        let stamp = batch.column(0).as_any().downcast_ref::<arrow::array::TimestampNanosecondArray>().unwrap();

        // 1000 sec * 1e9 + 500 nsec
        assert_eq!(stamp.value(0), 1_000_000_000_500i64);
    }

    #[test]
    fn test_transcode_list_of_structs() {
        // Simulates tf2_msgs/TFMessage: { transforms: TransformStamped[] }
        let mut registry = MessageRegistry::new();

        // TransformStamped has: header, child_frame_id, transform
        // Simplified: just child_frame_id (string) and x, y (floats)
        let transform_def = crate::ros1::MessageDefinition::parse(
            "string child_frame_id\nfloat64 x\nfloat64 y"
        ).unwrap();
        registry.register("TransformStamped", transform_def);

        let def = crate::ros1::MessageDefinition::parse("TransformStamped[] transforms").unwrap();

        // Build Arrow schema
        let transform_fields = Fields::from(vec![
            Arc::new(Field::new("child_frame_id", DataType::Utf8, false)),
            Arc::new(Field::new("x", DataType::Float64, false)),
            Arc::new(Field::new("y", DataType::Float64, false)),
        ]);
        let schema = Arc::new(Schema::new(vec![Arc::new(Field::new(
            "transforms",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(transform_fields),
                true,
            ))),
            false,
        ))]));

        let mut sink = ArrowRowSink::new(schema).unwrap();

        // Build ROS1 binary: 2 transforms
        let mut data = Vec::new();
        // Array length
        data.extend_from_slice(&2u32.to_le_bytes());
        // Transform 1
        data.extend_from_slice(&5u32.to_le_bytes()); // string length
        data.extend_from_slice(b"odom1");
        data.extend_from_slice(&1.0f64.to_le_bytes());
        data.extend_from_slice(&2.0f64.to_le_bytes());
        // Transform 2
        data.extend_from_slice(&5u32.to_le_bytes()); // string length
        data.extend_from_slice(b"odom2");
        data.extend_from_slice(&3.0f64.to_le_bytes());
        data.extend_from_slice(&4.0f64.to_le_bytes());

        transcode(&data, &def, &registry, &mut sink).unwrap();

        let batch = sink.finish().unwrap();
        assert_eq!(batch.num_rows(), 1);

        let list = batch.column(0).as_any().downcast_ref::<arrow::array::ListArray>().unwrap();
        assert_eq!(list.len(), 1);

        let structs = list.values().as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(structs.len(), 2);

        let child_frame = structs.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let x = structs.column(1).as_any().downcast_ref::<Float64Array>().unwrap();

        assert_eq!(child_frame.value(0), "odom1");
        assert_eq!(child_frame.value(1), "odom2");
        assert_eq!(x.value(0), 1.0);
        assert_eq!(x.value(1), 3.0);
    }

    // ========================================================================
    // CompiledTranscoder tests
    // ========================================================================

    #[test]
    fn test_compiled_transcode_simple_point() {
        let def = crate::ros1::MessageDefinition::parse("float64 x\nfloat64 y\nfloat64 z").unwrap();
        let registry = MessageRegistry::new();

        let schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Float64, false),
            Field::new("y", DataType::Float64, false),
            Field::new("z", DataType::Float64, false),
        ]));

        // Compile once
        let transcoder = CompiledTranscoder::compile(&def, &registry).unwrap();

        let mut sink = ArrowRowSink::new(schema).unwrap();

        // Create ROS1 binary: 3 float64s
        let mut data = Vec::new();
        data.extend_from_slice(&1.5f64.to_le_bytes());
        data.extend_from_slice(&2.5f64.to_le_bytes());
        data.extend_from_slice(&3.5f64.to_le_bytes());

        // Transcode using compiled transcoder
        transcoder.transcode(&data, &mut sink).unwrap();

        let batch = sink.finish().unwrap();
        assert_eq!(batch.num_rows(), 1);

        let x = batch.column(0).as_any().downcast_ref::<Float64Array>().unwrap();
        let y = batch.column(1).as_any().downcast_ref::<Float64Array>().unwrap();
        let z = batch.column(2).as_any().downcast_ref::<Float64Array>().unwrap();

        assert_eq!(x.value(0), 1.5);
        assert_eq!(y.value(0), 2.5);
        assert_eq!(z.value(0), 3.5);
    }

    #[test]
    fn test_compiled_transcode_nested_message() {
        let mut registry = MessageRegistry::new();

        let point_def =
            crate::ros1::MessageDefinition::parse("float64 x\nfloat64 y\nfloat64 z").unwrap();
        registry.register("geometry_msgs/Point", point_def);

        let def = crate::ros1::MessageDefinition::parse("geometry_msgs/Point position").unwrap();

        // Compile - this resolves geometry_msgs/Point at compile time
        let transcoder = CompiledTranscoder::compile(&def, &registry).unwrap();

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

        transcoder.transcode(&data, &mut sink).unwrap();

        let batch = sink.finish().unwrap();
        assert_eq!(batch.num_rows(), 1);

        let position = batch.column(0).as_any().downcast_ref::<StructArray>().unwrap();
        let x = position.column(0).as_any().downcast_ref::<Float64Array>().unwrap();

        assert_eq!(x.value(0), 1.0);
    }

    #[test]
    fn test_compiled_transcode_list_of_structs() {
        let mut registry = MessageRegistry::new();

        let transform_def = crate::ros1::MessageDefinition::parse(
            "string child_frame_id\nfloat64 x\nfloat64 y"
        ).unwrap();
        registry.register("TransformStamped", transform_def);

        let def = crate::ros1::MessageDefinition::parse("TransformStamped[] transforms").unwrap();

        // Compile once
        let transcoder = CompiledTranscoder::compile(&def, &registry).unwrap();

        let transform_fields = Fields::from(vec![
            Arc::new(Field::new("child_frame_id", DataType::Utf8, false)),
            Arc::new(Field::new("x", DataType::Float64, false)),
            Arc::new(Field::new("y", DataType::Float64, false)),
        ]);
        let schema = Arc::new(Schema::new(vec![Arc::new(Field::new(
            "transforms",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(transform_fields),
                true,
            ))),
            false,
        ))]));

        let mut sink = ArrowRowSink::new(schema).unwrap();

        // Build ROS1 binary: 2 transforms
        let mut data = Vec::new();
        data.extend_from_slice(&2u32.to_le_bytes());
        data.extend_from_slice(&5u32.to_le_bytes());
        data.extend_from_slice(b"odom1");
        data.extend_from_slice(&1.0f64.to_le_bytes());
        data.extend_from_slice(&2.0f64.to_le_bytes());
        data.extend_from_slice(&5u32.to_le_bytes());
        data.extend_from_slice(b"odom2");
        data.extend_from_slice(&3.0f64.to_le_bytes());
        data.extend_from_slice(&4.0f64.to_le_bytes());

        transcoder.transcode(&data, &mut sink).unwrap();

        let batch = sink.finish().unwrap();
        assert_eq!(batch.num_rows(), 1);

        let list = batch.column(0).as_any().downcast_ref::<arrow::array::ListArray>().unwrap();
        assert_eq!(list.len(), 1);

        let structs = list.values().as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(structs.len(), 2);

        let child_frame = structs.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let x = structs.column(1).as_any().downcast_ref::<Float64Array>().unwrap();

        assert_eq!(child_frame.value(0), "odom1");
        assert_eq!(child_frame.value(1), "odom2");
        assert_eq!(x.value(0), 1.0);
        assert_eq!(x.value(1), 3.0);
    }

    #[test]
    fn test_compiled_transcode_multiple_messages() {
        // Test that the same compiled transcoder can be reused for multiple messages
        let def = crate::ros1::MessageDefinition::parse("float64 x\nfloat64 y").unwrap();
        let registry = MessageRegistry::new();

        let transcoder = CompiledTranscoder::compile(&def, &registry).unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Float64, false),
            Field::new("y", DataType::Float64, false),
        ]));

        let mut sink = ArrowRowSink::new(schema).unwrap();

        // Transcode 3 messages with the same compiled transcoder
        for i in 0..3 {
            let mut data = Vec::new();
            data.extend_from_slice(&(i as f64).to_le_bytes());
            data.extend_from_slice(&((i * 10) as f64).to_le_bytes());
            transcoder.transcode(&data, &mut sink).unwrap();
        }

        let batch = sink.finish().unwrap();
        assert_eq!(batch.num_rows(), 3);

        let x = batch.column(0).as_any().downcast_ref::<Float64Array>().unwrap();
        let y = batch.column(1).as_any().downcast_ref::<Float64Array>().unwrap();

        assert_eq!(x.value(0), 0.0);
        assert_eq!(x.value(1), 1.0);
        assert_eq!(x.value(2), 2.0);
        assert_eq!(y.value(0), 0.0);
        assert_eq!(y.value(1), 10.0);
        assert_eq!(y.value(2), 20.0);
    }
}
