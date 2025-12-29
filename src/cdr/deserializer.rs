//! CDR (Common Data Representation) deserializer for ROS2 messages.
//!
//! CDR is the OMG serialization format used by DDS/ROS2. Key differences from ROS1:
//! - Primitives are aligned to their natural boundaries (2/4/8 bytes)
//! - Messages start with a 4-byte encapsulation header
//! - Strings include null terminator in their length

use std::collections::HashMap;

use thiserror::Error;

use crate::ros1::{FieldType, MessageDefinition, Primitive};

/// Errors that can occur during CDR deserialization.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum CdrDeserializeError {
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

    #[error("invalid encapsulation header")]
    InvalidEncapsulation,
}

/// A deserialized CDR value.
#[derive(Debug, Clone, PartialEq)]
pub enum CdrValue {
    Bool(bool),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Float32(f32),
    Float64(f64),
    String(String),
    /// Time represented as (seconds, nanoseconds)
    Time { sec: i32, nsec: u32 },
    /// Duration represented as (seconds, nanoseconds)
    Duration { sec: i32, nsec: u32 },
    /// Variable or fixed-length array
    Array(Vec<CdrValue>),
    /// Nested message as field name -> value
    Message(HashMap<String, CdrValue>),
}

impl CdrValue {
    /// Try to get this value as a bool.
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            CdrValue::Bool(v) => Some(*v),
            _ => None,
        }
    }

    /// Try to get this value as an i64 (works for all integer types).
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            CdrValue::Int8(v) => Some(*v as i64),
            CdrValue::Int16(v) => Some(*v as i64),
            CdrValue::Int32(v) => Some(*v as i64),
            CdrValue::Int64(v) => Some(*v),
            CdrValue::UInt8(v) => Some(*v as i64),
            CdrValue::UInt16(v) => Some(*v as i64),
            CdrValue::UInt32(v) => Some(*v as i64),
            _ => None,
        }
    }

    /// Try to get this value as a u64 (works for unsigned integer types).
    pub fn as_u64(&self) -> Option<u64> {
        match self {
            CdrValue::UInt8(v) => Some(*v as u64),
            CdrValue::UInt16(v) => Some(*v as u64),
            CdrValue::UInt32(v) => Some(*v as u64),
            CdrValue::UInt64(v) => Some(*v),
            _ => None,
        }
    }

    /// Try to get this value as an f64 (works for float types).
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            CdrValue::Float32(v) => Some(*v as f64),
            CdrValue::Float64(v) => Some(*v),
            _ => None,
        }
    }

    /// Try to get this value as a string reference.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            CdrValue::String(v) => Some(v),
            _ => None,
        }
    }

    /// Try to get this value as an array reference.
    pub fn as_array(&self) -> Option<&[CdrValue]> {
        match self {
            CdrValue::Array(v) => Some(v),
            _ => None,
        }
    }

    /// Try to get this value as a message (struct) reference.
    pub fn as_message(&self) -> Option<&HashMap<String, CdrValue>> {
        match self {
            CdrValue::Message(v) => Some(v),
            _ => None,
        }
    }

    /// Get a nested field from a message value.
    pub fn get(&self, field: &str) -> Option<&CdrValue> {
        self.as_message().and_then(|m| m.get(field))
    }
}

/// Registry of message definitions for resolving nested types during deserialization.
pub trait CdrTypeRegistry {
    fn get(&self, name: &str) -> Option<&MessageDefinition>;
}

impl CdrTypeRegistry for HashMap<String, MessageDefinition> {
    fn get(&self, name: &str) -> Option<&MessageDefinition> {
        HashMap::get(self, name)
    }
}

impl CdrTypeRegistry for crate::ros1::MessageRegistry {
    fn get(&self, name: &str) -> Option<&MessageDefinition> {
        crate::ros1::MessageRegistry::get(self, name)
    }
}

/// A cursor for reading CDR-encoded bytes with alignment support.
pub struct CdrCursor<'a> {
    data: &'a [u8],
    pos: usize,
    /// Starting offset for alignment calculations (after encapsulation header)
    origin: usize,
}

impl<'a> CdrCursor<'a> {
    /// Create a new CDR cursor.
    pub fn new(data: &'a [u8]) -> Self {
        Self {
            data,
            pos: 0,
            origin: 0,
        }
    }

    /// Create a cursor with an origin offset for alignment calculations.
    pub fn with_origin(data: &'a [u8], origin: usize) -> Self {
        Self {
            data,
            pos: 0,
            origin,
        }
    }

    /// Get current position.
    pub fn position(&self) -> usize {
        self.pos
    }

    /// Get remaining bytes.
    pub fn remaining(&self) -> usize {
        self.data.len().saturating_sub(self.pos)
    }

    /// Align to N-byte boundary relative to origin.
    pub fn align_to(&mut self, alignment: usize) -> Result<(), CdrDeserializeError> {
        if alignment <= 1 {
            return Ok(());
        }
        let offset_from_origin = self.pos + self.origin;
        let remainder = offset_from_origin % alignment;
        if remainder != 0 {
            let padding = alignment - remainder;
            if self.remaining() < padding {
                return Err(CdrDeserializeError::UnexpectedEof {
                    needed: padding,
                    offset: self.pos,
                    available: self.remaining(),
                });
            }
            self.pos += padding;
        }
        Ok(())
    }

    fn read_bytes(&mut self, n: usize) -> Result<&'a [u8], CdrDeserializeError> {
        if self.remaining() < n {
            return Err(CdrDeserializeError::UnexpectedEof {
                needed: n,
                offset: self.pos,
                available: self.remaining(),
            });
        }
        let bytes = &self.data[self.pos..self.pos + n];
        self.pos += n;
        Ok(bytes)
    }

    pub fn read_u8(&mut self) -> Result<u8, CdrDeserializeError> {
        // No alignment needed for 1-byte types
        let bytes = self.read_bytes(1)?;
        Ok(bytes[0])
    }

    pub fn read_i8(&mut self) -> Result<i8, CdrDeserializeError> {
        let bytes = self.read_bytes(1)?;
        Ok(bytes[0] as i8)
    }

    pub fn read_u16(&mut self) -> Result<u16, CdrDeserializeError> {
        self.align_to(2)?;
        let bytes = self.read_bytes(2)?;
        Ok(u16::from_le_bytes([bytes[0], bytes[1]]))
    }

    pub fn read_i16(&mut self) -> Result<i16, CdrDeserializeError> {
        self.align_to(2)?;
        let bytes = self.read_bytes(2)?;
        Ok(i16::from_le_bytes([bytes[0], bytes[1]]))
    }

    pub fn read_u32(&mut self) -> Result<u32, CdrDeserializeError> {
        self.align_to(4)?;
        let bytes = self.read_bytes(4)?;
        Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    pub fn read_i32(&mut self) -> Result<i32, CdrDeserializeError> {
        self.align_to(4)?;
        let bytes = self.read_bytes(4)?;
        Ok(i32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    pub fn read_u64(&mut self) -> Result<u64, CdrDeserializeError> {
        self.align_to(8)?;
        let bytes = self.read_bytes(8)?;
        Ok(u64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    pub fn read_i64(&mut self) -> Result<i64, CdrDeserializeError> {
        self.align_to(8)?;
        let bytes = self.read_bytes(8)?;
        Ok(i64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    pub fn read_f32(&mut self) -> Result<f32, CdrDeserializeError> {
        self.align_to(4)?;
        let bytes = self.read_bytes(4)?;
        Ok(f32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    pub fn read_f64(&mut self) -> Result<f64, CdrDeserializeError> {
        self.align_to(8)?;
        let bytes = self.read_bytes(8)?;
        Ok(f64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    /// Read a CDR string.
    /// CDR strings have a 4-byte length that includes the null terminator.
    pub fn read_string(&mut self) -> Result<String, CdrDeserializeError> {
        let len = self.read_u32()? as usize;
        if len == 0 {
            return Ok(String::new());
        }
        let bytes = self.read_bytes(len)?;
        // CDR strings include null terminator in length, so we strip it
        let string_bytes = if !bytes.is_empty() && bytes[bytes.len() - 1] == 0 {
            &bytes[..bytes.len() - 1]
        } else {
            bytes
        };
        String::from_utf8(string_bytes.to_vec())
            .map_err(|e| CdrDeserializeError::InvalidUtf8(e.to_string()))
    }

    /// Skip the 4-byte CDR encapsulation header and set origin for alignment.
    pub fn skip_encapsulation(&mut self) -> Result<(), CdrDeserializeError> {
        if self.remaining() < 4 {
            return Err(CdrDeserializeError::InvalidEncapsulation);
        }
        // Encapsulation header: 2 bytes encoding kind + 2 bytes options
        // 0x0001 = CDR_LE (little-endian), 0x0000 = CDR_BE (big-endian)
        // We only support little-endian for now (standard for ROS2)
        let _encoding = self.read_bytes(2)?;
        let _options = self.read_bytes(2)?;
        // Set origin after encapsulation header for alignment calculations
        self.origin = self.pos;
        Ok(())
    }
}

/// Deserialize a primitive value from the cursor.
fn deserialize_primitive(
    cursor: &mut CdrCursor,
    primitive: Primitive,
) -> Result<CdrValue, CdrDeserializeError> {
    match primitive {
        Primitive::Bool => {
            let byte = cursor.read_u8()?;
            Ok(CdrValue::Bool(byte != 0))
        }
        Primitive::Int8 => Ok(CdrValue::Int8(cursor.read_i8()?)),
        Primitive::Int16 => Ok(CdrValue::Int16(cursor.read_i16()?)),
        Primitive::Int32 => Ok(CdrValue::Int32(cursor.read_i32()?)),
        Primitive::Int64 => Ok(CdrValue::Int64(cursor.read_i64()?)),
        Primitive::UInt8 => Ok(CdrValue::UInt8(cursor.read_u8()?)),
        Primitive::UInt16 => Ok(CdrValue::UInt16(cursor.read_u16()?)),
        Primitive::UInt32 => Ok(CdrValue::UInt32(cursor.read_u32()?)),
        Primitive::UInt64 => Ok(CdrValue::UInt64(cursor.read_u64()?)),
        Primitive::Float32 => Ok(CdrValue::Float32(cursor.read_f32()?)),
        Primitive::Float64 => Ok(CdrValue::Float64(cursor.read_f64()?)),
        Primitive::String => Ok(CdrValue::String(cursor.read_string()?)),
        Primitive::Time => {
            // ROS2 Time is sec (i32) + nanosec (u32)
            let sec = cursor.read_i32()?;
            let nsec = cursor.read_u32()?;
            Ok(CdrValue::Time { sec, nsec })
        }
        Primitive::Duration => {
            // ROS2 Duration is sec (i32) + nanosec (u32)
            let sec = cursor.read_i32()?;
            let nsec = cursor.read_u32()?;
            Ok(CdrValue::Duration { sec, nsec })
        }
    }
}

/// Get the alignment requirement for a field type.
fn field_alignment(field_type: &FieldType) -> usize {
    match field_type {
        FieldType::Primitive(p) => match p {
            Primitive::Bool | Primitive::Int8 | Primitive::UInt8 => 1,
            Primitive::Int16 | Primitive::UInt16 => 2,
            Primitive::Int32 | Primitive::UInt32 | Primitive::Float32 => 4,
            Primitive::Int64 | Primitive::UInt64 | Primitive::Float64 => 8,
            Primitive::String => 4, // String length prefix is u32
            Primitive::Time | Primitive::Duration => 4, // First field is i32
        },
        FieldType::Array { .. } | FieldType::FixedArray { .. } => 4, // Array length prefix is u32
        FieldType::Nested { .. } => 1, // Nested structs align to their first field
    }
}

/// Deserialize a field type from the cursor.
fn deserialize_field_type<R: CdrTypeRegistry>(
    cursor: &mut CdrCursor,
    field_type: &FieldType,
    registry: &R,
) -> Result<CdrValue, CdrDeserializeError> {
    match field_type {
        FieldType::Primitive(p) => deserialize_primitive(cursor, *p),

        FieldType::Array { element } => {
            let count = cursor.read_u32()? as usize;
            let mut values = Vec::with_capacity(count);
            for _ in 0..count {
                // Align each element
                cursor.align_to(field_alignment(element))?;
                values.push(deserialize_field_type(cursor, element, registry)?);
            }
            Ok(CdrValue::Array(values))
        }

        FieldType::FixedArray { element, length } => {
            let mut values = Vec::with_capacity(*length);
            for i in 0..*length {
                // Align each element (except first which is already aligned)
                if i > 0 {
                    cursor.align_to(field_alignment(element))?;
                }
                values.push(deserialize_field_type(cursor, element, registry)?);
            }
            Ok(CdrValue::Array(values))
        }

        FieldType::Nested { package, name } => {
            let full_name = match package {
                Some(pkg) => format!("{}/{}", pkg, name),
                None => name.clone(),
            };

            let nested_def = registry
                .get(&full_name)
                .ok_or_else(|| CdrDeserializeError::UnresolvedType(full_name))?;

            deserialize_message(cursor, nested_def, registry)
        }
    }
}

/// Deserialize a complete message from the cursor.
fn deserialize_message<R: CdrTypeRegistry>(
    cursor: &mut CdrCursor,
    definition: &MessageDefinition,
    registry: &R,
) -> Result<CdrValue, CdrDeserializeError> {
    let mut fields = HashMap::new();

    for field in &definition.fields {
        // Align before each field
        cursor.align_to(field_alignment(&field.field_type))?;
        let value = deserialize_field_type(cursor, &field.field_type, registry)?;
        fields.insert(field.name.clone(), value);
    }

    Ok(CdrValue::Message(fields))
}

/// Deserialize a CDR message from bytes.
///
/// This is the main entry point for deserialization.
/// The data should include the 4-byte encapsulation header.
pub fn deserialize<R: CdrTypeRegistry>(
    data: &[u8],
    definition: &MessageDefinition,
    registry: &R,
) -> Result<CdrValue, CdrDeserializeError> {
    let mut cursor = CdrCursor::new(data);
    cursor.skip_encapsulation()?;
    deserialize_message(&mut cursor, definition, registry)
}

/// Deserialize a CDR message without encapsulation header.
/// Use this when the encapsulation has already been stripped.
pub fn deserialize_no_header<R: CdrTypeRegistry>(
    data: &[u8],
    definition: &MessageDefinition,
    registry: &R,
) -> Result<CdrValue, CdrDeserializeError> {
    let mut cursor = CdrCursor::new(data);
    deserialize_message(&mut cursor, definition, registry)
}

/// Deserialize a simple message with no nested types.
pub fn deserialize_simple(
    data: &[u8],
    definition: &MessageDefinition,
) -> Result<CdrValue, CdrDeserializeError> {
    let empty: HashMap<String, MessageDefinition> = HashMap::new();
    deserialize(data, definition, &empty)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ros1::MessageDefinition;

    /// Helper to create CDR data with encapsulation header
    fn with_encapsulation(data: &[u8]) -> Vec<u8> {
        let mut result = vec![0x00, 0x01, 0x00, 0x00]; // CDR_LE header
        result.extend_from_slice(data);
        result
    }

    mod primitive_deserialization {
        use super::*;

        #[test]
        fn test_bool() {
            let def = MessageDefinition::parse("bool value").unwrap();
            let data = with_encapsulation(&[1]);
            let value = deserialize_simple(&data, &def).unwrap();
            assert_eq!(value.get("value").unwrap().as_bool(), Some(true));
        }

        #[test]
        fn test_int32() {
            let def = MessageDefinition::parse("int32 value").unwrap();
            let data = with_encapsulation(&123456i32.to_le_bytes());
            let value = deserialize_simple(&data, &def).unwrap();
            assert_eq!(value.get("value").unwrap().as_i64(), Some(123456));
        }

        #[test]
        fn test_float64() {
            let def = MessageDefinition::parse("float64 value").unwrap();
            let data = with_encapsulation(&std::f64::consts::PI.to_le_bytes());
            let value = deserialize_simple(&data, &def).unwrap();
            let f = value.get("value").unwrap().as_f64().unwrap();
            assert!((f - std::f64::consts::PI).abs() < 0.0000001);
        }

        #[test]
        fn test_string() {
            let def = MessageDefinition::parse("string value").unwrap();
            let mut data = vec![0x00, 0x01, 0x00, 0x00]; // CDR_LE header
            data.extend_from_slice(&6u32.to_le_bytes()); // length including null
            data.extend_from_slice(b"hello\0");
            let value = deserialize_simple(&data, &def).unwrap();
            assert_eq!(value.get("value").unwrap().as_str(), Some("hello"));
        }

        #[test]
        fn test_empty_string() {
            let def = MessageDefinition::parse("string value").unwrap();
            let data = with_encapsulation(&0u32.to_le_bytes());
            let value = deserialize_simple(&data, &def).unwrap();
            assert_eq!(value.get("value").unwrap().as_str(), Some(""));
        }
    }

    mod alignment {
        use super::*;

        #[test]
        fn test_bool_then_int32_alignment() {
            // bool (1 byte) + padding (3 bytes) + int32 (4 bytes)
            let def = MessageDefinition::parse("bool a\nint32 b").unwrap();
            let mut data = vec![0x00, 0x01, 0x00, 0x00]; // CDR_LE header
            data.push(1); // bool a
            data.extend_from_slice(&[0, 0, 0]); // padding
            data.extend_from_slice(&42i32.to_le_bytes()); // int32 b

            let value = deserialize_simple(&data, &def).unwrap();
            assert_eq!(value.get("a").unwrap().as_bool(), Some(true));
            assert_eq!(value.get("b").unwrap().as_i64(), Some(42));
        }

        #[test]
        fn test_int16_then_int64_alignment() {
            // int16 (2 bytes) + padding (6 bytes) + int64 (8 bytes)
            let def = MessageDefinition::parse("int16 a\nint64 b").unwrap();
            let mut data = vec![0x00, 0x01, 0x00, 0x00]; // CDR_LE header
            data.extend_from_slice(&100i16.to_le_bytes()); // int16 a
            data.extend_from_slice(&[0, 0, 0, 0, 0, 0]); // padding to 8-byte boundary
            data.extend_from_slice(&999i64.to_le_bytes()); // int64 b

            let value = deserialize_simple(&data, &def).unwrap();
            assert_eq!(value.get("a").unwrap().as_i64(), Some(100));
            assert_eq!(value.get("b").unwrap().as_i64(), Some(999));
        }

        #[test]
        fn test_no_padding_needed() {
            // int32 (4 bytes) + int32 (4 bytes) - no padding needed
            let def = MessageDefinition::parse("int32 a\nint32 b").unwrap();
            let mut data = vec![0x00, 0x01, 0x00, 0x00]; // CDR_LE header
            data.extend_from_slice(&1i32.to_le_bytes());
            data.extend_from_slice(&2i32.to_le_bytes());

            let value = deserialize_simple(&data, &def).unwrap();
            assert_eq!(value.get("a").unwrap().as_i64(), Some(1));
            assert_eq!(value.get("b").unwrap().as_i64(), Some(2));
        }
    }

    mod arrays {
        use super::*;

        #[test]
        fn test_variable_array_uint8() {
            let def = MessageDefinition::parse("uint8[] data").unwrap();
            let mut data = vec![0x00, 0x01, 0x00, 0x00]; // CDR_LE header
            data.extend_from_slice(&4u32.to_le_bytes()); // count
            data.extend_from_slice(&[1, 2, 3, 4]);

            let value = deserialize_simple(&data, &def).unwrap();
            let arr = value.get("data").unwrap().as_array().unwrap();
            assert_eq!(arr.len(), 4);
            assert_eq!(arr[0].as_u64(), Some(1));
            assert_eq!(arr[3].as_u64(), Some(4));
        }

        #[test]
        fn test_fixed_array_float64() {
            let def = MessageDefinition::parse("float64[3] values").unwrap();
            let mut data = vec![0x00, 0x01, 0x00, 0x00]; // CDR_LE header
            // No length prefix for fixed arrays
            data.extend_from_slice(&1.0f64.to_le_bytes());
            data.extend_from_slice(&2.0f64.to_le_bytes());
            data.extend_from_slice(&3.0f64.to_le_bytes());

            let value = deserialize_simple(&data, &def).unwrap();
            let arr = value.get("values").unwrap().as_array().unwrap();
            assert_eq!(arr.len(), 3);
            assert_eq!(arr[0].as_f64(), Some(1.0));
            assert_eq!(arr[2].as_f64(), Some(3.0));
        }
    }

    mod nested_messages {
        use super::*;
        use crate::ros1::MessageRegistry;

        #[test]
        fn test_simple_nested() {
            let mut registry = MessageRegistry::new();
            let point_def = MessageDefinition::parse("float64 x\nfloat64 y\nfloat64 z").unwrap();
            registry.register("geometry_msgs/Point", point_def);

            let def = MessageDefinition::parse("geometry_msgs/Point position").unwrap();

            let mut data = vec![0x00, 0x01, 0x00, 0x00]; // CDR_LE header
            data.extend_from_slice(&1.0f64.to_le_bytes());
            data.extend_from_slice(&2.0f64.to_le_bytes());
            data.extend_from_slice(&3.0f64.to_le_bytes());

            let value = deserialize(&data, &def, &registry).unwrap();
            let pos = value.get("position").unwrap().as_message().unwrap();
            assert_eq!(pos.get("x").unwrap().as_f64(), Some(1.0));
            assert_eq!(pos.get("y").unwrap().as_f64(), Some(2.0));
            assert_eq!(pos.get("z").unwrap().as_f64(), Some(3.0));
        }
    }
}
