use std::collections::HashMap;

use thiserror::Error;

use crate::ros1::{FieldType, MessageDefinition, Primitive};

/// Errors that can occur during deserialization.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum DeserializeError {
    #[error("unexpected end of data: needed {needed} bytes, had {available}")]
    UnexpectedEof { needed: usize, available: usize },

    #[error("invalid UTF-8 string: {0}")]
    InvalidUtf8(String),

    #[error("unresolved nested type: {0}")]
    UnresolvedType(String),

    #[error("invalid boolean value: {0}")]
    InvalidBool(u8),
}

/// A deserialized ROS1 value.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
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
    Time { sec: u32, nsec: u32 },
    /// Duration represented as (seconds, nanoseconds)
    Duration { sec: i32, nsec: i32 },
    /// Variable or fixed-length array
    Array(Vec<Value>),
    /// Nested message as field name -> value
    Message(HashMap<String, Value>),
}

impl Value {
    /// Try to get this value as a bool.
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(v) => Some(*v),
            _ => None,
        }
    }

    /// Try to get this value as an i64 (works for all integer types).
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Int8(v) => Some(*v as i64),
            Value::Int16(v) => Some(*v as i64),
            Value::Int32(v) => Some(*v as i64),
            Value::Int64(v) => Some(*v),
            Value::UInt8(v) => Some(*v as i64),
            Value::UInt16(v) => Some(*v as i64),
            Value::UInt32(v) => Some(*v as i64),
            _ => None,
        }
    }

    /// Try to get this value as a u64 (works for unsigned integer types).
    pub fn as_u64(&self) -> Option<u64> {
        match self {
            Value::UInt8(v) => Some(*v as u64),
            Value::UInt16(v) => Some(*v as u64),
            Value::UInt32(v) => Some(*v as u64),
            Value::UInt64(v) => Some(*v),
            _ => None,
        }
    }

    /// Try to get this value as an f64 (works for float types).
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Float32(v) => Some(*v as f64),
            Value::Float64(v) => Some(*v),
            _ => None,
        }
    }

    /// Try to get this value as a string reference.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::String(v) => Some(v),
            _ => None,
        }
    }

    /// Try to get this value as an array reference.
    pub fn as_array(&self) -> Option<&[Value]> {
        match self {
            Value::Array(v) => Some(v),
            _ => None,
        }
    }

    /// Try to get this value as a message (struct) reference.
    pub fn as_message(&self) -> Option<&HashMap<String, Value>> {
        match self {
            Value::Message(v) => Some(v),
            _ => None,
        }
    }

    /// Get a nested field from a message value.
    pub fn get(&self, field: &str) -> Option<&Value> {
        self.as_message().and_then(|m| m.get(field))
    }
}

/// Registry of message definitions for resolving nested types during deserialization.
pub trait TypeRegistry {
    fn get(&self, name: &str) -> Option<&MessageDefinition>;
}

impl TypeRegistry for HashMap<String, MessageDefinition> {
    fn get(&self, name: &str) -> Option<&MessageDefinition> {
        HashMap::get(self, name)
    }
}

impl TypeRegistry for crate::ros1::MessageRegistry {
    fn get(&self, name: &str) -> Option<&MessageDefinition> {
        crate::ros1::MessageRegistry::get(self, name)
    }
}

/// A cursor for reading bytes during deserialization.
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

    fn read_bytes(&mut self, n: usize) -> Result<&'a [u8], DeserializeError> {
        if self.remaining() < n {
            return Err(DeserializeError::UnexpectedEof {
                needed: n,
                available: self.remaining(),
            });
        }
        let bytes = &self.data[self.pos..self.pos + n];
        self.pos += n;
        Ok(bytes)
    }

    fn read_u8(&mut self) -> Result<u8, DeserializeError> {
        let bytes = self.read_bytes(1)?;
        Ok(bytes[0])
    }

    fn read_i8(&mut self) -> Result<i8, DeserializeError> {
        let bytes = self.read_bytes(1)?;
        Ok(bytes[0] as i8)
    }

    fn read_u16(&mut self) -> Result<u16, DeserializeError> {
        let bytes = self.read_bytes(2)?;
        Ok(u16::from_le_bytes([bytes[0], bytes[1]]))
    }

    fn read_i16(&mut self) -> Result<i16, DeserializeError> {
        let bytes = self.read_bytes(2)?;
        Ok(i16::from_le_bytes([bytes[0], bytes[1]]))
    }

    fn read_u32(&mut self) -> Result<u32, DeserializeError> {
        let bytes = self.read_bytes(4)?;
        Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn read_i32(&mut self) -> Result<i32, DeserializeError> {
        let bytes = self.read_bytes(4)?;
        Ok(i32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn read_u64(&mut self) -> Result<u64, DeserializeError> {
        let bytes = self.read_bytes(8)?;
        Ok(u64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    fn read_i64(&mut self) -> Result<i64, DeserializeError> {
        let bytes = self.read_bytes(8)?;
        Ok(i64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    fn read_f32(&mut self) -> Result<f32, DeserializeError> {
        let bytes = self.read_bytes(4)?;
        Ok(f32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn read_f64(&mut self) -> Result<f64, DeserializeError> {
        let bytes = self.read_bytes(8)?;
        Ok(f64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    fn read_string(&mut self) -> Result<String, DeserializeError> {
        let len = self.read_u32()? as usize;
        let bytes = self.read_bytes(len)?;
        String::from_utf8(bytes.to_vec())
            .map_err(|e| DeserializeError::InvalidUtf8(e.to_string()))
    }
}

/// Deserialize a primitive value from the cursor.
fn deserialize_primitive(cursor: &mut Cursor, primitive: Primitive) -> Result<Value, DeserializeError> {
    match primitive {
        Primitive::Bool => {
            let byte = cursor.read_u8()?;
            match byte {
                0 => Ok(Value::Bool(false)),
                1 => Ok(Value::Bool(true)),
                // ROS1 is lenient - any non-zero is true
                _ => Ok(Value::Bool(true)),
            }
        }
        Primitive::Int8 => Ok(Value::Int8(cursor.read_i8()?)),
        Primitive::Int16 => Ok(Value::Int16(cursor.read_i16()?)),
        Primitive::Int32 => Ok(Value::Int32(cursor.read_i32()?)),
        Primitive::Int64 => Ok(Value::Int64(cursor.read_i64()?)),
        Primitive::UInt8 => Ok(Value::UInt8(cursor.read_u8()?)),
        Primitive::UInt16 => Ok(Value::UInt16(cursor.read_u16()?)),
        Primitive::UInt32 => Ok(Value::UInt32(cursor.read_u32()?)),
        Primitive::UInt64 => Ok(Value::UInt64(cursor.read_u64()?)),
        Primitive::Float32 => Ok(Value::Float32(cursor.read_f32()?)),
        Primitive::Float64 => Ok(Value::Float64(cursor.read_f64()?)),
        Primitive::String => Ok(Value::String(cursor.read_string()?)),
        Primitive::Time => {
            let sec = cursor.read_u32()?;
            let nsec = cursor.read_u32()?;
            Ok(Value::Time { sec, nsec })
        }
        Primitive::Duration => {
            let sec = cursor.read_i32()?;
            let nsec = cursor.read_i32()?;
            Ok(Value::Duration { sec, nsec })
        }
    }
}

/// Deserialize a field type from the cursor.
fn deserialize_field_type<R: TypeRegistry>(
    cursor: &mut Cursor,
    field_type: &FieldType,
    registry: &R,
) -> Result<Value, DeserializeError> {
    match field_type {
        FieldType::Primitive(p) => deserialize_primitive(cursor, *p),

        FieldType::Array { element } => {
            let count = cursor.read_u32()? as usize;
            let mut values = Vec::with_capacity(count);
            for _ in 0..count {
                values.push(deserialize_field_type(cursor, element, registry)?);
            }
            Ok(Value::Array(values))
        }

        FieldType::FixedArray { element, length } => {
            let mut values = Vec::with_capacity(*length);
            for _ in 0..*length {
                values.push(deserialize_field_type(cursor, element, registry)?);
            }
            Ok(Value::Array(values))
        }

        FieldType::Nested { package, name } => {
            let full_name = match package {
                Some(pkg) => format!("{}/{}", pkg, name),
                None => name.clone(),
            };

            let nested_def = registry
                .get(&full_name)
                .ok_or_else(|| DeserializeError::UnresolvedType(full_name))?;

            deserialize_message(cursor, nested_def, registry)
        }
    }
}

/// Deserialize a complete message from the cursor.
fn deserialize_message<R: TypeRegistry>(
    cursor: &mut Cursor,
    definition: &MessageDefinition,
    registry: &R,
) -> Result<Value, DeserializeError> {
    let mut fields = HashMap::new();

    for field in &definition.fields {
        let value = deserialize_field_type(cursor, &field.field_type, registry)?;
        fields.insert(field.name.clone(), value);
    }

    Ok(Value::Message(fields))
}

/// Deserialize a ROS1 message from bytes.
///
/// This is the main entry point for deserialization.
pub fn deserialize<R: TypeRegistry>(
    data: &[u8],
    definition: &MessageDefinition,
    registry: &R,
) -> Result<Value, DeserializeError> {
    let mut cursor = Cursor::new(data);
    deserialize_message(&mut cursor, definition, registry)
}

/// Deserialize a simple message with no nested types.
pub fn deserialize_simple(
    data: &[u8],
    definition: &MessageDefinition,
) -> Result<Value, DeserializeError> {
    let empty: HashMap<String, MessageDefinition> = HashMap::new();
    deserialize(data, definition, &empty)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ros1::MessageDefinition;

    mod primitive_deserialization {
        use super::*;

        #[test]
        fn test_bool_false() {
            let def = MessageDefinition::parse("bool value").unwrap();
            let data = [0u8];
            let value = deserialize_simple(&data, &def).unwrap();
            assert_eq!(value.get("value").unwrap().as_bool(), Some(false));
        }

        #[test]
        fn test_bool_true() {
            let def = MessageDefinition::parse("bool value").unwrap();
            let data = [1u8];
            let value = deserialize_simple(&data, &def).unwrap();
            assert_eq!(value.get("value").unwrap().as_bool(), Some(true));
        }

        #[test]
        fn test_bool_nonzero_is_true() {
            let def = MessageDefinition::parse("bool value").unwrap();
            let data = [42u8];
            let value = deserialize_simple(&data, &def).unwrap();
            assert_eq!(value.get("value").unwrap().as_bool(), Some(true));
        }

        #[test]
        fn test_int8() {
            let def = MessageDefinition::parse("int8 value").unwrap();
            let data = [0xFEu8]; // -2 in two's complement
            let value = deserialize_simple(&data, &def).unwrap();
            assert_eq!(value.get("value").unwrap().as_i64(), Some(-2));
        }

        #[test]
        fn test_uint8() {
            let def = MessageDefinition::parse("uint8 value").unwrap();
            let data = [255u8];
            let value = deserialize_simple(&data, &def).unwrap();
            assert_eq!(value.get("value").unwrap().as_u64(), Some(255));
        }

        #[test]
        fn test_int16() {
            let def = MessageDefinition::parse("int16 value").unwrap();
            let data = 1000i16.to_le_bytes();
            let value = deserialize_simple(&data, &def).unwrap();
            assert_eq!(value.get("value").unwrap().as_i64(), Some(1000));
        }

        #[test]
        fn test_int16_negative() {
            let def = MessageDefinition::parse("int16 value").unwrap();
            let data = (-1000i16).to_le_bytes();
            let value = deserialize_simple(&data, &def).unwrap();
            assert_eq!(value.get("value").unwrap().as_i64(), Some(-1000));
        }

        #[test]
        fn test_uint16() {
            let def = MessageDefinition::parse("uint16 value").unwrap();
            let data = 65535u16.to_le_bytes();
            let value = deserialize_simple(&data, &def).unwrap();
            assert_eq!(value.get("value").unwrap().as_u64(), Some(65535));
        }

        #[test]
        fn test_int32() {
            let def = MessageDefinition::parse("int32 value").unwrap();
            let data = 123456i32.to_le_bytes();
            let value = deserialize_simple(&data, &def).unwrap();
            assert_eq!(value.get("value").unwrap().as_i64(), Some(123456));
        }

        #[test]
        fn test_uint32() {
            let def = MessageDefinition::parse("uint32 value").unwrap();
            let data = 0xDEADBEEFu32.to_le_bytes();
            let value = deserialize_simple(&data, &def).unwrap();
            assert_eq!(value.get("value").unwrap().as_u64(), Some(0xDEADBEEF));
        }

        #[test]
        fn test_int64() {
            let def = MessageDefinition::parse("int64 value").unwrap();
            let data = (-9_000_000_000i64).to_le_bytes();
            let value = deserialize_simple(&data, &def).unwrap();
            assert_eq!(value.get("value").unwrap().as_i64(), Some(-9_000_000_000));
        }

        #[test]
        fn test_uint64() {
            let def = MessageDefinition::parse("uint64 value").unwrap();
            let data = 0xDEADBEEFCAFEBABEu64.to_le_bytes();
            let value = deserialize_simple(&data, &def).unwrap();
            match value.get("value").unwrap() {
                Value::UInt64(v) => assert_eq!(*v, 0xDEADBEEFCAFEBABE),
                _ => panic!("Expected UInt64"),
            }
        }

        #[test]
        fn test_float32() {
            let def = MessageDefinition::parse("float32 value").unwrap();
            let data = std::f32::consts::PI.to_le_bytes();
            let value = deserialize_simple(&data, &def).unwrap();
            let f = value.get("value").unwrap().as_f64().unwrap();
            assert!((f - std::f64::consts::PI).abs() < 0.0001);
        }

        #[test]
        fn test_float64() {
            let def = MessageDefinition::parse("float64 value").unwrap();
            let data = std::f64::consts::E.to_le_bytes();
            let value = deserialize_simple(&data, &def).unwrap();
            let f = value.get("value").unwrap().as_f64().unwrap();
            assert!((f - std::f64::consts::E).abs() < 0.0000001);
        }

        #[test]
        fn test_string() {
            let def = MessageDefinition::parse("string value").unwrap();
            let mut data = Vec::new();
            data.extend_from_slice(&5u32.to_le_bytes()); // length
            data.extend_from_slice(b"hello");
            let value = deserialize_simple(&data, &def).unwrap();
            assert_eq!(value.get("value").unwrap().as_str(), Some("hello"));
        }

        #[test]
        fn test_empty_string() {
            let def = MessageDefinition::parse("string value").unwrap();
            let data = 0u32.to_le_bytes();
            let value = deserialize_simple(&data, &def).unwrap();
            assert_eq!(value.get("value").unwrap().as_str(), Some(""));
        }

        #[test]
        fn test_string_unicode() {
            let def = MessageDefinition::parse("string value").unwrap();
            let text = "héllo 世界 🦀";
            let text_bytes = text.as_bytes();
            let mut data = Vec::new();
            data.extend_from_slice(&(text_bytes.len() as u32).to_le_bytes());
            data.extend_from_slice(text_bytes);
            let value = deserialize_simple(&data, &def).unwrap();
            assert_eq!(value.get("value").unwrap().as_str(), Some(text));
        }

        #[test]
        fn test_time() {
            let def = MessageDefinition::parse("time value").unwrap();
            let mut data = Vec::new();
            data.extend_from_slice(&1234567890u32.to_le_bytes()); // sec
            data.extend_from_slice(&123456789u32.to_le_bytes()); // nsec
            let value = deserialize_simple(&data, &def).unwrap();
            match value.get("value").unwrap() {
                Value::Time { sec, nsec } => {
                    assert_eq!(*sec, 1234567890);
                    assert_eq!(*nsec, 123456789);
                }
                _ => panic!("Expected Time"),
            }
        }

        #[test]
        fn test_duration() {
            let def = MessageDefinition::parse("duration value").unwrap();
            let mut data = Vec::new();
            data.extend_from_slice(&(-10i32).to_le_bytes()); // sec (negative)
            data.extend_from_slice(&500000000u32.to_le_bytes()); // nsec
            let value = deserialize_simple(&data, &def).unwrap();
            match value.get("value").unwrap() {
                Value::Duration { sec, nsec } => {
                    assert_eq!(*sec, -10);
                    assert_eq!(*nsec, 500000000);
                }
                _ => panic!("Expected Duration"),
            }
        }
    }

    mod array_deserialization {
        use super::*;

        #[test]
        fn test_variable_array_empty() {
            let def = MessageDefinition::parse("uint8[] data").unwrap();
            let data = 0u32.to_le_bytes();
            let value = deserialize_simple(&data, &def).unwrap();
            let arr = value.get("data").unwrap().as_array().unwrap();
            assert!(arr.is_empty());
        }

        #[test]
        fn test_variable_array_uint8() {
            let def = MessageDefinition::parse("uint8[] data").unwrap();
            let mut data = Vec::new();
            data.extend_from_slice(&4u32.to_le_bytes()); // count
            data.extend_from_slice(&[1, 2, 3, 4]);
            let value = deserialize_simple(&data, &def).unwrap();
            let arr = value.get("data").unwrap().as_array().unwrap();
            assert_eq!(arr.len(), 4);
            assert_eq!(arr[0].as_u64(), Some(1));
            assert_eq!(arr[3].as_u64(), Some(4));
        }

        #[test]
        fn test_variable_array_float64() {
            let def = MessageDefinition::parse("float64[] values").unwrap();
            let mut data = Vec::new();
            data.extend_from_slice(&3u32.to_le_bytes());
            data.extend_from_slice(&1.5f64.to_le_bytes());
            data.extend_from_slice(&2.5f64.to_le_bytes());
            data.extend_from_slice(&3.5f64.to_le_bytes());
            let value = deserialize_simple(&data, &def).unwrap();
            let arr = value.get("values").unwrap().as_array().unwrap();
            assert_eq!(arr.len(), 3);
            assert_eq!(arr[0].as_f64(), Some(1.5));
            assert_eq!(arr[1].as_f64(), Some(2.5));
            assert_eq!(arr[2].as_f64(), Some(3.5));
        }

        #[test]
        fn test_fixed_array() {
            let def = MessageDefinition::parse("float64[3] position").unwrap();
            let mut data = Vec::new();
            // No length prefix for fixed arrays!
            data.extend_from_slice(&1.0f64.to_le_bytes());
            data.extend_from_slice(&2.0f64.to_le_bytes());
            data.extend_from_slice(&3.0f64.to_le_bytes());
            let value = deserialize_simple(&data, &def).unwrap();
            let arr = value.get("position").unwrap().as_array().unwrap();
            assert_eq!(arr.len(), 3);
            assert_eq!(arr[0].as_f64(), Some(1.0));
            assert_eq!(arr[1].as_f64(), Some(2.0));
            assert_eq!(arr[2].as_f64(), Some(3.0));
        }

        #[test]
        fn test_variable_array_strings() {
            let def = MessageDefinition::parse("string[] names").unwrap();
            let mut data = Vec::new();
            data.extend_from_slice(&2u32.to_le_bytes()); // count
            // First string
            data.extend_from_slice(&5u32.to_le_bytes());
            data.extend_from_slice(b"alice");
            // Second string
            data.extend_from_slice(&3u32.to_le_bytes());
            data.extend_from_slice(b"bob");
            let value = deserialize_simple(&data, &def).unwrap();
            let arr = value.get("names").unwrap().as_array().unwrap();
            assert_eq!(arr.len(), 2);
            assert_eq!(arr[0].as_str(), Some("alice"));
            assert_eq!(arr[1].as_str(), Some("bob"));
        }
    }

    mod multi_field_messages {
        use super::*;

        #[test]
        fn test_point3d() {
            let def = MessageDefinition::parse("float64 x\nfloat64 y\nfloat64 z").unwrap();
            let mut data = Vec::new();
            data.extend_from_slice(&1.0f64.to_le_bytes());
            data.extend_from_slice(&2.0f64.to_le_bytes());
            data.extend_from_slice(&3.0f64.to_le_bytes());
            let value = deserialize_simple(&data, &def).unwrap();
            assert_eq!(value.get("x").unwrap().as_f64(), Some(1.0));
            assert_eq!(value.get("y").unwrap().as_f64(), Some(2.0));
            assert_eq!(value.get("z").unwrap().as_f64(), Some(3.0));
        }

        #[test]
        fn test_header_like() {
            let def = MessageDefinition::parse(
                r#"
                uint32 seq
                time stamp
                string frame_id
            "#,
            )
            .unwrap();

            let mut data = Vec::new();
            data.extend_from_slice(&42u32.to_le_bytes()); // seq
            data.extend_from_slice(&1000u32.to_le_bytes()); // stamp.sec
            data.extend_from_slice(&500u32.to_le_bytes()); // stamp.nsec
            data.extend_from_slice(&10u32.to_le_bytes()); // frame_id length
            data.extend_from_slice(b"base_link\0"); // include null for fun

            let value = deserialize_simple(&data, &def).unwrap();
            assert_eq!(value.get("seq").unwrap().as_u64(), Some(42));
            match value.get("stamp").unwrap() {
                Value::Time { sec, nsec } => {
                    assert_eq!(*sec, 1000);
                    assert_eq!(*nsec, 500);
                }
                _ => panic!("Expected Time"),
            }
            assert_eq!(value.get("frame_id").unwrap().as_str(), Some("base_link\0"));
        }

        #[test]
        fn test_mixed_types() {
            let def = MessageDefinition::parse(
                r#"
                bool active
                int32 count
                float64 ratio
                string name
                uint8[] data
            "#,
            )
            .unwrap();

            let mut data = Vec::new();
            data.push(1); // active
            data.extend_from_slice(&100i32.to_le_bytes()); // count
            data.extend_from_slice(&0.75f64.to_le_bytes()); // ratio
            data.extend_from_slice(&4u32.to_le_bytes()); // name length
            data.extend_from_slice(b"test"); // name
            data.extend_from_slice(&3u32.to_le_bytes()); // data count
            data.extend_from_slice(&[10, 20, 30]); // data

            let value = deserialize_simple(&data, &def).unwrap();
            assert_eq!(value.get("active").unwrap().as_bool(), Some(true));
            assert_eq!(value.get("count").unwrap().as_i64(), Some(100));
            assert_eq!(value.get("ratio").unwrap().as_f64(), Some(0.75));
            assert_eq!(value.get("name").unwrap().as_str(), Some("test"));
            let arr = value.get("data").unwrap().as_array().unwrap();
            assert_eq!(arr.len(), 3);
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

            let mut data = Vec::new();
            data.extend_from_slice(&1.0f64.to_le_bytes());
            data.extend_from_slice(&2.0f64.to_le_bytes());
            data.extend_from_slice(&3.0f64.to_le_bytes());

            let value = deserialize(&data, &def, &registry).unwrap();
            let pos = value.get("position").unwrap().as_message().unwrap();
            assert_eq!(pos.get("x").unwrap().as_f64(), Some(1.0));
            assert_eq!(pos.get("y").unwrap().as_f64(), Some(2.0));
            assert_eq!(pos.get("z").unwrap().as_f64(), Some(3.0));
        }

        #[test]
        fn test_deeply_nested() {
            let mut registry = MessageRegistry::new();

            let point_def = MessageDefinition::parse("float64 x\nfloat64 y\nfloat64 z").unwrap();
            registry.register("geometry_msgs/Point", point_def);

            let quat_def =
                MessageDefinition::parse("float64 x\nfloat64 y\nfloat64 z\nfloat64 w").unwrap();
            registry.register("geometry_msgs/Quaternion", quat_def);

            let pose_def = MessageDefinition::parse(
                "geometry_msgs/Point position\ngeometry_msgs/Quaternion orientation",
            )
            .unwrap();
            registry.register("geometry_msgs/Pose", pose_def);

            let def = MessageDefinition::parse("geometry_msgs/Pose pose").unwrap();

            let mut data = Vec::new();
            // position: x, y, z
            data.extend_from_slice(&1.0f64.to_le_bytes());
            data.extend_from_slice(&2.0f64.to_le_bytes());
            data.extend_from_slice(&3.0f64.to_le_bytes());
            // orientation: x, y, z, w
            data.extend_from_slice(&0.0f64.to_le_bytes());
            data.extend_from_slice(&0.0f64.to_le_bytes());
            data.extend_from_slice(&0.0f64.to_le_bytes());
            data.extend_from_slice(&1.0f64.to_le_bytes());

            let value = deserialize(&data, &def, &registry).unwrap();

            // Navigate: value -> pose -> position -> x
            let pose = value.get("pose").unwrap();
            let position = pose.get("position").unwrap();
            assert_eq!(position.get("x").unwrap().as_f64(), Some(1.0));

            let orientation = pose.get("orientation").unwrap();
            assert_eq!(orientation.get("w").unwrap().as_f64(), Some(1.0));
        }

        #[test]
        fn test_array_of_nested() {
            let mut registry = MessageRegistry::new();

            let point_def = MessageDefinition::parse("float64 x\nfloat64 y").unwrap();
            registry.register("Point", point_def);

            let def = MessageDefinition::parse("Point[] points").unwrap();

            let mut data = Vec::new();
            data.extend_from_slice(&2u32.to_le_bytes()); // count
            // Point 1
            data.extend_from_slice(&1.0f64.to_le_bytes());
            data.extend_from_slice(&2.0f64.to_le_bytes());
            // Point 2
            data.extend_from_slice(&3.0f64.to_le_bytes());
            data.extend_from_slice(&4.0f64.to_le_bytes());

            let value = deserialize(&data, &def, &registry).unwrap();
            let points = value.get("points").unwrap().as_array().unwrap();
            assert_eq!(points.len(), 2);
            assert_eq!(points[0].get("x").unwrap().as_f64(), Some(1.0));
            assert_eq!(points[1].get("y").unwrap().as_f64(), Some(4.0));
        }

        #[test]
        fn test_unresolved_nested_type() {
            let registry = MessageRegistry::new();
            let def = MessageDefinition::parse("geometry_msgs/Point position").unwrap();
            let data = [0u8; 24];
            let result = deserialize(&data, &def, &registry);
            assert!(matches!(result, Err(DeserializeError::UnresolvedType(_))));
        }
    }

    mod error_handling {
        use super::*;

        #[test]
        fn test_unexpected_eof_primitive() {
            let def = MessageDefinition::parse("int32 value").unwrap();
            let data = [0u8; 2]; // Only 2 bytes, need 4
            let result = deserialize_simple(&data, &def);
            match result {
                Err(DeserializeError::UnexpectedEof { needed: 4, available: 2 }) => {}
                _ => panic!("Expected UnexpectedEof error"),
            }
        }

        #[test]
        fn test_unexpected_eof_string() {
            let def = MessageDefinition::parse("string value").unwrap();
            let mut data = Vec::new();
            data.extend_from_slice(&100u32.to_le_bytes()); // claims 100 bytes
            data.extend_from_slice(b"short"); // only 5 bytes
            let result = deserialize_simple(&data, &def);
            assert!(matches!(result, Err(DeserializeError::UnexpectedEof { .. })));
        }

        #[test]
        fn test_unexpected_eof_array() {
            let def = MessageDefinition::parse("uint32[] values").unwrap();
            let mut data = Vec::new();
            data.extend_from_slice(&10u32.to_le_bytes()); // claims 10 elements
            data.extend_from_slice(&1u32.to_le_bytes()); // only 1 element
            let result = deserialize_simple(&data, &def);
            assert!(matches!(result, Err(DeserializeError::UnexpectedEof { .. })));
        }

        #[test]
        fn test_invalid_utf8() {
            let def = MessageDefinition::parse("string value").unwrap();
            let mut data = Vec::new();
            data.extend_from_slice(&4u32.to_le_bytes());
            data.extend_from_slice(&[0xFF, 0xFE, 0x00, 0x01]); // Invalid UTF-8
            let result = deserialize_simple(&data, &def);
            assert!(matches!(result, Err(DeserializeError::InvalidUtf8(_))));
        }
    }

    mod real_world_messages {
        use super::*;
        use crate::ros1::MessageRegistry;

        #[test]
        fn test_sensor_msgs_image_like() {
            let mut registry = MessageRegistry::new();

            let header_def =
                MessageDefinition::parse("uint32 seq\ntime stamp\nstring frame_id").unwrap();
            registry.register("std_msgs/Header", header_def);

            let def = MessageDefinition::parse(
                r#"
                std_msgs/Header header
                uint32 height
                uint32 width
                string encoding
                uint8 is_bigendian
                uint32 step
                uint8[] data
            "#,
            )
            .unwrap();

            let mut data = Vec::new();
            // header
            data.extend_from_slice(&1u32.to_le_bytes()); // seq
            data.extend_from_slice(&1000u32.to_le_bytes()); // stamp.sec
            data.extend_from_slice(&0u32.to_le_bytes()); // stamp.nsec
            data.extend_from_slice(&6u32.to_le_bytes()); // frame_id len
            data.extend_from_slice(b"camera");
            // image fields
            data.extend_from_slice(&480u32.to_le_bytes()); // height
            data.extend_from_slice(&640u32.to_le_bytes()); // width
            data.extend_from_slice(&4u32.to_le_bytes()); // encoding len
            data.extend_from_slice(b"rgb8");
            data.push(0); // is_bigendian
            data.extend_from_slice(&1920u32.to_le_bytes()); // step
            // Small fake image data
            data.extend_from_slice(&6u32.to_le_bytes()); // data len
            data.extend_from_slice(&[255, 0, 0, 0, 255, 0]); // 2 pixels

            let value = deserialize(&data, &def, &registry).unwrap();

            assert_eq!(value.get("height").unwrap().as_u64(), Some(480));
            assert_eq!(value.get("width").unwrap().as_u64(), Some(640));
            assert_eq!(value.get("encoding").unwrap().as_str(), Some("rgb8"));

            let header = value.get("header").unwrap();
            assert_eq!(header.get("seq").unwrap().as_u64(), Some(1));
            assert_eq!(header.get("frame_id").unwrap().as_str(), Some("camera"));

            let img_data = value.get("data").unwrap().as_array().unwrap();
            assert_eq!(img_data.len(), 6);
            assert_eq!(img_data[0].as_u64(), Some(255)); // R
        }

        #[test]
        fn test_covariance_matrix() {
            let def = MessageDefinition::parse("float64[36] covariance").unwrap();

            let mut data = Vec::new();
            // Identity-like covariance (diagonal = 1, rest = 0)
            for i in 0..36 {
                let val = if i % 7 == 0 { 1.0f64 } else { 0.0f64 };
                data.extend_from_slice(&val.to_le_bytes());
            }

            let value = deserialize_simple(&data, &def).unwrap();
            let cov = value.get("covariance").unwrap().as_array().unwrap();
            assert_eq!(cov.len(), 36);
            assert_eq!(cov[0].as_f64(), Some(1.0)); // [0,0]
            assert_eq!(cov[1].as_f64(), Some(0.0)); // [0,1]
            assert_eq!(cov[7].as_f64(), Some(1.0)); // [1,1]
        }
    }
}
