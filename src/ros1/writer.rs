//! ROS1 serialization using the generic RowSource trait.
//!
//! This module provides `Ros1Writer`, which serializes data from any `RowSource`
//! implementation to ROS1 binary format. This enables writing ROS1 messages from
//! Arrow, Parquet, or any other source that implements `RowSource`.

use std::fmt;

use thiserror::Error;

use super::from_arrow::Ros1FieldInfo;
use crate::source::RowSource;

/// Errors that can occur during ROS1 serialization via RowSource.
#[derive(Debug, Error)]
pub enum WriteError {
    #[error("source error: {0}")]
    Source(String),

    #[error("unsupported ROS1 type: {0}")]
    UnsupportedType(String),

    #[error("missing nested field info for {0}")]
    MissingNestedInfo(String),

    #[error("missing element info for array {0}")]
    MissingElementInfo(String),
}

/// A writer that serializes data from a `RowSource` to ROS1 binary format.
///
/// Unlike the direct `Serializer`, this writer is generic over the source,
/// enabling serialization from any format that implements `RowSource`.
pub struct Ros1Writer {
    buffer: Vec<u8>,
}

impl Ros1Writer {
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

    /// Serialize a single row from a `RowSource` using field info.
    ///
    /// The source should be positioned at a valid row (after calling `next_row()`).
    pub fn write_row<S: RowSource>(
        &mut self,
        source: &mut S,
        field_infos: &[Ros1FieldInfo],
    ) -> Result<(), WriteError>
    where
        S::Error: fmt::Display,
    {
        for info in field_infos {
            self.write_field(source, info)?;
        }
        Ok(())
    }

    /// Serialize a single field from the source.
    fn write_field<S: RowSource>(
        &mut self,
        source: &mut S,
        info: &Ros1FieldInfo,
    ) -> Result<(), WriteError>
    where
        S::Error: fmt::Display,
    {
        // Check for nested/array types first, then primitives
        if info.nested.is_some() {
            return self.write_struct(source, info);
        }

        if info.element.is_some() {
            if info.fixed_size.is_some() {
                return self.write_fixed_array(source, info);
            } else {
                return self.write_variable_array(source, info);
            }
        }

        // Primitive type - determine from ros_type
        match info.ros_type.as_str() {
            "bool" => {
                let value = source
                    .read_bool()
                    .map_err(|e| WriteError::Source(e.to_string()))?;
                self.write_bool(value);
            }
            "int8" | "byte" => {
                let value = source
                    .read_i8()
                    .map_err(|e| WriteError::Source(e.to_string()))?;
                self.write_i8(value);
            }
            "int16" => {
                let value = source
                    .read_i16()
                    .map_err(|e| WriteError::Source(e.to_string()))?;
                self.write_i16(value);
            }
            "int32" => {
                let value = source
                    .read_i32()
                    .map_err(|e| WriteError::Source(e.to_string()))?;
                self.write_i32(value);
            }
            "int64" => {
                let value = source
                    .read_i64()
                    .map_err(|e| WriteError::Source(e.to_string()))?;
                self.write_i64(value);
            }
            "uint8" | "char" => {
                let value = source
                    .read_u8()
                    .map_err(|e| WriteError::Source(e.to_string()))?;
                self.write_u8(value);
            }
            "uint16" => {
                let value = source
                    .read_u16()
                    .map_err(|e| WriteError::Source(e.to_string()))?;
                self.write_u16(value);
            }
            "uint32" => {
                let value = source
                    .read_u32()
                    .map_err(|e| WriteError::Source(e.to_string()))?;
                self.write_u32(value);
            }
            "uint64" => {
                let value = source
                    .read_u64()
                    .map_err(|e| WriteError::Source(e.to_string()))?;
                self.write_u64(value);
            }
            "float32" => {
                let value = source
                    .read_f32()
                    .map_err(|e| WriteError::Source(e.to_string()))?;
                self.write_f32(value);
            }
            "float64" => {
                let value = source
                    .read_f64()
                    .map_err(|e| WriteError::Source(e.to_string()))?;
                self.write_f64(value);
            }
            "string" => {
                let value = source
                    .read_string()
                    .map_err(|e| WriteError::Source(e.to_string()))?;
                self.write_string(&value);
            }
            "time" => {
                let nanos = source
                    .read_timestamp_nanos()
                    .map_err(|e| WriteError::Source(e.to_string()))?;
                // ROS1 time is sec (u32) + nsec (u32)
                let sec = (nanos / 1_000_000_000) as u32;
                let nsec = (nanos % 1_000_000_000) as u32;
                self.write_u32(sec);
                self.write_u32(nsec);
            }
            "duration" => {
                let nanos = source
                    .read_duration_nanos()
                    .map_err(|e| WriteError::Source(e.to_string()))?;
                // ROS1 duration is sec (i32) + nsec (i32)
                // Ensure nsec is non-negative (ROS1 convention)
                let mut sec = (nanos / 1_000_000_000) as i32;
                let mut nsec = (nanos % 1_000_000_000) as i32;
                if nsec < 0 {
                    sec -= 1;
                    nsec += 1_000_000_000;
                }
                self.write_i32(sec);
                self.write_i32(nsec);
            }
            other => {
                return Err(WriteError::UnsupportedType(other.to_string()));
            }
        }

        Ok(())
    }

    /// Serialize a struct (nested message).
    fn write_struct<S: RowSource>(
        &mut self,
        source: &mut S,
        info: &Ros1FieldInfo,
    ) -> Result<(), WriteError>
    where
        S::Error: fmt::Display,
    {
        let nested_infos = info
            .nested
            .as_ref()
            .ok_or_else(|| WriteError::MissingNestedInfo(info.name.clone()))?;

        source
            .enter_struct()
            .map_err(|e| WriteError::Source(e.to_string()))?;

        for nested_info in nested_infos {
            self.write_field(source, nested_info)?;
        }

        source
            .exit_struct()
            .map_err(|e| WriteError::Source(e.to_string()))?;

        Ok(())
    }

    /// Serialize a variable-length array.
    fn write_variable_array<S: RowSource>(
        &mut self,
        source: &mut S,
        info: &Ros1FieldInfo,
    ) -> Result<(), WriteError>
    where
        S::Error: fmt::Display,
    {
        let element_info = info
            .element
            .as_ref()
            .ok_or_else(|| WriteError::MissingElementInfo(info.name.clone()))?;

        let len = source
            .enter_list()
            .map_err(|e| WriteError::Source(e.to_string()))?;

        // Write length prefix
        self.write_u32(len as u32);

        // Write elements
        for _ in 0..len {
            self.write_field(source, element_info)?;
        }

        source
            .exit_list()
            .map_err(|e| WriteError::Source(e.to_string()))?;

        Ok(())
    }

    /// Serialize a fixed-length array.
    fn write_fixed_array<S: RowSource>(
        &mut self,
        source: &mut S,
        info: &Ros1FieldInfo,
    ) -> Result<(), WriteError>
    where
        S::Error: fmt::Display,
    {
        let element_info = info
            .element
            .as_ref()
            .ok_or_else(|| WriteError::MissingElementInfo(info.name.clone()))?;

        let len = source
            .enter_list()
            .map_err(|e| WriteError::Source(e.to_string()))?;

        // No length prefix for fixed arrays!
        for _ in 0..len {
            self.write_field(source, element_info)?;
        }

        source
            .exit_list()
            .map_err(|e| WriteError::Source(e.to_string()))?;

        Ok(())
    }

    // Primitive write methods

    fn write_bool(&mut self, value: bool) {
        self.buffer.push(if value { 1 } else { 0 });
    }

    fn write_i8(&mut self, value: i8) {
        self.buffer.push(value as u8);
    }

    fn write_u8(&mut self, value: u8) {
        self.buffer.push(value);
    }

    fn write_i16(&mut self, value: i16) {
        self.buffer.extend_from_slice(&value.to_le_bytes());
    }

    fn write_u16(&mut self, value: u16) {
        self.buffer.extend_from_slice(&value.to_le_bytes());
    }

    fn write_i32(&mut self, value: i32) {
        self.buffer.extend_from_slice(&value.to_le_bytes());
    }

    fn write_u32(&mut self, value: u32) {
        self.buffer.extend_from_slice(&value.to_le_bytes());
    }

    fn write_i64(&mut self, value: i64) {
        self.buffer.extend_from_slice(&value.to_le_bytes());
    }

    fn write_u64(&mut self, value: u64) {
        self.buffer.extend_from_slice(&value.to_le_bytes());
    }

    fn write_f32(&mut self, value: f32) {
        self.buffer.extend_from_slice(&value.to_le_bytes());
    }

    fn write_f64(&mut self, value: f64) {
        self.buffer.extend_from_slice(&value.to_le_bytes());
    }

    fn write_string(&mut self, value: &str) {
        let bytes = value.as_bytes();
        self.write_u32(bytes.len() as u32);
        self.buffer.extend_from_slice(bytes);
    }
}

impl Default for Ros1Writer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::ArrowRowSource;
    use crate::ros1::Ros1FieldInfo;
    use arrow::array::{
        ArrayRef, BooleanArray, Float64Array, Int32Array, StringArray, StructArray,
        TimestampNanosecondArray,
    };
    use arrow::datatypes::{DataType, Field, Fields, Schema, TimeUnit};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    fn make_field_info(name: &str, ros_type: &str) -> Ros1FieldInfo {
        Ros1FieldInfo {
            name: name.to_string(),
            ros_type: ros_type.to_string(),
            arrow_type: DataType::Null, // Not used by writer
            nested: None,
            element: None,
            fixed_size: None,
        }
    }

    #[test]
    fn test_write_primitives() {
        let schema = Arc::new(Schema::new(vec![
            Arc::new(Field::new("count", DataType::Int32, false)),
            Arc::new(Field::new("value", DataType::Float64, false)),
            Arc::new(Field::new("name", DataType::Utf8, false)),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![42])),
                Arc::new(Float64Array::from(vec![3.14])),
                Arc::new(StringArray::from(vec!["hello"])),
            ],
        )
        .unwrap();

        let mut source = ArrowRowSource::new(batch);
        source.next_row().unwrap();

        let field_infos = vec![
            make_field_info("count", "int32"),
            make_field_info("value", "float64"),
            make_field_info("name", "string"),
        ];

        let mut writer = Ros1Writer::new();
        writer.write_row(&mut source, &field_infos).unwrap();

        let bytes = writer.as_bytes();

        // Verify: 4 bytes int32 + 8 bytes float64 + 4 bytes len + 5 bytes "hello"
        assert_eq!(bytes.len(), 4 + 8 + 4 + 5);

        // Check int32
        assert_eq!(
            i32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]),
            42
        );

        // Check float64
        let float_bytes: [u8; 8] = bytes[4..12].try_into().unwrap();
        assert!((f64::from_le_bytes(float_bytes) - 3.14).abs() < 0.0001);

        // Check string length
        assert_eq!(
            u32::from_le_bytes([bytes[12], bytes[13], bytes[14], bytes[15]]),
            5
        );

        // Check string content
        assert_eq!(&bytes[16..21], b"hello");
    }

    #[test]
    fn test_write_bool() {
        let schema = Arc::new(Schema::new(vec![Arc::new(Field::new(
            "flag",
            DataType::Boolean,
            false,
        ))]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(BooleanArray::from(vec![true, false]))],
        )
        .unwrap();

        let field_infos = vec![make_field_info("flag", "bool")];

        // Test true
        let mut source = ArrowRowSource::new(batch.clone());
        source.next_row().unwrap();
        let mut writer = Ros1Writer::new();
        writer.write_row(&mut source, &field_infos).unwrap();
        assert_eq!(writer.as_bytes(), &[1]);

        // Test false
        source.next_row().unwrap();
        writer.clear();
        writer.write_row(&mut source, &field_infos).unwrap();
        assert_eq!(writer.as_bytes(), &[0]);
    }

    #[test]
    fn test_write_time() {
        let schema = Arc::new(Schema::new(vec![Arc::new(Field::new(
            "stamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ))]));

        // 1234567890 seconds + 123456789 nanoseconds
        let nanos: i64 = 1234567890 * 1_000_000_000 + 123456789;
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(TimestampNanosecondArray::from(vec![nanos]))],
        )
        .unwrap();

        let mut source = ArrowRowSource::new(batch);
        source.next_row().unwrap();

        let field_infos = vec![make_field_info("stamp", "time")];

        let mut writer = Ros1Writer::new();
        writer.write_row(&mut source, &field_infos).unwrap();

        let bytes = writer.as_bytes();
        assert_eq!(bytes.len(), 8);

        let sec = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        let nsec = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
        assert_eq!(sec, 1234567890);
        assert_eq!(nsec, 123456789);
    }

    #[test]
    fn test_write_struct() {
        let inner_fields = Fields::from(vec![
            Arc::new(Field::new("x", DataType::Float64, false)),
            Arc::new(Field::new("y", DataType::Float64, false)),
        ]);

        let struct_array = StructArray::from(vec![
            (
                Arc::new(Field::new("x", DataType::Float64, false)),
                Arc::new(Float64Array::from(vec![1.5])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("y", DataType::Float64, false)),
                Arc::new(Float64Array::from(vec![2.5])) as ArrayRef,
            ),
        ]);

        let schema = Arc::new(Schema::new(vec![Arc::new(Field::new(
            "point",
            DataType::Struct(inner_fields),
            false,
        ))]));

        let batch = RecordBatch::try_new(schema, vec![Arc::new(struct_array)]).unwrap();

        let mut source = ArrowRowSource::new(batch);
        source.next_row().unwrap();

        let field_infos = vec![Ros1FieldInfo {
            name: "point".to_string(),
            ros_type: "geometry_msgs/Point".to_string(),
            arrow_type: DataType::Null,
            nested: Some(vec![
                make_field_info("x", "float64"),
                make_field_info("y", "float64"),
            ]),
            element: None,
            fixed_size: None,
        }];

        let mut writer = Ros1Writer::new();
        writer.write_row(&mut source, &field_infos).unwrap();

        let bytes = writer.as_bytes();
        assert_eq!(bytes.len(), 16); // 2 * 8 bytes

        let x: [u8; 8] = bytes[0..8].try_into().unwrap();
        let y: [u8; 8] = bytes[8..16].try_into().unwrap();
        assert_eq!(f64::from_le_bytes(x), 1.5);
        assert_eq!(f64::from_le_bytes(y), 2.5);
    }

    #[test]
    fn test_write_variable_array() {
        let list_array =
            arrow::array::ListArray::from_iter_primitive::<arrow::datatypes::UInt8Type, _, _>(
                vec![Some(vec![Some(1u8), Some(2), Some(3)])],
            );

        let schema = Arc::new(Schema::new(vec![Arc::new(Field::new(
            "data",
            DataType::List(Arc::new(Field::new("item", DataType::UInt8, true))),
            false,
        ))]));

        let batch = RecordBatch::try_new(schema, vec![Arc::new(list_array)]).unwrap();

        let mut source = ArrowRowSource::new(batch);
        source.next_row().unwrap();

        let field_infos = vec![Ros1FieldInfo {
            name: "data".to_string(),
            ros_type: "uint8[]".to_string(),
            arrow_type: DataType::Null,
            nested: None,
            element: Some(Box::new(make_field_info("item", "uint8"))),
            fixed_size: None,
        }];

        let mut writer = Ros1Writer::new();
        writer.write_row(&mut source, &field_infos).unwrap();

        let bytes = writer.as_bytes();
        // 4 bytes length + 3 bytes data
        assert_eq!(bytes.len(), 7);
        assert_eq!(
            u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]),
            3
        );
        assert_eq!(&bytes[4..], &[1, 2, 3]);
    }

    #[test]
    fn test_write_fixed_array() {
        let list_array = arrow::array::FixedSizeListArray::from_iter_primitive::<
            arrow::datatypes::Float64Type,
            _,
            _,
        >(vec![Some(vec![Some(1.0), Some(2.0), Some(3.0)])], 3);

        let schema = Arc::new(Schema::new(vec![Arc::new(Field::new(
            "position",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float64, true)), 3),
            false,
        ))]));

        let batch = RecordBatch::try_new(schema, vec![Arc::new(list_array)]).unwrap();

        let mut source = ArrowRowSource::new(batch);
        source.next_row().unwrap();

        let field_infos = vec![Ros1FieldInfo {
            name: "position".to_string(),
            ros_type: "float64[3]".to_string(),
            arrow_type: DataType::Null,
            nested: None,
            element: Some(Box::new(make_field_info("item", "float64"))),
            fixed_size: Some(3),
        }];

        let mut writer = Ros1Writer::new();
        writer.write_row(&mut source, &field_infos).unwrap();

        let bytes = writer.as_bytes();
        // No length prefix! Just 3 * 8 bytes = 24 bytes
        assert_eq!(bytes.len(), 24);

        let v1: [u8; 8] = bytes[0..8].try_into().unwrap();
        let v2: [u8; 8] = bytes[8..16].try_into().unwrap();
        let v3: [u8; 8] = bytes[16..24].try_into().unwrap();
        assert_eq!(f64::from_le_bytes(v1), 1.0);
        assert_eq!(f64::from_le_bytes(v2), 2.0);
        assert_eq!(f64::from_le_bytes(v3), 3.0);
    }

    #[test]
    fn test_write_multiple_rows() {
        let schema = Arc::new(Schema::new(vec![
            Arc::new(Field::new("x", DataType::Float64, false)),
            Arc::new(Field::new("y", DataType::Float64, false)),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Float64Array::from(vec![1.0, 3.0])),
                Arc::new(Float64Array::from(vec![2.0, 4.0])),
            ],
        )
        .unwrap();

        let mut source = ArrowRowSource::new(batch);
        let field_infos = vec![
            make_field_info("x", "float64"),
            make_field_info("y", "float64"),
        ];

        let mut writer = Ros1Writer::new();

        // Row 1
        source.next_row().unwrap();
        writer.write_row(&mut source, &field_infos).unwrap();

        let bytes1 = writer.as_bytes().to_vec();
        let x1: [u8; 8] = bytes1[0..8].try_into().unwrap();
        let y1: [u8; 8] = bytes1[8..16].try_into().unwrap();
        assert_eq!(f64::from_le_bytes(x1), 1.0);
        assert_eq!(f64::from_le_bytes(y1), 2.0);

        // Row 2
        writer.clear();
        source.next_row().unwrap();
        writer.write_row(&mut source, &field_infos).unwrap();

        let bytes2 = writer.as_bytes();
        let x2: [u8; 8] = bytes2[0..8].try_into().unwrap();
        let y2: [u8; 8] = bytes2[8..16].try_into().unwrap();
        assert_eq!(f64::from_le_bytes(x2), 3.0);
        assert_eq!(f64::from_le_bytes(y2), 4.0);
    }
}
