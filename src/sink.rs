//! Generic row sink trait for schema-driven transcoding.
//!
//! The `RowSink` trait provides a format-agnostic interface for receiving
//! deserialized values. Source format transcoders (ROS1, Protobuf, etc.)
//! call these methods in schema order, and sink implementations (Arrow,
//! Parquet, etc.) handle the actual storage.

/// A sink that receives row data in schema order.
///
/// Implementations track their position internally. Values are pushed
/// in the order defined by the schema, with `enter_*`/`exit_*` calls
/// for nested structures.
///
/// # Example flow for a message like `{ header: { seq: 1, frame: "map" }, x: 1.5 }`:
/// ```ignore
/// sink.enter_struct();      // header
/// sink.push_u32(1);         // header.seq
/// sink.push_string("map");  // header.frame
/// sink.exit_struct();       // end header
/// sink.push_f64(1.5);       // x
/// sink.finish_row();
/// ```
pub trait RowSink {
    type Error;

    // Primitive types
    fn push_bool(&mut self, value: bool) -> Result<(), Self::Error>;
    fn push_i8(&mut self, value: i8) -> Result<(), Self::Error>;
    fn push_i16(&mut self, value: i16) -> Result<(), Self::Error>;
    fn push_i32(&mut self, value: i32) -> Result<(), Self::Error>;
    fn push_i64(&mut self, value: i64) -> Result<(), Self::Error>;
    fn push_u8(&mut self, value: u8) -> Result<(), Self::Error>;
    fn push_u16(&mut self, value: u16) -> Result<(), Self::Error>;
    fn push_u32(&mut self, value: u32) -> Result<(), Self::Error>;
    fn push_u64(&mut self, value: u64) -> Result<(), Self::Error>;
    fn push_f32(&mut self, value: f32) -> Result<(), Self::Error>;
    fn push_f64(&mut self, value: f64) -> Result<(), Self::Error>;
    fn push_string(&mut self, value: &str) -> Result<(), Self::Error>;

    /// Push raw bytes (for protobuf bytes type).
    fn push_bytes(&mut self, value: &[u8]) -> Result<(), Self::Error>;

    /// Push a timestamp as nanoseconds since Unix epoch.
    fn push_timestamp_nanos(&mut self, nanos: i64) -> Result<(), Self::Error>;

    /// Push a duration as nanoseconds.
    fn push_duration_nanos(&mut self, nanos: i64) -> Result<(), Self::Error>;

    // Batch operations for performance

    /// Push an entire u8 array at once. This is a performance optimization
    /// for binary data like images and point clouds.
    /// Default implementation falls back to element-by-element pushing.
    fn push_u8_array(&mut self, values: &[u8]) -> Result<(), Self::Error> {
        self.enter_list(values.len())?;
        for &v in values {
            self.push_u8(v)?;
        }
        self.exit_list()
    }

    /// Push an entire f32 array at once.
    fn push_f32_array(&mut self, values: &[f32]) -> Result<(), Self::Error> {
        self.enter_list(values.len())?;
        for &v in values {
            self.push_f32(v)?;
        }
        self.exit_list()
    }

    /// Push an entire f64 array at once.
    fn push_f64_array(&mut self, values: &[f64]) -> Result<(), Self::Error> {
        self.enter_list(values.len())?;
        for &v in values {
            self.push_f64(v)?;
        }
        self.exit_list()
    }

    // Nested structure navigation

    /// Enter a nested struct field. Subsequent pushes go to struct fields.
    fn enter_struct(&mut self) -> Result<(), Self::Error>;

    /// Exit the current struct, returning to the parent context.
    fn exit_struct(&mut self) -> Result<(), Self::Error>;

    /// Enter a list/array field with the given length.
    /// Subsequent pushes go to list elements.
    fn enter_list(&mut self, len: usize) -> Result<(), Self::Error>;

    /// Exit the current list, returning to the parent context.
    fn exit_list(&mut self) -> Result<(), Self::Error>;

    /// Complete the current row. Resets position for the next row.
    fn finish_row(&mut self) -> Result<(), Self::Error>;
}
