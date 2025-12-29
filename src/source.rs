//! Generic row source trait for schema-driven transcoding.
//!
//! The `RowSource` trait provides a format-agnostic interface for reading
//! deserialized values. It mirrors the `RowSink` trait for symmetric
//! bidirectional format conversion.
//!
//! Source implementations (Arrow, Parquet, etc.) read from their native format,
//! and serializers (ROS1, Protobuf, etc.) call these methods in schema order.

/// A source that provides row data in schema order.
///
/// Implementations track their position internally. Values are read
/// in the order defined by the schema, with `enter_*`/`exit_*` calls
/// for nested structures.
///
/// # Example flow for reading a message like `{ header: { seq: 1, frame: "map" }, x: 1.5 }`:
/// ```ignore
/// source.next_row();             // advance to first row
/// source.enter_struct();         // header
/// let seq = source.read_u32();   // header.seq
/// let frame = source.read_string(); // header.frame
/// source.exit_struct();          // end header
/// let x = source.read_f64();     // x
/// ```
pub trait RowSource {
    type Error;

    // Primitive types
    fn read_bool(&mut self) -> Result<bool, Self::Error>;
    fn read_i8(&mut self) -> Result<i8, Self::Error>;
    fn read_i16(&mut self) -> Result<i16, Self::Error>;
    fn read_i32(&mut self) -> Result<i32, Self::Error>;
    fn read_i64(&mut self) -> Result<i64, Self::Error>;
    fn read_u8(&mut self) -> Result<u8, Self::Error>;
    fn read_u16(&mut self) -> Result<u16, Self::Error>;
    fn read_u32(&mut self) -> Result<u32, Self::Error>;
    fn read_u64(&mut self) -> Result<u64, Self::Error>;
    fn read_f32(&mut self) -> Result<f32, Self::Error>;
    fn read_f64(&mut self) -> Result<f64, Self::Error>;
    fn read_string(&mut self) -> Result<String, Self::Error>;

    /// Read raw bytes (for protobuf bytes type).
    fn read_bytes(&mut self) -> Result<Vec<u8>, Self::Error>;

    /// Read a timestamp as nanoseconds since Unix epoch.
    fn read_timestamp_nanos(&mut self) -> Result<i64, Self::Error>;

    /// Read a duration as nanoseconds.
    fn read_duration_nanos(&mut self) -> Result<i64, Self::Error>;

    // Nested structure navigation

    /// Enter a nested struct field. Subsequent reads come from struct fields.
    fn enter_struct(&mut self) -> Result<(), Self::Error>;

    /// Exit the current struct, returning to the parent context.
    fn exit_struct(&mut self) -> Result<(), Self::Error>;

    /// Enter a list/array field. Returns the length of the list.
    /// Subsequent reads come from list elements.
    fn enter_list(&mut self) -> Result<usize, Self::Error>;

    /// Exit the current list, returning to the parent context.
    fn exit_list(&mut self) -> Result<(), Self::Error>;

    /// Advance to the next row. Returns false when exhausted.
    fn next_row(&mut self) -> Result<bool, Self::Error>;

    /// Returns the number of rows remaining (if known).
    fn rows_remaining(&self) -> Option<usize>;
}
