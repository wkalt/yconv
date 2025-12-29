//! Arrow implementation of RowSink for direct transcoding.

use std::sync::Arc;

use arrow::array::{
    ArrayBuilder, ArrayRef, BinaryBuilder, BooleanBuilder, DurationNanosecondBuilder,
    Float32Builder, Float64Builder, Int16Builder, Int32Builder, Int64Builder, Int8Builder,
    RecordBatch, StringBuilder, StructArray, TimestampNanosecondBuilder, UInt16Builder,
    UInt32Builder, UInt64Builder, UInt8Builder,
};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{DataType, Field, Fields, Schema, TimeUnit};
use thiserror::Error;

use crate::sink::RowSink;

#[derive(Debug, Error)]
pub enum SinkError {
    #[error("type mismatch: expected {expected}, got {got}")]
    TypeMismatch { expected: String, got: String },

    #[error("navigation error: {0}")]
    Navigation(String),

    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
}

/// Recursive builder that can handle nested structs and lists.
enum Builder {
    Bool(BooleanBuilder),
    Int8(Int8Builder),
    Int16(Int16Builder),
    Int32(Int32Builder),
    Int64(Int64Builder),
    UInt8(UInt8Builder),
    UInt16(UInt16Builder),
    UInt32(UInt32Builder),
    UInt64(UInt64Builder),
    Float32(Float32Builder),
    Float64(Float64Builder),
    String(StringBuilder),
    Binary(BinaryBuilder),
    TimestampNs(TimestampNanosecondBuilder),
    DurationNs(DurationNanosecondBuilder),
    List(ListState),
    FixedSizeList(FixedSizeListState),
    Struct(StructState),
}

/// State for building a variable-length list.
struct ListState {
    /// The element builder (recursively defined)
    element: Box<Builder>,
    /// Offsets for list entries
    offsets: Vec<i32>,
    /// Null buffer
    nulls: Vec<bool>,
    /// Current element count in the active list entry (reserved for future use)
    #[allow(dead_code)]
    current_len: i32,
    /// Data type of elements
    element_type: DataType,
}

/// State for building a fixed-size list.
struct FixedSizeListState {
    /// The element builder (recursively defined)
    element: Box<Builder>,
    /// Fixed size of each list
    size: i32,
    /// Null buffer (one entry per list)
    nulls: Vec<bool>,
    /// Data type of elements
    element_type: DataType,
}

/// State for building a struct.
struct StructState {
    /// Child builders, one per field
    children: Vec<Builder>,
    /// Field definitions
    fields: Fields,
    /// Null buffer
    nulls: Vec<bool>,
}

impl Builder {
    fn new(data_type: &DataType) -> Result<Self, SinkError> {
        match data_type {
            DataType::Boolean => Ok(Builder::Bool(BooleanBuilder::new())),
            DataType::Int8 => Ok(Builder::Int8(Int8Builder::new())),
            DataType::Int16 => Ok(Builder::Int16(Int16Builder::new())),
            DataType::Int32 => Ok(Builder::Int32(Int32Builder::new())),
            DataType::Int64 => Ok(Builder::Int64(Int64Builder::new())),
            DataType::UInt8 => Ok(Builder::UInt8(UInt8Builder::new())),
            DataType::UInt16 => Ok(Builder::UInt16(UInt16Builder::new())),
            DataType::UInt32 => Ok(Builder::UInt32(UInt32Builder::new())),
            DataType::UInt64 => Ok(Builder::UInt64(UInt64Builder::new())),
            DataType::Float32 => Ok(Builder::Float32(Float32Builder::new())),
            DataType::Float64 => Ok(Builder::Float64(Float64Builder::new())),
            DataType::Utf8 => Ok(Builder::String(StringBuilder::new())),
            DataType::Binary | DataType::LargeBinary => Ok(Builder::Binary(BinaryBuilder::new())),
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                Ok(Builder::TimestampNs(TimestampNanosecondBuilder::new()))
            }
            DataType::Duration(TimeUnit::Nanosecond) => {
                Ok(Builder::DurationNs(DurationNanosecondBuilder::new()))
            }
            DataType::List(field) => {
                let element = Box::new(Builder::new(field.data_type())?);
                Ok(Builder::List(ListState {
                    element,
                    offsets: vec![0],
                    nulls: Vec::new(),
                    current_len: 0,
                    element_type: field.data_type().clone(),
                }))
            }
            DataType::FixedSizeList(field, size) => {
                let element = Box::new(Builder::new(field.data_type())?);
                Ok(Builder::FixedSizeList(FixedSizeListState {
                    element,
                    size: *size,
                    nulls: Vec::new(),
                    element_type: field.data_type().clone(),
                }))
            }
            DataType::Struct(fields) => {
                let children = fields
                    .iter()
                    .map(|f| Builder::new(f.data_type()))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Builder::Struct(StructState {
                    children,
                    fields: fields.clone(),
                    nulls: Vec::new(),
                }))
            }
            other => Err(SinkError::TypeMismatch {
                expected: "supported type".into(),
                got: format!("{:?}", other),
            }),
        }
    }

    fn finish(&mut self) -> ArrayRef {
        match self {
            Builder::Bool(b) => Arc::new(b.finish()),
            Builder::Int8(b) => Arc::new(b.finish()),
            Builder::Int16(b) => Arc::new(b.finish()),
            Builder::Int32(b) => Arc::new(b.finish()),
            Builder::Int64(b) => Arc::new(b.finish()),
            Builder::UInt8(b) => Arc::new(b.finish()),
            Builder::UInt16(b) => Arc::new(b.finish()),
            Builder::UInt32(b) => Arc::new(b.finish()),
            Builder::UInt64(b) => Arc::new(b.finish()),
            Builder::Float32(b) => Arc::new(b.finish()),
            Builder::Float64(b) => Arc::new(b.finish()),
            Builder::String(b) => Arc::new(b.finish()),
            Builder::Binary(b) => Arc::new(b.finish()),
            Builder::TimestampNs(b) => Arc::new(b.finish()),
            Builder::DurationNs(b) => Arc::new(b.finish()),
            Builder::List(state) => {
                let values = state.element.finish();
                let offsets_vec = std::mem::take(&mut state.offsets);
                let offsets = arrow::buffer::OffsetBuffer::new(
                    arrow::buffer::ScalarBuffer::from(offsets_vec),
                );
                // Always take the nulls to reset for next batch
                let nulls_vec = std::mem::take(&mut state.nulls);
                let nulls = if nulls_vec.iter().all(|&n| n) {
                    None
                } else {
                    Some(NullBuffer::from(nulls_vec))
                };
                // Reset offsets for next batch (must start with 0)
                state.offsets.push(0);
                let field = Arc::new(Field::new("item", state.element_type.clone(), true));
                Arc::new(arrow::array::ListArray::new(field, offsets, values, nulls))
            }
            Builder::FixedSizeList(state) => {
                let values = state.element.finish();
                // Always take the nulls to reset for next batch
                let nulls_vec = std::mem::take(&mut state.nulls);
                let nulls = if nulls_vec.iter().all(|&n| n) {
                    None
                } else {
                    Some(NullBuffer::from(nulls_vec))
                };
                let field = Arc::new(Field::new("item", state.element_type.clone(), true));
                Arc::new(arrow::array::FixedSizeListArray::new(field, state.size, values, nulls))
            }
            Builder::Struct(state) => {
                let arrays: Vec<ArrayRef> = state.children.iter_mut().map(|c| c.finish()).collect();
                let nulls = NullBuffer::from(std::mem::take(&mut state.nulls));
                if state.fields.is_empty() {
                    Arc::new(StructArray::new_empty_fields(nulls.len(), Some(nulls)))
                } else {
                    Arc::new(StructArray::new(state.fields.clone(), arrays, Some(nulls)))
                }
            }
        }
    }
}

/// Context frame for navigating nested structures.
/// Uses raw pointers to avoid repeated path navigation.
enum Context {
    /// At top level
    TopLevel,
    /// Inside a struct - pointer to the struct's children array
    Struct {
        /// Pointer to the StructState we're inside
        struct_ptr: *mut StructState,
        /// Field index we were at before entering (for restoration)
        saved_field_idx: usize,
    },
    /// Inside a list - pointer to the element builder
    List {
        /// Pointer to the element builder
        element_ptr: *mut Builder,
        /// Field index we were at before entering (for restoration)
        saved_field_idx: usize,
    },
}

/// Arrow implementation of RowSink.
pub struct ArrowRowSink {
    schema: Arc<Schema>,
    builders: Vec<Builder>,
    /// Stack of contexts for nested navigation
    context_stack: Vec<Context>,
    /// Current field index at top level or within current struct
    current_field: usize,
    /// Row count
    row_count: usize,
    /// Cached pointer to the current container's builders (either top-level or struct children)
    /// For TopLevel: points to self.builders
    /// For Struct: points to struct_state.children
    /// For List: not used (element_ptr is used directly)
    current_builders_ptr: *mut Builder,
    current_builders_len: usize,
    /// Approximate bytes written to variable-length fields (for size-based flushing)
    bytes_written: usize,
}

impl ArrowRowSink {
    /// Create a new sink for the given Arrow schema.
    pub fn new(schema: Arc<Schema>) -> Result<Self, SinkError> {
        let mut builders = schema
            .fields()
            .iter()
            .map(|f| Builder::new(f.data_type()))
            .collect::<Result<Vec<_>, _>>()?;

        let current_builders_ptr = builders.as_mut_ptr();
        let current_builders_len = builders.len();

        Ok(Self {
            schema,
            builders,
            context_stack: vec![Context::TopLevel],
            current_field: 0,
            row_count: 0,
            current_builders_ptr,
            current_builders_len,
            bytes_written: 0,
        })
    }

    /// Finish building and return the RecordBatch.
    pub fn finish(&mut self) -> Result<RecordBatch, SinkError> {
        let arrays: Vec<ArrayRef> = self.builders.iter_mut().map(|b| b.finish()).collect();
        let row_count = self.row_count;
        self.row_count = 0;
        self.current_field = 0;
        self.bytes_written = 0;

        // Use with_match_field_names(false) to allow metadata differences between
        // schema fields and array fields. Arrow builders don't preserve field metadata,
        // but Lance will use the schema we provide for encoding decisions.
        let options = arrow::record_batch::RecordBatchOptions::new()
            .with_row_count(Some(row_count))
            .with_match_field_names(false);
        Ok(RecordBatch::try_new_with_options(
            self.schema.clone(),
            arrays,
            &options,
        )?)
    }

    /// Get approximate bytes written to variable-length fields.
    /// Used for size-based batch flushing to avoid i32 offset overflow.
    pub fn bytes_written(&self) -> usize {
        self.bytes_written
    }

    /// Get the current builder we should push to.
    /// Uses cached pointers for O(1) access instead of path navigation.
    #[inline(always)]
    fn current_builder(&mut self) -> Result<&mut Builder, SinkError> {
        match self.context_stack.last() {
            Some(Context::TopLevel) | Some(Context::Struct { .. }) => {
                // Use cached pointer to current builders array
                if self.current_field >= self.current_builders_len {
                    return Err(SinkError::Navigation(format!(
                        "field {} out of bounds (have {})",
                        self.current_field, self.current_builders_len
                    )));
                }
                // SAFETY: We maintain the invariant that current_builders_ptr points to
                // a valid array of length current_builders_len, and current_field < len
                unsafe { Ok(&mut *self.current_builders_ptr.add(self.current_field)) }
            }
            Some(Context::List { element_ptr, .. }) => {
                // SAFETY: element_ptr is set when entering a list and remains valid
                // until we exit the list
                unsafe { Ok(&mut **element_ptr) }
            }
            None => Err(SinkError::Navigation("empty context stack".into())),
        }
    }

    fn advance_field(&mut self) {
        // Only advance if we're not in a list (list elements don't advance the field counter)
        if let Some(Context::List { .. }) = self.context_stack.last() {
            // In a list - don't advance, we're pushing to the same element builder
        } else {
            self.current_field += 1;
        }
    }
}

impl RowSink for ArrowRowSink {
    type Error = SinkError;

    fn push_bool(&mut self, value: bool) -> Result<(), Self::Error> {
        let builder = self.current_builder()?;
        match builder {
            Builder::Bool(b) => b.append_value(value),
            other => {
                let got = match other {
                    Builder::Bool(_) => "bool",
                    Builder::Int8(_) => "i8",
                    Builder::UInt8(_) => "u8",
                    Builder::UInt32(_) => "u32",
                    Builder::Float64(_) => "f64",
                    Builder::String(_) => "string",
                    Builder::TimestampNs(_) => "timestamp",
                    Builder::List(_) => "list",
                    Builder::FixedSizeList(_) => "fixed_list",
                    Builder::Struct(_) => "struct",
                    _ => "other",
                };
                return Err(SinkError::TypeMismatch {
                    expected: format!("bool (field={}, depth={})", self.current_field, self.context_stack.len()),
                    got: got.into(),
                });
            }
        }
        self.advance_field();
        Ok(())
    }

    fn push_i8(&mut self, value: i8) -> Result<(), Self::Error> {
        let builder = self.current_builder()?;
        match builder {
            Builder::Int8(b) => b.append_value(value),
            _ => return Err(SinkError::TypeMismatch {
                expected: "i8".into(),
                got: "other".into(),
            }),
        }
        self.advance_field();
        Ok(())
    }

    fn push_i16(&mut self, value: i16) -> Result<(), Self::Error> {
        let builder = self.current_builder()?;
        match builder {
            Builder::Int16(b) => b.append_value(value),
            _ => return Err(SinkError::TypeMismatch {
                expected: "i16".into(),
                got: "other".into(),
            }),
        }
        self.advance_field();
        Ok(())
    }

    fn push_i32(&mut self, value: i32) -> Result<(), Self::Error> {
        let builder = self.current_builder()?;
        match builder {
            Builder::Int32(b) => b.append_value(value),
            _ => return Err(SinkError::TypeMismatch {
                expected: "i32".into(),
                got: "other".into(),
            }),
        }
        self.advance_field();
        Ok(())
    }

    fn push_i64(&mut self, value: i64) -> Result<(), Self::Error> {
        let builder = self.current_builder()?;
        match builder {
            Builder::Int64(b) => b.append_value(value),
            _ => return Err(SinkError::TypeMismatch {
                expected: "i64".into(),
                got: "other".into(),
            }),
        }
        self.advance_field();
        Ok(())
    }

    fn push_u8(&mut self, value: u8) -> Result<(), Self::Error> {
        let builder = self.current_builder()?;
        match builder {
            Builder::UInt8(b) => b.append_value(value),
            _ => return Err(SinkError::TypeMismatch {
                expected: "u8".into(),
                got: "other".into(),
            }),
        }
        self.advance_field();
        Ok(())
    }

    fn push_u16(&mut self, value: u16) -> Result<(), Self::Error> {
        let builder = self.current_builder()?;
        match builder {
            Builder::UInt16(b) => b.append_value(value),
            _ => return Err(SinkError::TypeMismatch {
                expected: "u16".into(),
                got: "other".into(),
            }),
        }
        self.advance_field();
        Ok(())
    }

    fn push_u32(&mut self, value: u32) -> Result<(), Self::Error> {
        let builder = self.current_builder()?;
        match builder {
            Builder::UInt32(b) => b.append_value(value),
            _ => return Err(SinkError::TypeMismatch {
                expected: "u32".into(),
                got: "other".into(),
            }),
        }
        self.advance_field();
        Ok(())
    }

    fn push_u64(&mut self, value: u64) -> Result<(), Self::Error> {
        let builder = self.current_builder()?;
        match builder {
            Builder::UInt64(b) => b.append_value(value),
            _ => return Err(SinkError::TypeMismatch {
                expected: "u64".into(),
                got: "other".into(),
            }),
        }
        self.advance_field();
        Ok(())
    }

    fn push_f32(&mut self, value: f32) -> Result<(), Self::Error> {
        let builder = self.current_builder()?;
        match builder {
            Builder::Float32(b) => b.append_value(value),
            _ => return Err(SinkError::TypeMismatch {
                expected: "f32".into(),
                got: "other".into(),
            }),
        }
        self.advance_field();
        Ok(())
    }

    fn push_f64(&mut self, value: f64) -> Result<(), Self::Error> {
        let builder = self.current_builder()?;
        match builder {
            Builder::Float64(b) => b.append_value(value),
            _ => return Err(SinkError::TypeMismatch {
                expected: "f64".into(),
                got: "other".into(),
            }),
        }
        self.advance_field();
        Ok(())
    }

    fn push_string(&mut self, value: &str) -> Result<(), Self::Error> {
        let builder = self.current_builder()?;
        match builder {
            Builder::String(b) => b.append_value(value),
            other => {
                let got = match other {
                    Builder::Bool(_) => "bool",
                    Builder::Int8(_) => "i8",
                    Builder::Int16(_) => "i16",
                    Builder::Int32(_) => "i32",
                    Builder::Int64(_) => "i64",
                    Builder::UInt8(_) => "u8",
                    Builder::UInt16(_) => "u16",
                    Builder::UInt32(_) => "u32",
                    Builder::UInt64(_) => "u64",
                    Builder::Float32(_) => "f32",
                    Builder::Float64(_) => "f64",
                    Builder::String(_) => "string",
                    Builder::Binary(_) => "binary",
                    Builder::TimestampNs(_) => "timestamp",
                    Builder::DurationNs(_) => "duration",
                    Builder::List(_) => "list",
                    Builder::FixedSizeList(_) => "fixed_list",
                    Builder::Struct(_) => "struct",
                };
                return Err(SinkError::TypeMismatch {
                    expected: format!("string (field={}, context_depth={})", self.current_field, self.context_stack.len()),
                    got: got.into(),
                });
            }
        }
        self.advance_field();
        Ok(())
    }

    fn push_bytes(&mut self, value: &[u8]) -> Result<(), Self::Error> {
        let builder = self.current_builder()?;
        match builder {
            Builder::Binary(b) => b.append_value(value),
            _ => return Err(SinkError::TypeMismatch {
                expected: "binary".into(),
                got: "other".into(),
            }),
        }
        self.advance_field();
        Ok(())
    }

    fn push_timestamp_nanos(&mut self, nanos: i64) -> Result<(), Self::Error> {
        let builder = self.current_builder()?;
        match builder {
            Builder::TimestampNs(b) => b.append_value(nanos),
            _ => return Err(SinkError::TypeMismatch {
                expected: "timestamp".into(),
                got: "other".into(),
            }),
        }
        self.advance_field();
        Ok(())
    }

    fn push_duration_nanos(&mut self, nanos: i64) -> Result<(), Self::Error> {
        let builder = self.current_builder()?;
        match builder {
            Builder::DurationNs(b) => b.append_value(nanos),
            _ => return Err(SinkError::TypeMismatch {
                expected: "duration".into(),
                got: "other".into(),
            }),
        }
        self.advance_field();
        Ok(())
    }

    fn push_u8_array(&mut self, values: &[u8]) -> Result<(), Self::Error> {
        // Track bytes for size-based flushing
        self.bytes_written += values.len();

        // Get the list builder
        let builder = self.current_builder()?;

        match builder {
            Builder::List(state) => {
                // Batch append to the element builder
                match state.element.as_mut() {
                    Builder::UInt8(b) => {
                        b.append_slice(values);
                    }
                    _ => return Err(SinkError::TypeMismatch {
                        expected: "list<u8>".into(),
                        got: "list<other>".into(),
                    }),
                }
                // Update offset
                let element_len = match state.element.as_ref() {
                    Builder::UInt8(b) => b.len(),
                    _ => unreachable!(),
                };
                state.offsets.push(element_len as i32);
                state.nulls.push(true);
            }
            Builder::FixedSizeList(state) => {
                // Batch append to the element builder
                match state.element.as_mut() {
                    Builder::UInt8(b) => {
                        b.append_slice(values);
                    }
                    _ => return Err(SinkError::TypeMismatch {
                        expected: "fixed_list<u8>".into(),
                        got: "fixed_list<other>".into(),
                    }),
                }
                state.nulls.push(true);
            }
            _ => return Err(SinkError::TypeMismatch {
                expected: "list".into(),
                got: "other".into(),
            }),
        }
        self.advance_field();
        Ok(())
    }

    fn push_f32_array(&mut self, values: &[f32]) -> Result<(), Self::Error> {
        let builder = self.current_builder()?;

        match builder {
            Builder::List(state) => {
                match state.element.as_mut() {
                    Builder::Float32(b) => {
                        b.append_slice(values);
                    }
                    _ => return Err(SinkError::TypeMismatch {
                        expected: "list<f32>".into(),
                        got: "list<other>".into(),
                    }),
                }
                let element_len = match state.element.as_ref() {
                    Builder::Float32(b) => b.len(),
                    _ => unreachable!(),
                };
                state.offsets.push(element_len as i32);
                state.nulls.push(true);
            }
            Builder::FixedSizeList(state) => {
                match state.element.as_mut() {
                    Builder::Float32(b) => {
                        b.append_slice(values);
                    }
                    _ => return Err(SinkError::TypeMismatch {
                        expected: "fixed_list<f32>".into(),
                        got: "fixed_list<other>".into(),
                    }),
                }
                state.nulls.push(true);
            }
            _ => return Err(SinkError::TypeMismatch {
                expected: "list".into(),
                got: "other".into(),
            }),
        }
        self.advance_field();
        Ok(())
    }

    fn push_f64_array(&mut self, values: &[f64]) -> Result<(), Self::Error> {
        let builder = self.current_builder()?;

        match builder {
            Builder::List(state) => {
                match state.element.as_mut() {
                    Builder::Float64(b) => {
                        b.append_slice(values);
                    }
                    _ => return Err(SinkError::TypeMismatch {
                        expected: "list<f64>".into(),
                        got: "list<other>".into(),
                    }),
                }
                let element_len = match state.element.as_ref() {
                    Builder::Float64(b) => b.len(),
                    _ => unreachable!(),
                };
                state.offsets.push(element_len as i32);
                state.nulls.push(true);
            }
            Builder::FixedSizeList(state) => {
                match state.element.as_mut() {
                    Builder::Float64(b) => {
                        b.append_slice(values);
                    }
                    _ => return Err(SinkError::TypeMismatch {
                        expected: "fixed_list<f64>".into(),
                        got: "fixed_list<other>".into(),
                    }),
                }
                state.nulls.push(true);
            }
            _ => return Err(SinkError::TypeMismatch {
                expected: "list".into(),
                got: "other".into(),
            }),
        }
        self.advance_field();
        Ok(())
    }

    fn enter_struct(&mut self) -> Result<(), Self::Error> {
        // Get the struct builder at current position
        let builder = self.current_builder()?;

        // Get pointer to the struct state
        let struct_ptr = match builder {
            Builder::Struct(state) => state as *mut StructState,
            _ => return Err(SinkError::Navigation("expected struct".into())),
        };

        // SAFETY: struct_ptr remains valid as long as the builder tree exists
        let struct_state = unsafe { &mut *struct_ptr };

        // Update cached pointers to point to struct's children
        self.current_builders_ptr = struct_state.children.as_mut_ptr();
        self.current_builders_len = struct_state.children.len();

        // Save current field index and push new context
        let saved_field_idx = self.current_field;
        self.context_stack.push(Context::Struct {
            struct_ptr,
            saved_field_idx,
        });
        self.current_field = 0;
        Ok(())
    }

    fn exit_struct(&mut self) -> Result<(), Self::Error> {
        let context = self.context_stack.pop().ok_or_else(|| {
            SinkError::Navigation("exit_struct with empty stack".into())
        })?;

        match context {
            Context::Struct { struct_ptr, saved_field_idx } => {
                // SAFETY: struct_ptr was valid when we entered and hasn't been invalidated
                let struct_state = unsafe { &mut *struct_ptr };
                struct_state.nulls.push(true);

                // Restore parent context's cached pointers
                match self.context_stack.last() {
                    Some(Context::TopLevel) => {
                        self.current_builders_ptr = self.builders.as_mut_ptr();
                        self.current_builders_len = self.builders.len();
                        self.current_field = saved_field_idx + 1;
                    }
                    Some(Context::Struct { struct_ptr: parent_ptr, .. }) => {
                        // SAFETY: parent struct_ptr is still valid
                        let parent_state = unsafe { &mut **parent_ptr };
                        self.current_builders_ptr = parent_state.children.as_mut_ptr();
                        self.current_builders_len = parent_state.children.len();
                        self.current_field = saved_field_idx + 1;
                    }
                    Some(Context::List { .. }) => {
                        // In a list - don't advance field, just restore
                        self.current_field = saved_field_idx;
                    }
                    None => {
                        return Err(SinkError::Navigation("empty context after exit_struct".into()));
                    }
                }
            }
            _ => {
                return Err(SinkError::Navigation("exit_struct but was not in struct".into()));
            }
        }
        Ok(())
    }

    fn enter_list(&mut self, _len: usize) -> Result<(), Self::Error> {
        // Get the list builder at current position
        let builder = self.current_builder()?;

        // Get pointer to the element builder
        let element_ptr: *mut Builder = match builder {
            Builder::List(state) => state.element.as_mut() as *mut Builder,
            Builder::FixedSizeList(state) => state.element.as_mut() as *mut Builder,
            _ => return Err(SinkError::Navigation("expected list".into())),
        };

        // Save current field index and push new context
        let saved_field_idx = self.current_field;
        self.context_stack.push(Context::List {
            element_ptr,
            saved_field_idx,
        });
        // Note: current_field is not used in list context, but we don't change cached pointers
        // because list elements are accessed directly via element_ptr
        Ok(())
    }

    fn exit_list(&mut self) -> Result<(), Self::Error> {
        let context = self.context_stack.pop().ok_or_else(|| {
            SinkError::Navigation("exit_list with empty stack".into())
        })?;

        match context {
            Context::List { element_ptr, saved_field_idx } => {
                // We need to finalize the list by updating offsets
                // First, get the list builder from the parent context
                self.current_field = saved_field_idx;

                // Restore parent context pointers first
                match self.context_stack.last() {
                    Some(Context::TopLevel) => {
                        self.current_builders_ptr = self.builders.as_mut_ptr();
                        self.current_builders_len = self.builders.len();
                    }
                    Some(Context::Struct { struct_ptr, .. }) => {
                        let struct_state = unsafe { &mut **struct_ptr };
                        self.current_builders_ptr = struct_state.children.as_mut_ptr();
                        self.current_builders_len = struct_state.children.len();
                    }
                    Some(Context::List { .. }) => {
                        // Nested list - parent is also a list, keep as is
                    }
                    None => {
                        return Err(SinkError::Navigation("empty context after exit_list".into()));
                    }
                }

                // Now get the list builder and finalize it
                let builder = self.current_builder()?;
                match builder {
                    Builder::List(state) => {
                        // SAFETY: element_ptr was pointing to state.element
                        let element = unsafe { &*element_ptr };
                        let element_len = match element {
                            Builder::Bool(b) => b.len(),
                            Builder::Int8(b) => b.len(),
                            Builder::Int16(b) => b.len(),
                            Builder::Int32(b) => b.len(),
                            Builder::Int64(b) => b.len(),
                            Builder::UInt8(b) => b.len(),
                            Builder::UInt16(b) => b.len(),
                            Builder::UInt32(b) => b.len(),
                            Builder::UInt64(b) => b.len(),
                            Builder::Float32(b) => b.len(),
                            Builder::Float64(b) => b.len(),
                            Builder::String(b) => b.len(),
                            Builder::Binary(b) => b.len(),
                            Builder::TimestampNs(b) => b.len(),
                            Builder::DurationNs(b) => b.len(),
                            Builder::Struct(s) => s.nulls.len(),
                            Builder::List(l) => l.nulls.len(),
                            Builder::FixedSizeList(l) => l.nulls.len(),
                        };
                        state.offsets.push(element_len as i32);
                        state.nulls.push(true);
                    }
                    Builder::FixedSizeList(state) => {
                        state.nulls.push(true);
                    }
                    _ => return Err(SinkError::Navigation("expected list".into())),
                }

                // Advance past the list field
                self.current_field += 1;
            }
            _ => {
                return Err(SinkError::Navigation("exit_list but was not in list".into()));
            }
        }
        Ok(())
    }

    fn finish_row(&mut self) -> Result<(), Self::Error> {
        // Should only have TopLevel context remaining
        if self.context_stack.len() != 1 {
            return Err(SinkError::Navigation(format!(
                "finish_row with {} contexts (expected 1)",
                self.context_stack.len()
            )));
        }
        if !matches!(self.context_stack.last(), Some(Context::TopLevel)) {
            return Err(SinkError::Navigation(
                "finish_row but not at top level".into(),
            ));
        }
        self.current_field = 0;
        self.row_count += 1;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Float64Array, Int32Array, StringArray, StructArray, ListArray};

    #[test]
    fn test_simple_primitives() {
        let schema = Arc::new(Schema::new(vec![
            Arc::new(Field::new("x", DataType::Float64, false)),
            Arc::new(Field::new("y", DataType::Float64, false)),
            Arc::new(Field::new("z", DataType::Float64, false)),
        ]));

        let mut sink = ArrowRowSink::new(schema).unwrap();

        // Row 1
        sink.push_f64(1.0).unwrap();
        sink.push_f64(2.0).unwrap();
        sink.push_f64(3.0).unwrap();
        sink.finish_row().unwrap();

        // Row 2
        sink.push_f64(4.0).unwrap();
        sink.push_f64(5.0).unwrap();
        sink.push_f64(6.0).unwrap();
        sink.finish_row().unwrap();

        let batch = sink.finish().unwrap();
        assert_eq!(batch.num_rows(), 2);

        let x = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(x.value(0), 1.0);
        assert_eq!(x.value(1), 4.0);
    }

    #[test]
    fn test_nested_struct() {
        let inner_fields = Fields::from(vec![
            Arc::new(Field::new("a", DataType::Int32, false)),
            Arc::new(Field::new("b", DataType::Utf8, false)),
        ]);
        let schema = Arc::new(Schema::new(vec![
            Arc::new(Field::new("nested", DataType::Struct(inner_fields), false)),
            Arc::new(Field::new("value", DataType::Float64, false)),
        ]));

        let mut sink = ArrowRowSink::new(schema).unwrap();

        // Row 1: { nested: { a: 1, b: "hello" }, value: 3.14 }
        sink.enter_struct().unwrap();
        sink.push_i32(1).unwrap();
        sink.push_string("hello").unwrap();
        sink.exit_struct().unwrap();
        sink.push_f64(3.14).unwrap();
        sink.finish_row().unwrap();

        let batch = sink.finish().unwrap();
        assert_eq!(batch.num_rows(), 1);

        let nested = batch
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let a = nested
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let b = nested
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert_eq!(a.value(0), 1);
        assert_eq!(b.value(0), "hello");
    }

    #[test]
    fn test_list_of_primitives() {
        let schema = Arc::new(Schema::new(vec![
            Arc::new(Field::new(
                "values",
                DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                false,
            )),
        ]));

        let mut sink = ArrowRowSink::new(schema).unwrap();

        // Row 1: [1, 2, 3]
        sink.enter_list(3).unwrap();
        sink.push_i32(1).unwrap();
        sink.push_i32(2).unwrap();
        sink.push_i32(3).unwrap();
        sink.exit_list().unwrap();
        sink.finish_row().unwrap();

        // Row 2: [4, 5]
        sink.enter_list(2).unwrap();
        sink.push_i32(4).unwrap();
        sink.push_i32(5).unwrap();
        sink.exit_list().unwrap();
        sink.finish_row().unwrap();

        let batch = sink.finish().unwrap();
        assert_eq!(batch.num_rows(), 2);

        let list = batch
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();

        assert_eq!(list.len(), 2);

        let values = list.values().as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(values.value(0), 1);
        assert_eq!(values.value(1), 2);
        assert_eq!(values.value(2), 3);
        assert_eq!(values.value(3), 4);
        assert_eq!(values.value(4), 5);
    }

    #[test]
    fn test_list_of_structs() {
        // Schema: { transforms: List<{ x: f64, y: f64 }> }
        let inner_fields = Fields::from(vec![
            Arc::new(Field::new("x", DataType::Float64, false)),
            Arc::new(Field::new("y", DataType::Float64, false)),
        ]);
        let schema = Arc::new(Schema::new(vec![Arc::new(Field::new(
            "transforms",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(inner_fields),
                true,
            ))),
            false,
        ))]));

        let mut sink = ArrowRowSink::new(schema).unwrap();

        // Row 1: [{ x: 1.0, y: 2.0 }, { x: 3.0, y: 4.0 }]
        sink.enter_list(2).unwrap();
        sink.enter_struct().unwrap();
        sink.push_f64(1.0).unwrap();
        sink.push_f64(2.0).unwrap();
        sink.exit_struct().unwrap();
        sink.enter_struct().unwrap();
        sink.push_f64(3.0).unwrap();
        sink.push_f64(4.0).unwrap();
        sink.exit_struct().unwrap();
        sink.exit_list().unwrap();
        sink.finish_row().unwrap();

        let batch = sink.finish().unwrap();
        assert_eq!(batch.num_rows(), 1);

        let list = batch
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(list.len(), 1);

        let structs = list.values().as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(structs.len(), 2);

        let x = structs.column(0).as_any().downcast_ref::<Float64Array>().unwrap();
        let y = structs.column(1).as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(x.value(0), 1.0);
        assert_eq!(y.value(0), 2.0);
        assert_eq!(x.value(1), 3.0);
        assert_eq!(y.value(1), 4.0);
    }

    #[test]
    fn test_bytes_written_tracking() {
        // Test that bytes_written is tracked for size-based batch flushing
        let schema = Arc::new(Schema::new(vec![Arc::new(Field::new(
            "data",
            DataType::List(Arc::new(Field::new("item", DataType::UInt8, true))),
            false,
        ))]));

        let mut sink = ArrowRowSink::new(schema).unwrap();
        assert_eq!(sink.bytes_written(), 0);

        // Push a 1000-byte array
        let data = vec![0u8; 1000];
        sink.push_u8_array(&data).unwrap();
        sink.finish_row().unwrap();
        assert_eq!(sink.bytes_written(), 1000);

        // Push another 500-byte array
        let data2 = vec![0u8; 500];
        sink.push_u8_array(&data2).unwrap();
        sink.finish_row().unwrap();
        assert_eq!(sink.bytes_written(), 1500);

        // Finish should reset bytes_written
        let _batch = sink.finish().unwrap();
        assert_eq!(sink.bytes_written(), 0);

        // New batch should start fresh
        let data3 = vec![0u8; 200];
        sink.push_u8_array(&data3).unwrap();
        sink.finish_row().unwrap();
        assert_eq!(sink.bytes_written(), 200);
    }

    #[test]
    fn test_large_list_offsets_within_i32() {
        // Test that list offsets are correctly built for multiple rows
        // This catches the i32 overflow bug where offsets were exceeding i32::MAX
        let schema = Arc::new(Schema::new(vec![Arc::new(Field::new(
            "data",
            DataType::List(Arc::new(Field::new("item", DataType::UInt8, true))),
            false,
        ))]));

        let mut sink = ArrowRowSink::new(schema).unwrap();

        // Push multiple rows with varying sizes
        // The offsets should be: [0, 100, 350, 850, 1850]
        let sizes = [100, 250, 500, 1000];
        for size in sizes {
            let data = vec![42u8; size];
            sink.push_u8_array(&data).unwrap();
            sink.finish_row().unwrap();
        }

        let batch = sink.finish().unwrap();
        assert_eq!(batch.num_rows(), 4);

        let list = batch
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();

        // Verify offsets are monotonically increasing
        let offsets = list.offsets();
        assert_eq!(offsets.len(), 5); // n+1 offsets for n rows
        assert_eq!(offsets[0], 0);
        assert_eq!(offsets[1], 100);
        assert_eq!(offsets[2], 350);
        assert_eq!(offsets[3], 850);
        assert_eq!(offsets[4], 1850);
    }
}
