//! Arrow implementation of RowSource for reading from RecordBatches.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, DurationNanosecondArray, FixedSizeListArray,
    Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, LargeBinaryArray,
    LargeStringArray, ListArray, StringArray, StructArray, TimestampNanosecondArray, UInt16Array,
    UInt32Array, UInt64Array, UInt8Array,
};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use thiserror::Error;

use crate::source::RowSource;

#[derive(Debug, Error)]
pub enum SourceError {
    #[error("type mismatch: expected {expected}, got {got}")]
    TypeMismatch { expected: String, got: String },

    #[error("navigation error: {0}")]
    Navigation(String),

    #[error("null value at row {row}, field {field}")]
    NullValue { row: usize, field: usize },

    #[error("no more rows")]
    NoMoreRows,

    #[error("index out of bounds: {index} >= {len}")]
    IndexOutOfBounds { index: usize, len: usize },
}

/// Context for navigating nested structures.
enum Context {
    /// At top level, reading from batch columns.
    TopLevel {
        columns: Vec<ArrayRef>,
        field_idx: usize,
    },
    /// Inside a struct.
    Struct {
        array: StructArray,
        row: usize,
        field_idx: usize,
    },
    /// Inside a variable-length list.
    List {
        values: ArrayRef,
        element_idx: usize,
        len: usize,
    },
    /// Inside a fixed-size list.
    FixedList {
        values: ArrayRef,
        element_idx: usize,
        len: usize,
    },
}

/// Arrow implementation of RowSource.
///
/// Reads row data from an Arrow RecordBatch in schema order.
pub struct ArrowRowSource {
    batch: RecordBatch,
    current_row: isize, // -1 before first next_row() call
    context_stack: Vec<Context>,
}

impl ArrowRowSource {
    /// Create a new source from a RecordBatch.
    pub fn new(batch: RecordBatch) -> Self {
        Self {
            batch,
            current_row: -1,
            context_stack: Vec::new(),
        }
    }

    /// Create a source positioned at a specific row.
    ///
    /// This is useful when you need to read a single row from a batch,
    /// such as when processing rows from multiple batches in a priority queue.
    pub fn from_batch_row(batch: RecordBatch, row: usize) -> Self {
        let mut source = Self {
            batch,
            current_row: row as isize,
            context_stack: Vec::new(),
        };
        // Initialize the top-level context
        source.context_stack.push(Context::TopLevel {
            columns: source.batch.columns().to_vec(),
            field_idx: 0,
        });
        source
    }

    /// Create a source from specific columns at a specific row.
    ///
    /// This is useful when you need to serialize only a subset of columns
    /// (e.g., excluding metadata columns like _log_time).
    pub fn from_columns_row(columns: Vec<ArrayRef>, row: usize) -> Self {
        // Create an empty batch - we won't use it directly
        let batch = RecordBatch::new_empty(Arc::new(arrow::datatypes::Schema::empty()));
        let mut source = Self {
            batch,
            current_row: row as isize,
            context_stack: Vec::new(),
        };
        // Initialize with the provided columns
        source.context_stack.push(Context::TopLevel {
            columns,
            field_idx: 0,
        });
        source
    }

    /// Get the current array and row index for reading.
    fn current_array(&self) -> Result<(ArrayRef, usize), SourceError> {
        if self.current_row < 0 {
            return Err(SourceError::Navigation(
                "must call next_row() before reading".into(),
            ));
        }

        let row = self.current_row as usize;

        match self.context_stack.last() {
            None => Err(SourceError::Navigation(
                "no context - internal error".into(),
            )),
            Some(Context::TopLevel { columns, field_idx }) => {
                let array = columns.get(*field_idx).ok_or_else(|| {
                    SourceError::Navigation(format!(
                        "field {} out of bounds (have {})",
                        field_idx,
                        columns.len()
                    ))
                })?;
                Ok((array.clone(), row))
            }
            Some(Context::Struct {
                array,
                row: struct_row,
                field_idx,
            }) => {
                let column = array.column(*field_idx);
                Ok((column.clone(), *struct_row))
            }
            Some(Context::List {
                values,
                element_idx,
                len,
            }) => {
                if *element_idx >= *len {
                    return Err(SourceError::IndexOutOfBounds {
                        index: *element_idx,
                        len: *len,
                    });
                }
                Ok((values.clone(), *element_idx))
            }
            Some(Context::FixedList {
                values,
                element_idx,
                len,
            }) => {
                if *element_idx >= *len {
                    return Err(SourceError::IndexOutOfBounds {
                        index: *element_idx,
                        len: *len,
                    });
                }
                Ok((values.clone(), *element_idx))
            }
        }
    }

    /// Advance to the next field.
    fn advance_field(&mut self) {
        if let Some(context) = self.context_stack.last_mut() {
            match context {
                Context::TopLevel { field_idx, .. } => {
                    *field_idx += 1;
                }
                Context::Struct { field_idx, .. } => {
                    *field_idx += 1;
                }
                Context::List { element_idx, .. } => {
                    *element_idx += 1;
                }
                Context::FixedList { element_idx, .. } => {
                    *element_idx += 1;
                }
            }
        }
    }

    /// Read a primitive value using a downcast function.
    fn read_primitive<T, A, F>(&mut self, type_name: &str, extract: F) -> Result<T, SourceError>
    where
        A: Array + 'static,
        F: FnOnce(&A, usize) -> T,
    {
        let (array, row) = self.current_array()?;

        if array.is_null(row) {
            return Err(SourceError::NullValue {
                row,
                field: self
                    .context_stack
                    .last()
                    .map(|c| match c {
                        Context::TopLevel { field_idx, .. } => *field_idx,
                        Context::Struct { field_idx, .. } => *field_idx,
                        Context::List { element_idx, .. } => *element_idx,
                        Context::FixedList { element_idx, .. } => *element_idx,
                    })
                    .unwrap_or(0),
            });
        }

        let typed_array =
            array
                .as_any()
                .downcast_ref::<A>()
                .ok_or_else(|| SourceError::TypeMismatch {
                    expected: type_name.into(),
                    got: format!("{:?}", array.data_type()),
                })?;

        let value = extract(typed_array, row);
        self.advance_field();
        Ok(value)
    }
}

impl RowSource for ArrowRowSource {
    type Error = SourceError;

    fn read_bool(&mut self) -> Result<bool, Self::Error> {
        self.read_primitive("bool", |arr: &BooleanArray, row| arr.value(row))
    }

    fn read_i8(&mut self) -> Result<i8, Self::Error> {
        self.read_primitive("i8", |arr: &Int8Array, row| arr.value(row))
    }

    fn read_i16(&mut self) -> Result<i16, Self::Error> {
        self.read_primitive("i16", |arr: &Int16Array, row| arr.value(row))
    }

    fn read_i32(&mut self) -> Result<i32, Self::Error> {
        self.read_primitive("i32", |arr: &Int32Array, row| arr.value(row))
    }

    fn read_i64(&mut self) -> Result<i64, Self::Error> {
        self.read_primitive("i64", |arr: &Int64Array, row| arr.value(row))
    }

    fn read_u8(&mut self) -> Result<u8, Self::Error> {
        self.read_primitive("u8", |arr: &UInt8Array, row| arr.value(row))
    }

    fn read_u16(&mut self) -> Result<u16, Self::Error> {
        self.read_primitive("u16", |arr: &UInt16Array, row| arr.value(row))
    }

    fn read_u32(&mut self) -> Result<u32, Self::Error> {
        self.read_primitive("u32", |arr: &UInt32Array, row| arr.value(row))
    }

    fn read_u64(&mut self) -> Result<u64, Self::Error> {
        self.read_primitive("u64", |arr: &UInt64Array, row| arr.value(row))
    }

    fn read_f32(&mut self) -> Result<f32, Self::Error> {
        self.read_primitive("f32", |arr: &Float32Array, row| arr.value(row))
    }

    fn read_f64(&mut self) -> Result<f64, Self::Error> {
        self.read_primitive("f64", |arr: &Float64Array, row| arr.value(row))
    }

    fn read_string(&mut self) -> Result<String, Self::Error> {
        let (array, row) = self.current_array()?;

        if array.is_null(row) {
            return Err(SourceError::NullValue {
                row,
                field: self
                    .context_stack
                    .last()
                    .map(|c| match c {
                        Context::TopLevel { field_idx, .. } => *field_idx,
                        Context::Struct { field_idx, .. } => *field_idx,
                        Context::List { element_idx, .. } => *element_idx,
                        Context::FixedList { element_idx, .. } => *element_idx,
                    })
                    .unwrap_or(0),
            });
        }

        let value = match array.data_type() {
            DataType::Utf8 => {
                let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
                arr.value(row).to_string()
            }
            DataType::LargeUtf8 => {
                let arr = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
                arr.value(row).to_string()
            }
            dt => {
                return Err(SourceError::TypeMismatch {
                    expected: "string".into(),
                    got: format!("{:?}", dt),
                })
            }
        };

        self.advance_field();
        Ok(value)
    }

    fn read_bytes(&mut self) -> Result<Vec<u8>, Self::Error> {
        let (array, row) = self.current_array()?;

        if array.is_null(row) {
            return Err(SourceError::NullValue {
                row,
                field: self
                    .context_stack
                    .last()
                    .map(|c| match c {
                        Context::TopLevel { field_idx, .. } => *field_idx,
                        Context::Struct { field_idx, .. } => *field_idx,
                        Context::List { element_idx, .. } => *element_idx,
                        Context::FixedList { element_idx, .. } => *element_idx,
                    })
                    .unwrap_or(0),
            });
        }

        let value = match array.data_type() {
            DataType::Binary => {
                let arr = array.as_any().downcast_ref::<BinaryArray>().unwrap();
                arr.value(row).to_vec()
            }
            DataType::LargeBinary => {
                let arr = array.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
                arr.value(row).to_vec()
            }
            dt => {
                return Err(SourceError::TypeMismatch {
                    expected: "binary".into(),
                    got: format!("{:?}", dt),
                })
            }
        };

        self.advance_field();
        Ok(value)
    }

    fn read_timestamp_nanos(&mut self) -> Result<i64, Self::Error> {
        self.read_primitive("timestamp", |arr: &TimestampNanosecondArray, row| {
            arr.value(row)
        })
    }

    fn read_duration_nanos(&mut self) -> Result<i64, Self::Error> {
        self.read_primitive("duration", |arr: &DurationNanosecondArray, row| {
            arr.value(row)
        })
    }

    fn enter_struct(&mut self) -> Result<(), Self::Error> {
        let (array, row) = self.current_array()?;

        let struct_array = array
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| SourceError::TypeMismatch {
                expected: "struct".into(),
                got: format!("{:?}", array.data_type()),
            })?
            .clone();

        self.context_stack.push(Context::Struct {
            array: struct_array,
            row,
            field_idx: 0,
        });

        Ok(())
    }

    fn exit_struct(&mut self) -> Result<(), Self::Error> {
        match self.context_stack.pop() {
            Some(Context::Struct { .. }) => {
                // Advance past the struct field in the parent context
                self.advance_field();
                Ok(())
            }
            Some(_) => Err(SourceError::Navigation(
                "exit_struct but not in struct".into(),
            )),
            None => Err(SourceError::Navigation(
                "exit_struct with empty stack".into(),
            )),
        }
    }

    fn enter_list(&mut self) -> Result<usize, Self::Error> {
        let (array, row) = self.current_array()?;

        match array.data_type() {
            DataType::List(_) => {
                let list_array = array.as_any().downcast_ref::<ListArray>().unwrap();
                let values = list_array.value(row);
                let len = values.len();

                self.context_stack.push(Context::List {
                    values: Arc::new(values),
                    element_idx: 0,
                    len,
                });

                Ok(len)
            }
            DataType::LargeList(_) => {
                let list_array = array
                    .as_any()
                    .downcast_ref::<arrow::array::LargeListArray>()
                    .unwrap();
                let values = list_array.value(row);
                let len = values.len();

                self.context_stack.push(Context::List {
                    values: Arc::new(values),
                    element_idx: 0,
                    len,
                });

                Ok(len)
            }
            DataType::FixedSizeList(_, size) => {
                let list_array = array.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
                let values = list_array.value(row);
                let len = *size as usize;

                self.context_stack.push(Context::FixedList {
                    values: Arc::new(values),
                    element_idx: 0,
                    len,
                });

                Ok(len)
            }
            dt => Err(SourceError::TypeMismatch {
                expected: "list".into(),
                got: format!("{:?}", dt),
            }),
        }
    }

    fn exit_list(&mut self) -> Result<(), Self::Error> {
        match self.context_stack.pop() {
            Some(Context::List { .. }) | Some(Context::FixedList { .. }) => {
                // Advance past the list field in the parent context
                self.advance_field();
                Ok(())
            }
            Some(_) => Err(SourceError::Navigation("exit_list but not in list".into())),
            None => Err(SourceError::Navigation("exit_list with empty stack".into())),
        }
    }

    fn next_row(&mut self) -> Result<bool, Self::Error> {
        self.current_row += 1;

        if self.current_row as usize >= self.batch.num_rows() {
            return Ok(false);
        }

        // Reset to top-level context
        self.context_stack.clear();
        self.context_stack.push(Context::TopLevel {
            columns: self.batch.columns().to_vec(),
            field_idx: 0,
        });

        Ok(true)
    }

    fn rows_remaining(&self) -> Option<usize> {
        if self.current_row < 0 {
            Some(self.batch.num_rows())
        } else {
            let remaining = self.batch.num_rows() as isize - self.current_row - 1;
            Some(remaining.max(0) as usize)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{Field, Fields, Schema};
    use std::sync::Arc;

    #[test]
    fn test_simple_primitives() {
        let schema = Arc::new(Schema::new(vec![
            Arc::new(Field::new("x", DataType::Float64, false)),
            Arc::new(Field::new("y", DataType::Float64, false)),
            Arc::new(Field::new("z", DataType::Float64, false)),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Float64Array::from(vec![1.0, 4.0])),
                Arc::new(Float64Array::from(vec![2.0, 5.0])),
                Arc::new(Float64Array::from(vec![3.0, 6.0])),
            ],
        )
        .unwrap();

        let mut source = ArrowRowSource::new(batch);

        // Row 1
        assert!(source.next_row().unwrap());
        assert_eq!(source.read_f64().unwrap(), 1.0);
        assert_eq!(source.read_f64().unwrap(), 2.0);
        assert_eq!(source.read_f64().unwrap(), 3.0);

        // Row 2
        assert!(source.next_row().unwrap());
        assert_eq!(source.read_f64().unwrap(), 4.0);
        assert_eq!(source.read_f64().unwrap(), 5.0);
        assert_eq!(source.read_f64().unwrap(), 6.0);

        // No more rows
        assert!(!source.next_row().unwrap());
    }

    #[test]
    fn test_nested_struct() {
        let inner_fields = Fields::from(vec![
            Arc::new(Field::new("a", DataType::Int32, false)),
            Arc::new(Field::new("b", DataType::Utf8, false)),
        ]);

        let struct_array = StructArray::from(vec![
            (
                Arc::new(Field::new("a", DataType::Int32, false)),
                Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("b", DataType::Utf8, false)),
                Arc::new(StringArray::from(vec!["hello", "world"])) as ArrayRef,
            ),
        ]);

        let schema = Arc::new(Schema::new(vec![
            Arc::new(Field::new("nested", DataType::Struct(inner_fields), false)),
            Arc::new(Field::new("value", DataType::Float64, false)),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(struct_array),
                Arc::new(Float64Array::from(vec![3.14, 2.71])),
            ],
        )
        .unwrap();

        let mut source = ArrowRowSource::new(batch);

        // Row 1
        assert!(source.next_row().unwrap());
        source.enter_struct().unwrap();
        assert_eq!(source.read_i32().unwrap(), 1);
        assert_eq!(source.read_string().unwrap(), "hello");
        source.exit_struct().unwrap();
        assert_eq!(source.read_f64().unwrap(), 3.14);

        // Row 2
        assert!(source.next_row().unwrap());
        source.enter_struct().unwrap();
        assert_eq!(source.read_i32().unwrap(), 2);
        assert_eq!(source.read_string().unwrap(), "world");
        source.exit_struct().unwrap();
        assert_eq!(source.read_f64().unwrap(), 2.71);
    }

    #[test]
    fn test_list_of_primitives() {
        let list_array =
            arrow::array::ListArray::from_iter_primitive::<arrow::datatypes::Int32Type, _, _>(
                vec![
                    Some(vec![Some(1), Some(2), Some(3)]),
                    Some(vec![Some(4), Some(5)]),
                ],
            );

        let schema = Arc::new(Schema::new(vec![Arc::new(Field::new(
            "values",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            false,
        ))]));

        let batch = RecordBatch::try_new(schema, vec![Arc::new(list_array)]).unwrap();

        let mut source = ArrowRowSource::new(batch);

        // Row 1: [1, 2, 3]
        assert!(source.next_row().unwrap());
        let len = source.enter_list().unwrap();
        assert_eq!(len, 3);
        assert_eq!(source.read_i32().unwrap(), 1);
        assert_eq!(source.read_i32().unwrap(), 2);
        assert_eq!(source.read_i32().unwrap(), 3);
        source.exit_list().unwrap();

        // Row 2: [4, 5]
        assert!(source.next_row().unwrap());
        let len = source.enter_list().unwrap();
        assert_eq!(len, 2);
        assert_eq!(source.read_i32().unwrap(), 4);
        assert_eq!(source.read_i32().unwrap(), 5);
        source.exit_list().unwrap();
    }

    #[test]
    fn test_list_of_structs() {
        // Create a list of structs: List<{ x: f64, y: f64 }>
        let inner_fields = Fields::from(vec![
            Arc::new(Field::new("x", DataType::Float64, false)),
            Arc::new(Field::new("y", DataType::Float64, false)),
        ]);

        // Build the values array (all struct elements across all lists)
        let x_values = Float64Array::from(vec![1.0, 3.0, 5.0]);
        let y_values = Float64Array::from(vec![2.0, 4.0, 6.0]);
        let struct_values = StructArray::from(vec![
            (
                Arc::new(Field::new("x", DataType::Float64, false)),
                Arc::new(x_values) as ArrayRef,
            ),
            (
                Arc::new(Field::new("y", DataType::Float64, false)),
                Arc::new(y_values) as ArrayRef,
            ),
        ]);

        // Build the list array with offsets [0, 2, 3] -> first row has 2 elements, second has 1
        let offsets =
            arrow::buffer::OffsetBuffer::new(arrow::buffer::ScalarBuffer::from(vec![0i32, 2, 3]));
        let list_field = Arc::new(Field::new(
            "item",
            DataType::Struct(inner_fields.clone()),
            true,
        ));
        let list_array = ListArray::new(list_field, offsets, Arc::new(struct_values), None);

        let schema = Arc::new(Schema::new(vec![Arc::new(Field::new(
            "transforms",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(inner_fields),
                true,
            ))),
            false,
        ))]));

        let batch = RecordBatch::try_new(schema, vec![Arc::new(list_array)]).unwrap();

        let mut source = ArrowRowSource::new(batch);

        // Row 1: [{ x: 1.0, y: 2.0 }, { x: 3.0, y: 4.0 }]
        assert!(source.next_row().unwrap());
        let len = source.enter_list().unwrap();
        assert_eq!(len, 2);

        source.enter_struct().unwrap();
        assert_eq!(source.read_f64().unwrap(), 1.0);
        assert_eq!(source.read_f64().unwrap(), 2.0);
        source.exit_struct().unwrap();

        source.enter_struct().unwrap();
        assert_eq!(source.read_f64().unwrap(), 3.0);
        assert_eq!(source.read_f64().unwrap(), 4.0);
        source.exit_struct().unwrap();

        source.exit_list().unwrap();

        // Row 2: [{ x: 5.0, y: 6.0 }]
        assert!(source.next_row().unwrap());
        let len = source.enter_list().unwrap();
        assert_eq!(len, 1);

        source.enter_struct().unwrap();
        assert_eq!(source.read_f64().unwrap(), 5.0);
        assert_eq!(source.read_f64().unwrap(), 6.0);
        source.exit_struct().unwrap();

        source.exit_list().unwrap();
    }

    #[test]
    fn test_rows_remaining() {
        let schema = Arc::new(Schema::new(vec![Arc::new(Field::new(
            "x",
            DataType::Int32,
            false,
        ))]));

        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3]))]).unwrap();

        let mut source = ArrowRowSource::new(batch);

        assert_eq!(source.rows_remaining(), Some(3));

        source.next_row().unwrap();
        assert_eq!(source.rows_remaining(), Some(2));

        source.next_row().unwrap();
        assert_eq!(source.rows_remaining(), Some(1));

        source.next_row().unwrap();
        assert_eq!(source.rows_remaining(), Some(0));

        source.next_row().unwrap(); // returns false
        assert_eq!(source.rows_remaining(), Some(0));
    }
}
