use std::sync::Arc;

use arrow::array::{
    ArrayBuilder, ArrayRef, BooleanBuilder, DurationNanosecondBuilder, FixedSizeListBuilder,
    Float32Builder, Float64Builder, Int16Builder, Int32Builder, Int64Builder, Int8Builder,
    ListBuilder, StringBuilder, StructBuilder, TimestampNanosecondBuilder, UInt16Builder,
    UInt32Builder, UInt64Builder, UInt8Builder,
};
use arrow::datatypes::{DataType, Fields, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use thiserror::Error;

use crate::ros1::Value;

/// Errors that can occur during Arrow building.
#[derive(Debug, Error)]
pub enum BuilderError {
    #[error("schema mismatch: expected {expected}, got {got}")]
    SchemaMismatch { expected: String, got: String },

    #[error("missing field: {0}")]
    MissingField(String),

    #[error("unsupported data type: {0}")]
    UnsupportedType(DataType),

    #[error("arrow error: {0}")]
    ArrowError(#[from] arrow::error::ArrowError),

    #[error("expected message value, got {0}")]
    ExpectedMessage(String),
}

/// A dynamic builder that can append `Value`s to Arrow arrays.
enum DynBuilder {
    Boolean(BooleanBuilder),
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
    TimestampNs(TimestampNanosecondBuilder),
    DurationNs(DurationNanosecondBuilder),
    List {
        builder: ListBuilder<Box<dyn ArrayBuilder>>,
        element_type: DataType,
    },
    FixedSizeList {
        builder: FixedSizeListBuilder<Box<dyn ArrayBuilder>>,
        element_type: DataType,
    },
    Struct {
        field_builders: Vec<DynBuilder>,
        field_names: Vec<String>,
        fields: Fields,
        null_buffer: Vec<bool>,
    },
}

impl DynBuilder {
    /// Create a builder for the given data type.
    fn new(data_type: &DataType) -> Result<Self, BuilderError> {
        match data_type {
            DataType::Boolean => Ok(DynBuilder::Boolean(BooleanBuilder::new())),
            DataType::Int8 => Ok(DynBuilder::Int8(Int8Builder::new())),
            DataType::Int16 => Ok(DynBuilder::Int16(Int16Builder::new())),
            DataType::Int32 => Ok(DynBuilder::Int32(Int32Builder::new())),
            DataType::Int64 => Ok(DynBuilder::Int64(Int64Builder::new())),
            DataType::UInt8 => Ok(DynBuilder::UInt8(UInt8Builder::new())),
            DataType::UInt16 => Ok(DynBuilder::UInt16(UInt16Builder::new())),
            DataType::UInt32 => Ok(DynBuilder::UInt32(UInt32Builder::new())),
            DataType::UInt64 => Ok(DynBuilder::UInt64(UInt64Builder::new())),
            DataType::Float32 => Ok(DynBuilder::Float32(Float32Builder::new())),
            DataType::Float64 => Ok(DynBuilder::Float64(Float64Builder::new())),
            DataType::Utf8 => Ok(DynBuilder::String(StringBuilder::new())),
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                Ok(DynBuilder::TimestampNs(TimestampNanosecondBuilder::new()))
            }
            DataType::Duration(TimeUnit::Nanosecond) => {
                Ok(DynBuilder::DurationNs(DurationNanosecondBuilder::new()))
            }
            DataType::List(field) => {
                let inner = make_builder(field.data_type())?;
                Ok(DynBuilder::List {
                    builder: ListBuilder::new(inner),
                    element_type: field.data_type().clone(),
                })
            }
            DataType::FixedSizeList(field, size) => {
                let inner = make_builder(field.data_type())?;
                Ok(DynBuilder::FixedSizeList {
                    builder: FixedSizeListBuilder::new(inner, *size),
                    element_type: field.data_type().clone(),
                })
            }
            DataType::Struct(fields) => {
                let field_names: Vec<String> =
                    fields.iter().map(|f| f.name().clone()).collect();
                let field_builders: Vec<DynBuilder> = fields
                    .iter()
                    .map(|f| DynBuilder::new(f.data_type()))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(DynBuilder::Struct {
                    field_builders,
                    field_names,
                    fields: fields.clone(),
                    null_buffer: Vec::new(),
                })
            }
            other => Err(BuilderError::UnsupportedType(other.clone())),
        }
    }

    /// Append a value to this builder.
    fn append(&mut self, value: &Value) -> Result<(), BuilderError> {
        match (self, value) {
            (DynBuilder::Boolean(b), Value::Bool(v)) => {
                b.append_value(*v);
            }
            (DynBuilder::Int8(b), Value::Int8(v)) => {
                b.append_value(*v);
            }
            (DynBuilder::Int16(b), Value::Int16(v)) => {
                b.append_value(*v);
            }
            (DynBuilder::Int32(b), Value::Int32(v)) => {
                b.append_value(*v);
            }
            (DynBuilder::Int64(b), Value::Int64(v)) => {
                b.append_value(*v);
            }
            (DynBuilder::UInt8(b), Value::UInt8(v)) => {
                b.append_value(*v);
            }
            (DynBuilder::UInt16(b), Value::UInt16(v)) => {
                b.append_value(*v);
            }
            (DynBuilder::UInt32(b), Value::UInt32(v)) => {
                b.append_value(*v);
            }
            (DynBuilder::UInt64(b), Value::UInt64(v)) => {
                b.append_value(*v);
            }
            (DynBuilder::Float32(b), Value::Float32(v)) => {
                b.append_value(*v);
            }
            (DynBuilder::Float64(b), Value::Float64(v)) => {
                b.append_value(*v);
            }
            (DynBuilder::String(b), Value::String(v)) => {
                b.append_value(v);
            }
            (DynBuilder::TimestampNs(b), Value::Time { sec, nsec }) => {
                let nanos = (*sec as i64) * 1_000_000_000 + (*nsec as i64);
                b.append_value(nanos);
            }
            (DynBuilder::DurationNs(b), Value::Duration { sec, nsec }) => {
                let nanos = (*sec as i64) * 1_000_000_000 + (*nsec as i64);
                b.append_value(nanos);
            }
            (
                DynBuilder::List {
                    builder,
                    element_type,
                },
                Value::Array(values),
            ) => {
                let inner = builder.values();
                for v in values {
                    append_to_builder(inner, v, element_type)?;
                }
                builder.append(true);
            }
            (
                DynBuilder::FixedSizeList {
                    builder,
                    element_type,
                },
                Value::Array(values),
            ) => {
                let inner = builder.values();
                for v in values {
                    append_to_builder(inner, v, element_type)?;
                }
                builder.append(true);
            }
            (
                DynBuilder::Struct {
                    field_builders,
                    field_names,
                    null_buffer,
                    ..
                },
                Value::Message(msg_fields),
            ) => {
                for (i, field_name) in field_names.iter().enumerate() {
                    if let Some(value) = msg_fields.get(field_name) {
                        field_builders[i].append(value)?;
                    } else {
                        return Err(BuilderError::MissingField(field_name.clone()));
                    }
                }
                null_buffer.push(true);
            }
            (builder, value) => {
                return Err(BuilderError::SchemaMismatch {
                    expected: builder_type_name(builder),
                    got: value_type_name(value),
                });
            }
        }
        Ok(())
    }

    /// Append a null value to this builder.
    fn append_null(&mut self) {
        match self {
            DynBuilder::Boolean(b) => b.append_null(),
            DynBuilder::Int8(b) => b.append_null(),
            DynBuilder::Int16(b) => b.append_null(),
            DynBuilder::Int32(b) => b.append_null(),
            DynBuilder::Int64(b) => b.append_null(),
            DynBuilder::UInt8(b) => b.append_null(),
            DynBuilder::UInt16(b) => b.append_null(),
            DynBuilder::UInt32(b) => b.append_null(),
            DynBuilder::UInt64(b) => b.append_null(),
            DynBuilder::Float32(b) => b.append_null(),
            DynBuilder::Float64(b) => b.append_null(),
            DynBuilder::String(b) => b.append_null(),
            DynBuilder::TimestampNs(b) => b.append_null(),
            DynBuilder::DurationNs(b) => b.append_null(),
            DynBuilder::List { builder, .. } => builder.append_null(),
            DynBuilder::FixedSizeList { builder, .. } => builder.append(false),
            DynBuilder::Struct {
                field_builders,
                null_buffer,
                ..
            } => {
                for fb in field_builders.iter_mut() {
                    fb.append_null();
                }
                null_buffer.push(false);
            }
        }
    }

    /// Finish building and return the array.
    fn finish(&mut self) -> ArrayRef {
        match self {
            DynBuilder::Boolean(b) => Arc::new(b.finish()),
            DynBuilder::Int8(b) => Arc::new(b.finish()),
            DynBuilder::Int16(b) => Arc::new(b.finish()),
            DynBuilder::Int32(b) => Arc::new(b.finish()),
            DynBuilder::Int64(b) => Arc::new(b.finish()),
            DynBuilder::UInt8(b) => Arc::new(b.finish()),
            DynBuilder::UInt16(b) => Arc::new(b.finish()),
            DynBuilder::UInt32(b) => Arc::new(b.finish()),
            DynBuilder::UInt64(b) => Arc::new(b.finish()),
            DynBuilder::Float32(b) => Arc::new(b.finish()),
            DynBuilder::Float64(b) => Arc::new(b.finish()),
            DynBuilder::String(b) => Arc::new(b.finish()),
            DynBuilder::TimestampNs(b) => Arc::new(b.finish()),
            DynBuilder::DurationNs(b) => Arc::new(b.finish()),
            DynBuilder::List { builder, .. } => Arc::new(builder.finish()),
            DynBuilder::FixedSizeList { builder, .. } => Arc::new(builder.finish()),
            DynBuilder::Struct {
                field_builders,
                fields,
                null_buffer,
                ..
            } => {
                use arrow::array::StructArray;
                use arrow::buffer::NullBuffer;
                let len = null_buffer.len();
                let arrays: Vec<ArrayRef> = field_builders.iter_mut().map(|fb| fb.finish()).collect();
                let null_buf = NullBuffer::from(std::mem::take(null_buffer));
                // Handle 0-field structs (e.g., empty ROS messages) by specifying length explicitly
                if fields.is_empty() {
                    Arc::new(StructArray::new_empty_fields(len, Some(null_buf)))
                } else {
                    Arc::new(StructArray::new(fields.clone(), arrays, Some(null_buf)))
                }
            }
        }
    }

    /// Get the number of items in the builder.
    fn len(&self) -> usize {
        match self {
            DynBuilder::Boolean(b) => b.len(),
            DynBuilder::Int8(b) => b.len(),
            DynBuilder::Int16(b) => b.len(),
            DynBuilder::Int32(b) => b.len(),
            DynBuilder::Int64(b) => b.len(),
            DynBuilder::UInt8(b) => b.len(),
            DynBuilder::UInt16(b) => b.len(),
            DynBuilder::UInt32(b) => b.len(),
            DynBuilder::UInt64(b) => b.len(),
            DynBuilder::Float32(b) => b.len(),
            DynBuilder::Float64(b) => b.len(),
            DynBuilder::String(b) => b.len(),
            DynBuilder::TimestampNs(b) => b.len(),
            DynBuilder::DurationNs(b) => b.len(),
            DynBuilder::List { builder, .. } => builder.len(),
            DynBuilder::FixedSizeList { builder, .. } => builder.len(),
            DynBuilder::Struct { null_buffer, .. } => null_buffer.len(),
        }
    }
}

/// Create a boxed ArrayBuilder for the given data type.
fn make_builder(data_type: &DataType) -> Result<Box<dyn ArrayBuilder>, BuilderError> {
    match data_type {
        DataType::Boolean => Ok(Box::new(BooleanBuilder::new())),
        DataType::Int8 => Ok(Box::new(Int8Builder::new())),
        DataType::Int16 => Ok(Box::new(Int16Builder::new())),
        DataType::Int32 => Ok(Box::new(Int32Builder::new())),
        DataType::Int64 => Ok(Box::new(Int64Builder::new())),
        DataType::UInt8 => Ok(Box::new(UInt8Builder::new())),
        DataType::UInt16 => Ok(Box::new(UInt16Builder::new())),
        DataType::UInt32 => Ok(Box::new(UInt32Builder::new())),
        DataType::UInt64 => Ok(Box::new(UInt64Builder::new())),
        DataType::Float32 => Ok(Box::new(Float32Builder::new())),
        DataType::Float64 => Ok(Box::new(Float64Builder::new())),
        DataType::Utf8 => Ok(Box::new(StringBuilder::new())),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            Ok(Box::new(TimestampNanosecondBuilder::new()))
        }
        DataType::Duration(TimeUnit::Nanosecond) => {
            Ok(Box::new(DurationNanosecondBuilder::new()))
        }
        DataType::List(field) => {
            let inner = make_builder(field.data_type())?;
            Ok(Box::new(ListBuilder::new(inner)))
        }
        DataType::FixedSizeList(field, size) => {
            let inner = make_builder(field.data_type())?;
            Ok(Box::new(FixedSizeListBuilder::new(inner, *size)))
        }
        DataType::Struct(fields) => {
            let builders: Vec<Box<dyn ArrayBuilder>> = fields
                .iter()
                .map(|f| make_builder(f.data_type()))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Box::new(StructBuilder::new(fields.clone(), builders)))
        }
        other => Err(BuilderError::UnsupportedType(other.clone())),
    }
}

/// Append a value to a boxed ArrayBuilder.
/// The data_type parameter is needed to get field names for nested structs.
fn append_to_builder(
    builder: &mut dyn ArrayBuilder,
    value: &Value,
    data_type: &DataType,
) -> Result<(), BuilderError> {
    // We need to downcast the builder to the appropriate type
    if let Some(b) = builder.as_any_mut().downcast_mut::<BooleanBuilder>() {
        if let Value::Bool(v) = value {
            b.append_value(*v);
            return Ok(());
        }
    } else if let Some(b) = builder.as_any_mut().downcast_mut::<Int8Builder>() {
        if let Value::Int8(v) = value {
            b.append_value(*v);
            return Ok(());
        }
    } else if let Some(b) = builder.as_any_mut().downcast_mut::<Int16Builder>() {
        if let Value::Int16(v) = value {
            b.append_value(*v);
            return Ok(());
        }
    } else if let Some(b) = builder.as_any_mut().downcast_mut::<Int32Builder>() {
        if let Value::Int32(v) = value {
            b.append_value(*v);
            return Ok(());
        }
    } else if let Some(b) = builder.as_any_mut().downcast_mut::<Int64Builder>() {
        if let Value::Int64(v) = value {
            b.append_value(*v);
            return Ok(());
        }
    } else if let Some(b) = builder.as_any_mut().downcast_mut::<UInt8Builder>() {
        if let Value::UInt8(v) = value {
            b.append_value(*v);
            return Ok(());
        }
    } else if let Some(b) = builder.as_any_mut().downcast_mut::<UInt16Builder>() {
        if let Value::UInt16(v) = value {
            b.append_value(*v);
            return Ok(());
        }
    } else if let Some(b) = builder.as_any_mut().downcast_mut::<UInt32Builder>() {
        if let Value::UInt32(v) = value {
            b.append_value(*v);
            return Ok(());
        }
    } else if let Some(b) = builder.as_any_mut().downcast_mut::<UInt64Builder>() {
        if let Value::UInt64(v) = value {
            b.append_value(*v);
            return Ok(());
        }
    } else if let Some(b) = builder.as_any_mut().downcast_mut::<Float32Builder>() {
        if let Value::Float32(v) = value {
            b.append_value(*v);
            return Ok(());
        }
    } else if let Some(b) = builder.as_any_mut().downcast_mut::<Float64Builder>() {
        if let Value::Float64(v) = value {
            b.append_value(*v);
            return Ok(());
        }
    } else if let Some(b) = builder.as_any_mut().downcast_mut::<StringBuilder>() {
        if let Value::String(v) = value {
            b.append_value(v);
            return Ok(());
        }
    } else if let Some(b) = builder
        .as_any_mut()
        .downcast_mut::<TimestampNanosecondBuilder>()
    {
        if let Value::Time { sec, nsec } = value {
            let nanos = (*sec as i64) * 1_000_000_000 + (*nsec as i64);
            b.append_value(nanos);
            return Ok(());
        }
    } else if let Some(b) = builder
        .as_any_mut()
        .downcast_mut::<DurationNanosecondBuilder>()
    {
        if let Value::Duration { sec, nsec } = value {
            let nanos = (*sec as i64) * 1_000_000_000 + (*nsec as i64);
            b.append_value(nanos);
            return Ok(());
        }
    } else if let Some(b) = builder
        .as_any_mut()
        .downcast_mut::<ListBuilder<Box<dyn ArrayBuilder>>>()
    {
        if let Value::Array(values) = value {
            if let DataType::List(inner_field) = data_type {
                let inner = b.values();
                for v in values {
                    append_to_builder(inner, v, inner_field.data_type())?;
                }
                b.append(true);
                return Ok(());
            }
        }
    } else if let Some(b) = builder
        .as_any_mut()
        .downcast_mut::<FixedSizeListBuilder<Box<dyn ArrayBuilder>>>()
    {
        if let Value::Array(values) = value {
            if let DataType::FixedSizeList(inner_field, _) = data_type {
                let inner = b.values();
                for v in values {
                    append_to_builder(inner, v, inner_field.data_type())?;
                }
                b.append(true);
                return Ok(());
            }
        }
    }
    // For struct types inside lists, we need special handling because we can't use
    // Arrow's StructBuilder dynamically. Instead we create a temporary DynBuilder.
    if let DataType::Struct(struct_fields) = data_type {
        if let Value::Message(msg_fields) = value {
            // We need to access the StructBuilder's internal builders
            // Since we can't easily, we'll use a workaround: create arrays for each field
            // and build a struct from them. But this is complex for nested cases.
            // For now, let's try using the boxed builder approach.
            if let Some(b) = builder.as_any_mut().downcast_mut::<StructBuilder>() {
                // We need to manually append to each field builder
                for (i, field) in struct_fields.iter().enumerate() {
                    let field_name = field.name();
                    if let Some(field_value) = msg_fields.get(field_name) {
                        // Try to get and append to the field builder
                        // This is tricky because we need to know the concrete type
                        append_to_struct_field(b, i, field_value, field.data_type())?;
                    } else {
                        return Err(BuilderError::MissingField(field_name.clone()));
                    }
                }
                b.append(true);
                return Ok(());
            }
        }
    }

    Err(BuilderError::SchemaMismatch {
        expected: "matching builder".to_string(),
        got: value_type_name(value),
    })
}

/// Append a value to a specific field in a StructBuilder.
/// This uses type-specific dispatch to access the field builder.
fn append_to_struct_field(
    builder: &mut StructBuilder,
    index: usize,
    value: &Value,
    data_type: &DataType,
) -> Result<(), BuilderError> {
    match (data_type, value) {
        (DataType::Boolean, Value::Bool(v)) => {
            if let Some(b) = builder.field_builder::<BooleanBuilder>(index) {
                b.append_value(*v);
                return Ok(());
            }
        }
        (DataType::Int8, Value::Int8(v)) => {
            if let Some(b) = builder.field_builder::<Int8Builder>(index) {
                b.append_value(*v);
                return Ok(());
            }
        }
        (DataType::Int16, Value::Int16(v)) => {
            if let Some(b) = builder.field_builder::<Int16Builder>(index) {
                b.append_value(*v);
                return Ok(());
            }
        }
        (DataType::Int32, Value::Int32(v)) => {
            if let Some(b) = builder.field_builder::<Int32Builder>(index) {
                b.append_value(*v);
                return Ok(());
            }
        }
        (DataType::Int64, Value::Int64(v)) => {
            if let Some(b) = builder.field_builder::<Int64Builder>(index) {
                b.append_value(*v);
                return Ok(());
            }
        }
        (DataType::UInt8, Value::UInt8(v)) => {
            if let Some(b) = builder.field_builder::<UInt8Builder>(index) {
                b.append_value(*v);
                return Ok(());
            }
        }
        (DataType::UInt16, Value::UInt16(v)) => {
            if let Some(b) = builder.field_builder::<UInt16Builder>(index) {
                b.append_value(*v);
                return Ok(());
            }
        }
        (DataType::UInt32, Value::UInt32(v)) => {
            if let Some(b) = builder.field_builder::<UInt32Builder>(index) {
                b.append_value(*v);
                return Ok(());
            }
        }
        (DataType::UInt64, Value::UInt64(v)) => {
            if let Some(b) = builder.field_builder::<UInt64Builder>(index) {
                b.append_value(*v);
                return Ok(());
            }
        }
        (DataType::Float32, Value::Float32(v)) => {
            if let Some(b) = builder.field_builder::<Float32Builder>(index) {
                b.append_value(*v);
                return Ok(());
            }
        }
        (DataType::Float64, Value::Float64(v)) => {
            if let Some(b) = builder.field_builder::<Float64Builder>(index) {
                b.append_value(*v);
                return Ok(());
            }
        }
        (DataType::Utf8, Value::String(v)) => {
            if let Some(b) = builder.field_builder::<StringBuilder>(index) {
                b.append_value(v);
                return Ok(());
            }
        }
        (DataType::Timestamp(TimeUnit::Nanosecond, _), Value::Time { sec, nsec }) => {
            if let Some(b) = builder.field_builder::<TimestampNanosecondBuilder>(index) {
                let nanos = (*sec as i64) * 1_000_000_000 + (*nsec as i64);
                b.append_value(nanos);
                return Ok(());
            }
        }
        (DataType::Duration(TimeUnit::Nanosecond), Value::Duration { sec, nsec }) => {
            if let Some(b) = builder.field_builder::<DurationNanosecondBuilder>(index) {
                let nanos = (*sec as i64) * 1_000_000_000 + (*nsec as i64);
                b.append_value(nanos);
                return Ok(());
            }
        }
        (DataType::List(inner_field), Value::Array(values)) => {
            if let Some(b) =
                builder.field_builder::<ListBuilder<Box<dyn ArrayBuilder>>>(index)
            {
                let inner = b.values();
                for v in values {
                    append_to_builder(inner, v, inner_field.data_type())?;
                }
                b.append(true);
                return Ok(());
            }
        }
        (DataType::FixedSizeList(inner_field, _), Value::Array(values)) => {
            if let Some(b) =
                builder.field_builder::<FixedSizeListBuilder<Box<dyn ArrayBuilder>>>(index)
            {
                let inner = b.values();
                for v in values {
                    append_to_builder(inner, v, inner_field.data_type())?;
                }
                b.append(true);
                return Ok(());
            }
        }
        (DataType::Struct(struct_fields), Value::Message(msg_fields)) => {
            if let Some(b) = builder.field_builder::<StructBuilder>(index) {
                for (i, field) in struct_fields.iter().enumerate() {
                    let field_name = field.name();
                    if let Some(field_value) = msg_fields.get(field_name) {
                        append_to_struct_field(b, i, field_value, field.data_type())?;
                    } else {
                        return Err(BuilderError::MissingField(field_name.clone()));
                    }
                }
                b.append(true);
                return Ok(());
            }
        }
        _ => {}
    }

    Err(BuilderError::SchemaMismatch {
        expected: format!("{:?}", data_type),
        got: value_type_name(value),
    })
}

fn builder_type_name(builder: &DynBuilder) -> String {
    match builder {
        DynBuilder::Boolean(_) => "Boolean".to_string(),
        DynBuilder::Int8(_) => "Int8".to_string(),
        DynBuilder::Int16(_) => "Int16".to_string(),
        DynBuilder::Int32(_) => "Int32".to_string(),
        DynBuilder::Int64(_) => "Int64".to_string(),
        DynBuilder::UInt8(_) => "UInt8".to_string(),
        DynBuilder::UInt16(_) => "UInt16".to_string(),
        DynBuilder::UInt32(_) => "UInt32".to_string(),
        DynBuilder::UInt64(_) => "UInt64".to_string(),
        DynBuilder::Float32(_) => "Float32".to_string(),
        DynBuilder::Float64(_) => "Float64".to_string(),
        DynBuilder::String(_) => "String".to_string(),
        DynBuilder::TimestampNs(_) => "Timestamp".to_string(),
        DynBuilder::DurationNs(_) => "Duration".to_string(),
        DynBuilder::List { .. } => "List".to_string(),
        DynBuilder::FixedSizeList { .. } => "FixedSizeList".to_string(),
        DynBuilder::Struct { .. } => "Struct".to_string(),
    }
}

fn value_type_name(value: &Value) -> String {
    match value {
        Value::Bool(_) => "Bool".to_string(),
        Value::Int8(_) => "Int8".to_string(),
        Value::Int16(_) => "Int16".to_string(),
        Value::Int32(_) => "Int32".to_string(),
        Value::Int64(_) => "Int64".to_string(),
        Value::UInt8(_) => "UInt8".to_string(),
        Value::UInt16(_) => "UInt16".to_string(),
        Value::UInt32(_) => "UInt32".to_string(),
        Value::UInt64(_) => "UInt64".to_string(),
        Value::Float32(_) => "Float32".to_string(),
        Value::Float64(_) => "Float64".to_string(),
        Value::String(_) => "String".to_string(),
        Value::Time { .. } => "Time".to_string(),
        Value::Duration { .. } => "Duration".to_string(),
        Value::Array(_) => "Array".to_string(),
        Value::Message(_) => "Message".to_string(),
    }
}

/// Builder for constructing Arrow RecordBatches from ROS1 Values.
pub struct RecordBatchBuilder {
    schema: Arc<Schema>,
    field_names: Vec<String>,
    builders: Vec<DynBuilder>,
    /// Track row count for schemas with 0 fields (e.g., std_msgs/Empty)
    row_count: usize,
}

impl RecordBatchBuilder {
    /// Create a new builder for the given schema.
    pub fn new(schema: Arc<Schema>) -> Result<Self, BuilderError> {
        let field_names: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();

        let builders: Vec<DynBuilder> = schema
            .fields()
            .iter()
            .map(|f| DynBuilder::new(f.data_type()))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            schema,
            field_names,
            builders,
            row_count: 0,
        })
    }

    /// Append a message value to the builder.
    ///
    /// The value must be a `Value::Message` with fields matching the schema.
    pub fn append(&mut self, value: &Value) -> Result<(), BuilderError> {
        let fields = match value {
            Value::Message(f) => f,
            other => {
                return Err(BuilderError::ExpectedMessage(value_type_name(other)));
            }
        };

        for (i, name) in self.field_names.iter().enumerate() {
            if let Some(field_value) = fields.get(name) {
                self.builders[i].append(field_value)?;
            } else {
                // Field not present - append null
                self.builders[i].append_null();
            }
        }

        self.row_count += 1;
        Ok(())
    }

    /// Get the number of rows currently in the builder.
    pub fn len(&self) -> usize {
        // Use row_count for 0-column schemas (e.g., std_msgs/Empty)
        self.builders.first().map(|b| b.len()).unwrap_or(self.row_count)
    }

    /// Check if the builder is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Finish building and return the RecordBatch.
    ///
    /// After calling this, the builder is reset and can be reused.
    /// Arrow builders reset themselves after finish(), so we don't need to recreate them.
    pub fn finish(&mut self) -> Result<RecordBatch, BuilderError> {
        let arrays: Vec<ArrayRef> = self.builders.iter_mut().map(|b| b.finish()).collect();
        let row_count = self.row_count;

        // Arrow builders reset themselves after finish(), so just reset our row count
        self.row_count = 0;

        // Handle 0-column schemas (e.g., std_msgs/Empty) by specifying row count explicitly
        if arrays.is_empty() {
            let options = arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(row_count));
            Ok(RecordBatch::try_new_with_options(self.schema.clone(), arrays, &options)?)
        } else {
            Ok(RecordBatch::try_new(self.schema.clone(), arrays)?)
        }
    }

    /// Get the schema this builder is using.
    pub fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }
}

/// Convert a batch of Values to a RecordBatch.
///
/// This is a convenience function for one-shot conversion.
pub fn values_to_record_batch(
    schema: Arc<Schema>,
    values: &[Value],
) -> Result<RecordBatch, BuilderError> {
    let mut builder = RecordBatchBuilder::new(schema)?;
    for value in values {
        builder.append(value)?;
    }
    builder.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        Array, BooleanArray, DurationNanosecondArray, FixedSizeListArray, Float32Array,
        Float64Array, Int32Array, ListArray, StringArray, StructArray, TimestampNanosecondArray,
        UInt32Array, UInt8Array,
    };
    use arrow::datatypes::Field;
    use std::collections::HashMap;

    fn make_schema(fields: Vec<(&str, DataType)>) -> Arc<Schema> {
        Arc::new(Schema::new(
            fields
                .into_iter()
                .map(|(name, dt)| Field::new(name, dt, true))
                .collect::<Vec<_>>(),
        ))
    }

    fn make_message(fields: Vec<(&str, Value)>) -> Value {
        Value::Message(fields.into_iter().map(|(k, v)| (k.to_string(), v)).collect())
    }

    mod primitive_building {
        use super::*;

        #[test]
        fn test_boolean() {
            let schema = make_schema(vec![("flag", DataType::Boolean)]);
            let values = vec![
                make_message(vec![("flag", Value::Bool(true))]),
                make_message(vec![("flag", Value::Bool(false))]),
                make_message(vec![("flag", Value::Bool(true))]),
            ];

            let batch = values_to_record_batch(schema, &values).unwrap();

            assert_eq!(batch.num_rows(), 3);
            let col = batch.column(0).as_any().downcast_ref::<BooleanArray>().unwrap();
            assert_eq!(col.value(0), true);
            assert_eq!(col.value(1), false);
            assert_eq!(col.value(2), true);
        }

        #[test]
        fn test_integers() {
            let schema = make_schema(vec![
                ("i32", DataType::Int32),
                ("u32", DataType::UInt32),
            ]);

            let values = vec![
                make_message(vec![
                    ("i32", Value::Int32(-100)),
                    ("u32", Value::UInt32(200)),
                ]),
                make_message(vec![
                    ("i32", Value::Int32(300)),
                    ("u32", Value::UInt32(400)),
                ]),
            ];

            let batch = values_to_record_batch(schema, &values).unwrap();

            let i32_col = batch.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
            assert_eq!(i32_col.value(0), -100);
            assert_eq!(i32_col.value(1), 300);

            let u32_col = batch.column(1).as_any().downcast_ref::<UInt32Array>().unwrap();
            assert_eq!(u32_col.value(0), 200);
            assert_eq!(u32_col.value(1), 400);
        }

        #[test]
        fn test_floats() {
            let schema = make_schema(vec![
                ("f32", DataType::Float32),
                ("f64", DataType::Float64),
            ]);

            let values = vec![make_message(vec![
                ("f32", Value::Float32(1.5)),
                ("f64", Value::Float64(2.5)),
            ])];

            let batch = values_to_record_batch(schema, &values).unwrap();

            let f32_col = batch.column(0).as_any().downcast_ref::<Float32Array>().unwrap();
            assert_eq!(f32_col.value(0), 1.5);

            let f64_col = batch.column(1).as_any().downcast_ref::<Float64Array>().unwrap();
            assert_eq!(f64_col.value(0), 2.5);
        }

        #[test]
        fn test_string() {
            let schema = make_schema(vec![("name", DataType::Utf8)]);

            let values = vec![
                make_message(vec![("name", Value::String("alice".to_string()))]),
                make_message(vec![("name", Value::String("bob".to_string()))]),
            ];

            let batch = values_to_record_batch(schema, &values).unwrap();

            let col = batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
            assert_eq!(col.value(0), "alice");
            assert_eq!(col.value(1), "bob");
        }

        #[test]
        fn test_timestamp() {
            let schema = make_schema(vec![(
                "stamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
            )]);

            let values = vec![make_message(vec![(
                "stamp",
                Value::Time {
                    sec: 1000,
                    nsec: 500,
                },
            )])];

            let batch = values_to_record_batch(schema, &values).unwrap();

            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            // 1000 seconds * 1e9 + 500 nanoseconds
            assert_eq!(col.value(0), 1_000_000_000_500i64);
        }

        #[test]
        fn test_duration() {
            let schema = make_schema(vec![("dur", DataType::Duration(TimeUnit::Nanosecond))]);

            let values = vec![make_message(vec![(
                "dur",
                Value::Duration { sec: -5, nsec: 100 },
            )])];

            let batch = values_to_record_batch(schema, &values).unwrap();

            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<DurationNanosecondArray>()
                .unwrap();
            // -5 seconds * 1e9 + 100 nanoseconds
            assert_eq!(col.value(0), -5_000_000_000i64 + 100);
        }
    }

    mod array_building {
        use super::*;

        #[test]
        fn test_variable_list() {
            let schema = make_schema(vec![(
                "data",
                DataType::List(Arc::new(Field::new_list_field(DataType::UInt8, true))),
            )]);

            let values = vec![
                make_message(vec![(
                    "data",
                    Value::Array(vec![Value::UInt8(1), Value::UInt8(2), Value::UInt8(3)]),
                )]),
                make_message(vec![("data", Value::Array(vec![Value::UInt8(4)]))]),
                make_message(vec![("data", Value::Array(vec![]))]),
            ];

            let batch = values_to_record_batch(schema, &values).unwrap();

            let col = batch.column(0).as_any().downcast_ref::<ListArray>().unwrap();
            assert_eq!(col.len(), 3);

            // First row: [1, 2, 3]
            let row0 = col.value(0);
            let row0 = row0.as_any().downcast_ref::<UInt8Array>().unwrap();
            assert_eq!(row0.len(), 3);
            assert_eq!(row0.value(0), 1);

            // Second row: [4]
            let row1 = col.value(1);
            let row1 = row1.as_any().downcast_ref::<UInt8Array>().unwrap();
            assert_eq!(row1.len(), 1);

            // Third row: []
            let row2 = col.value(2);
            assert_eq!(row2.len(), 0);
        }

        #[test]
        fn test_fixed_size_list() {
            let schema = make_schema(vec![(
                "position",
                DataType::FixedSizeList(
                    Arc::new(Field::new_list_field(DataType::Float64, true)),
                    3,
                ),
            )]);

            let values = vec![
                make_message(vec![(
                    "position",
                    Value::Array(vec![
                        Value::Float64(1.0),
                        Value::Float64(2.0),
                        Value::Float64(3.0),
                    ]),
                )]),
                make_message(vec![(
                    "position",
                    Value::Array(vec![
                        Value::Float64(4.0),
                        Value::Float64(5.0),
                        Value::Float64(6.0),
                    ]),
                )]),
            ];

            let batch = values_to_record_batch(schema, &values).unwrap();

            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .unwrap();
            assert_eq!(col.len(), 2);
            assert_eq!(col.value_length(), 3);

            let row0 = col.value(0);
            let row0 = row0.as_any().downcast_ref::<Float64Array>().unwrap();
            assert_eq!(row0.value(0), 1.0);
            assert_eq!(row0.value(2), 3.0);
        }

        #[test]
        fn test_list_of_strings() {
            let schema = make_schema(vec![(
                "names",
                DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, true))),
            )]);

            let values = vec![make_message(vec![(
                "names",
                Value::Array(vec![
                    Value::String("a".to_string()),
                    Value::String("b".to_string()),
                ]),
            )])];

            let batch = values_to_record_batch(schema, &values).unwrap();

            let col = batch.column(0).as_any().downcast_ref::<ListArray>().unwrap();
            let row0 = col.value(0);
            let row0 = row0.as_any().downcast_ref::<StringArray>().unwrap();
            assert_eq!(row0.value(0), "a");
            assert_eq!(row0.value(1), "b");
        }
    }

    mod struct_building {
        use super::*;

        #[test]
        fn test_nested_struct() {
            let inner_fields = Fields::from(vec![
                Field::new("x", DataType::Float64, true),
                Field::new("y", DataType::Float64, true),
            ]);
            let schema = make_schema(vec![("point", DataType::Struct(inner_fields))]);

            let mut point_fields = HashMap::new();
            point_fields.insert("x".to_string(), Value::Float64(1.5));
            point_fields.insert("y".to_string(), Value::Float64(2.5));

            let values = vec![make_message(vec![("point", Value::Message(point_fields))])];

            let batch = values_to_record_batch(schema, &values).unwrap();

            let col = batch.column(0).as_any().downcast_ref::<StructArray>().unwrap();
            let x_col = col.column(0).as_any().downcast_ref::<Float64Array>().unwrap();
            let y_col = col.column(1).as_any().downcast_ref::<Float64Array>().unwrap();

            assert_eq!(x_col.value(0), 1.5);
            assert_eq!(y_col.value(0), 2.5);
        }

        #[test]
        fn test_deeply_nested_struct() {
            // Pose { position: Point { x, y, z }, orientation: Quaternion { x, y, z, w } }
            let point_fields = Fields::from(vec![
                Field::new("x", DataType::Float64, true),
                Field::new("y", DataType::Float64, true),
                Field::new("z", DataType::Float64, true),
            ]);

            let quat_fields = Fields::from(vec![
                Field::new("x", DataType::Float64, true),
                Field::new("y", DataType::Float64, true),
                Field::new("z", DataType::Float64, true),
                Field::new("w", DataType::Float64, true),
            ]);

            let pose_fields = Fields::from(vec![
                Field::new("position", DataType::Struct(point_fields), true),
                Field::new("orientation", DataType::Struct(quat_fields), true),
            ]);

            let schema = make_schema(vec![("pose", DataType::Struct(pose_fields))]);

            let mut position = HashMap::new();
            position.insert("x".to_string(), Value::Float64(1.0));
            position.insert("y".to_string(), Value::Float64(2.0));
            position.insert("z".to_string(), Value::Float64(3.0));

            let mut orientation = HashMap::new();
            orientation.insert("x".to_string(), Value::Float64(0.0));
            orientation.insert("y".to_string(), Value::Float64(0.0));
            orientation.insert("z".to_string(), Value::Float64(0.0));
            orientation.insert("w".to_string(), Value::Float64(1.0));

            let mut pose = HashMap::new();
            pose.insert("position".to_string(), Value::Message(position));
            pose.insert("orientation".to_string(), Value::Message(orientation));

            let values = vec![make_message(vec![("pose", Value::Message(pose))])];

            let batch = values_to_record_batch(schema, &values).unwrap();

            let pose_col = batch.column(0).as_any().downcast_ref::<StructArray>().unwrap();
            let position_col = pose_col.column(0).as_any().downcast_ref::<StructArray>().unwrap();
            let x_col = position_col
                .column(0)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();

            assert_eq!(x_col.value(0), 1.0);

            let orientation_col = pose_col.column(1).as_any().downcast_ref::<StructArray>().unwrap();
            let w_col = orientation_col
                .column(3)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();

            assert_eq!(w_col.value(0), 1.0);
        }

        #[test]
        fn test_list_of_structs() {
            let point_fields = Fields::from(vec![
                Field::new("x", DataType::Float64, true),
                Field::new("y", DataType::Float64, true),
            ]);

            let schema = make_schema(vec![(
                "points",
                DataType::List(Arc::new(Field::new_list_field(
                    DataType::Struct(point_fields),
                    true,
                ))),
            )]);

            let mut p1 = HashMap::new();
            p1.insert("x".to_string(), Value::Float64(1.0));
            p1.insert("y".to_string(), Value::Float64(2.0));

            let mut p2 = HashMap::new();
            p2.insert("x".to_string(), Value::Float64(3.0));
            p2.insert("y".to_string(), Value::Float64(4.0));

            let values = vec![make_message(vec![(
                "points",
                Value::Array(vec![Value::Message(p1), Value::Message(p2)]),
            )])];

            let batch = values_to_record_batch(schema, &values).unwrap();

            let col = batch.column(0).as_any().downcast_ref::<ListArray>().unwrap();
            let row0 = col.value(0);
            let struct_array = row0.as_any().downcast_ref::<StructArray>().unwrap();

            assert_eq!(struct_array.len(), 2);

            let x_col = struct_array
                .column(0)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            assert_eq!(x_col.value(0), 1.0);
            assert_eq!(x_col.value(1), 3.0);
        }
    }

    mod builder_reuse {
        use super::*;

        #[test]
        fn test_builder_reuse() {
            let schema = make_schema(vec![("value", DataType::Int32)]);
            let mut builder = RecordBatchBuilder::new(schema).unwrap();

            // First batch
            builder
                .append(&make_message(vec![("value", Value::Int32(1))]))
                .unwrap();
            builder
                .append(&make_message(vec![("value", Value::Int32(2))]))
                .unwrap();

            let batch1 = builder.finish().unwrap();
            assert_eq!(batch1.num_rows(), 2);

            // Builder should be empty now
            assert!(builder.is_empty());

            // Second batch
            builder
                .append(&make_message(vec![("value", Value::Int32(3))]))
                .unwrap();

            let batch2 = builder.finish().unwrap();
            assert_eq!(batch2.num_rows(), 1);

            let col = batch2.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
            assert_eq!(col.value(0), 3);
        }

        #[test]
        fn test_len_tracking() {
            let schema = make_schema(vec![("value", DataType::Int32)]);
            let mut builder = RecordBatchBuilder::new(schema).unwrap();

            assert_eq!(builder.len(), 0);
            assert!(builder.is_empty());

            builder
                .append(&make_message(vec![("value", Value::Int32(1))]))
                .unwrap();
            assert_eq!(builder.len(), 1);

            builder
                .append(&make_message(vec![("value", Value::Int32(2))]))
                .unwrap();
            assert_eq!(builder.len(), 2);
        }
    }

    mod real_world_schemas {
        use super::*;

        #[test]
        fn test_header_message() {
            let schema = make_schema(vec![
                ("seq", DataType::UInt32),
                ("stamp", DataType::Timestamp(TimeUnit::Nanosecond, None)),
                ("frame_id", DataType::Utf8),
            ]);

            let values = vec![
                make_message(vec![
                    ("seq", Value::UInt32(1)),
                    ("stamp", Value::Time { sec: 1000, nsec: 0 }),
                    ("frame_id", Value::String("base_link".to_string())),
                ]),
                make_message(vec![
                    ("seq", Value::UInt32(2)),
                    ("stamp", Value::Time { sec: 1001, nsec: 500 }),
                    ("frame_id", Value::String("camera".to_string())),
                ]),
            ];

            let batch = values_to_record_batch(schema, &values).unwrap();
            assert_eq!(batch.num_rows(), 2);

            let seq = batch.column(0).as_any().downcast_ref::<UInt32Array>().unwrap();
            assert_eq!(seq.value(0), 1);
            assert_eq!(seq.value(1), 2);

            let frame = batch.column(2).as_any().downcast_ref::<StringArray>().unwrap();
            assert_eq!(frame.value(0), "base_link");
        }

        #[test]
        fn test_image_like_message() {
            let header_fields = Fields::from(vec![
                Field::new("seq", DataType::UInt32, true),
                Field::new("stamp", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
                Field::new("frame_id", DataType::Utf8, true),
            ]);

            let schema = make_schema(vec![
                ("header", DataType::Struct(header_fields)),
                ("height", DataType::UInt32),
                ("width", DataType::UInt32),
                ("encoding", DataType::Utf8),
                (
                    "data",
                    DataType::List(Arc::new(Field::new_list_field(DataType::UInt8, true))),
                ),
            ]);

            let mut header = HashMap::new();
            header.insert("seq".to_string(), Value::UInt32(1));
            header.insert(
                "stamp".to_string(),
                Value::Time {
                    sec: 1000,
                    nsec: 0,
                },
            );
            header.insert("frame_id".to_string(), Value::String("camera".to_string()));

            let values = vec![make_message(vec![
                ("header", Value::Message(header)),
                ("height", Value::UInt32(480)),
                ("width", Value::UInt32(640)),
                ("encoding", Value::String("rgb8".to_string())),
                (
                    "data",
                    Value::Array(vec![Value::UInt8(255), Value::UInt8(0), Value::UInt8(0)]),
                ),
            ])];

            let batch = values_to_record_batch(schema, &values).unwrap();
            assert_eq!(batch.num_rows(), 1);

            let height = batch.column(1).as_any().downcast_ref::<UInt32Array>().unwrap();
            assert_eq!(height.value(0), 480);

            let encoding = batch.column(3).as_any().downcast_ref::<StringArray>().unwrap();
            assert_eq!(encoding.value(0), "rgb8");
        }

        #[test]
        fn test_covariance_matrix() {
            let schema = make_schema(vec![(
                "covariance",
                DataType::FixedSizeList(
                    Arc::new(Field::new_list_field(DataType::Float64, true)),
                    36,
                ),
            )]);

            let cov_values: Vec<Value> = (0..36).map(|i| Value::Float64(i as f64)).collect();

            let values = vec![make_message(vec![("covariance", Value::Array(cov_values))])];

            let batch = values_to_record_batch(schema, &values).unwrap();

            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .unwrap();
            let row0 = col.value(0);
            let arr = row0.as_any().downcast_ref::<Float64Array>().unwrap();

            assert_eq!(arr.len(), 36);
            assert_eq!(arr.value(0), 0.0);
            assert_eq!(arr.value(35), 35.0);
        }
    }

    mod error_handling {
        use super::*;

        #[test]
        fn test_type_mismatch() {
            let schema = make_schema(vec![("value", DataType::Int32)]);

            let values = vec![make_message(vec![(
                "value",
                Value::String("not an int".to_string()),
            )])];

            let result = values_to_record_batch(schema, &values);
            assert!(result.is_err());
        }

        #[test]
        fn test_expected_message() {
            let schema = make_schema(vec![("value", DataType::Int32)]);
            let mut builder = RecordBatchBuilder::new(schema).unwrap();

            let result = builder.append(&Value::Int32(42));
            assert!(matches!(result, Err(BuilderError::ExpectedMessage(_))));
        }
    }
}
