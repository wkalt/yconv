//! Schema evolution for handling changes in ROS1 message schemas over time.
//!
//! When appending data from new MCAP files to existing LanceDB tables, the message
//! schemas may have evolved. This module provides utilities to:
//!
//! - Compare schemas and detect differences
//! - Merge schemas into a compatible superset
//! - Widen numeric types when safe (e.g., int32 → int64)
//! - Track what alterations need to be made to existing tables

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Fields, Schema, TimeUnit};
use thiserror::Error;

/// Errors that can occur during schema evolution.
#[derive(Debug, Error, PartialEq)]
pub enum EvolutionError {
    #[error("incompatible type change for field '{field}': {existing} → {incoming}")]
    IncompatibleType {
        field: String,
        existing: String,
        incoming: String,
    },

    #[error("field '{field}' nullability changed from required to nullable in existing schema")]
    NullabilityConflict { field: String },

    #[error("list element type mismatch for field '{field}'")]
    ListTypeMismatch { field: String },

    #[error("fixed size list length mismatch for field '{field}': {existing} vs {incoming}")]
    FixedSizeListLengthMismatch {
        field: String,
        existing: i32,
        incoming: i32,
    },

    #[error("struct field mismatch in '{field}': {details}")]
    StructMismatch { field: String, details: String },
}

/// Describes what needs to happen to evolve an existing schema.
#[derive(Debug, Clone, Default)]
pub struct SchemaEvolution {
    /// Fields that need to be added to the existing table.
    pub new_fields: Vec<Field>,

    /// Fields that need type widening (field name → new wider type).
    pub widened_fields: HashMap<String, DataType>,

    /// The merged schema that is compatible with both existing and incoming data.
    pub merged_schema: Option<Arc<Schema>>,

    /// Whether any evolution is needed.
    pub needs_evolution: bool,
}

impl SchemaEvolution {
    /// Check if the existing table needs to be altered.
    pub fn needs_table_alteration(&self) -> bool {
        !self.new_fields.is_empty() || !self.widened_fields.is_empty()
    }
}

/// Check if a type can be widened from `from` to `to`.
///
/// Widening is allowed for:
/// - Signed integers: int8 → int16 → int32 → int64
/// - Unsigned integers: uint8 → uint16 → uint32 → uint64
/// - Floats: float32 → float64
/// - Same type (no widening needed)
pub fn can_widen(from: &DataType, to: &DataType) -> bool {
    if from == to {
        return true;
    }

    match (from, to) {
        // Signed integer widening
        (DataType::Int8, DataType::Int16 | DataType::Int32 | DataType::Int64) => true,
        (DataType::Int16, DataType::Int32 | DataType::Int64) => true,
        (DataType::Int32, DataType::Int64) => true,

        // Unsigned integer widening
        (DataType::UInt8, DataType::UInt16 | DataType::UInt32 | DataType::UInt64) => true,
        (DataType::UInt16, DataType::UInt32 | DataType::UInt64) => true,
        (DataType::UInt32, DataType::UInt64) => true,

        // Float widening
        (DataType::Float32, DataType::Float64) => true,

        // Duration widening (different time units)
        (DataType::Duration(u1), DataType::Duration(u2)) => can_widen_time_unit(u1, u2),

        // Timestamp widening (different time units, same timezone)
        (DataType::Timestamp(u1, tz1), DataType::Timestamp(u2, tz2)) => {
            tz1 == tz2 && can_widen_time_unit(u1, u2)
        }

        // List types - check element type
        (DataType::List(f1), DataType::List(f2)) => can_widen(f1.data_type(), f2.data_type()),

        // Fixed size list - must have same size, check element type
        (DataType::FixedSizeList(f1, s1), DataType::FixedSizeList(f2, s2)) => {
            s1 == s2 && can_widen(f1.data_type(), f2.data_type())
        }

        // Struct types - all fields must be widenable
        (DataType::Struct(fields1), DataType::Struct(fields2)) => {
            can_widen_struct(fields1, fields2)
        }

        _ => false,
    }
}

/// Check if time unit can be widened (to higher precision).
fn can_widen_time_unit(from: &TimeUnit, to: &TimeUnit) -> bool {
    use TimeUnit::*;
    matches!(
        (from, to),
        (Second, Second | Millisecond | Microsecond | Nanosecond)
            | (Millisecond, Millisecond | Microsecond | Nanosecond)
            | (Microsecond, Microsecond | Nanosecond)
            | (Nanosecond, Nanosecond)
    )
}

/// Check if struct fields can be widened.
fn can_widen_struct(from: &Fields, to: &Fields) -> bool {
    // Build a map of `to` fields
    let to_map: HashMap<&str, &Field> = to.iter().map(|f| (f.name().as_str(), f.as_ref())).collect();

    // Every field in `from` must either:
    // 1. Not exist in `to` (will be null in new data)
    // 2. Exist in `to` with a widenable type
    for field in from.iter() {
        if let Some(to_field) = to_map.get(field.name().as_str()) {
            if !can_widen(field.data_type(), to_field.data_type()) {
                return false;
            }
        }
        // Field not in `to` is OK - it will be null
    }

    true
}

/// Get the wider of two compatible types.
///
/// Returns the type that can hold values from both input types.
/// Panics if types are not compatible (use `can_widen` first).
pub fn wider_type(t1: &DataType, t2: &DataType) -> DataType {
    if t1 == t2 {
        return t1.clone();
    }

    // Return the "larger" type
    match (t1, t2) {
        // Signed integers - return the larger
        (DataType::Int8, t) | (t, DataType::Int8) if matches!(t, DataType::Int16 | DataType::Int32 | DataType::Int64) => t.clone(),
        (DataType::Int16, t) | (t, DataType::Int16) if matches!(t, DataType::Int32 | DataType::Int64) => t.clone(),
        (DataType::Int32, DataType::Int64) | (DataType::Int64, DataType::Int32) => DataType::Int64,

        // Unsigned integers
        (DataType::UInt8, t) | (t, DataType::UInt8) if matches!(t, DataType::UInt16 | DataType::UInt32 | DataType::UInt64) => t.clone(),
        (DataType::UInt16, t) | (t, DataType::UInt16) if matches!(t, DataType::UInt32 | DataType::UInt64) => t.clone(),
        (DataType::UInt32, DataType::UInt64) | (DataType::UInt64, DataType::UInt32) => DataType::UInt64,

        // Floats
        (DataType::Float32, DataType::Float64) | (DataType::Float64, DataType::Float32) => DataType::Float64,

        // Duration - use higher precision
        (DataType::Duration(u1), DataType::Duration(u2)) => {
            DataType::Duration(wider_time_unit(u1, u2))
        }

        // Timestamp - use higher precision
        (DataType::Timestamp(u1, tz), DataType::Timestamp(u2, _)) => {
            DataType::Timestamp(wider_time_unit(u1, u2), tz.clone())
        }

        // List - widen element type
        (DataType::List(f1), DataType::List(f2)) => {
            let wider_elem = wider_type(f1.data_type(), f2.data_type());
            let nullable = f1.is_nullable() || f2.is_nullable();
            DataType::List(Arc::new(Field::new(f1.name(), wider_elem, nullable)))
        }

        // Fixed size list
        (DataType::FixedSizeList(f1, size), DataType::FixedSizeList(f2, _)) => {
            let wider_elem = wider_type(f1.data_type(), f2.data_type());
            let nullable = f1.is_nullable() || f2.is_nullable();
            DataType::FixedSizeList(Arc::new(Field::new(f1.name(), wider_elem, nullable)), *size)
        }

        // Struct - merge fields
        (DataType::Struct(fields1), DataType::Struct(fields2)) => {
            DataType::Struct(merge_struct_fields(fields1, fields2))
        }

        _ => t1.clone(), // Fallback (should not happen if can_widen was checked)
    }
}

/// Get the higher precision time unit.
fn wider_time_unit(u1: &TimeUnit, u2: &TimeUnit) -> TimeUnit {
    use TimeUnit::*;
    match (u1, u2) {
        (Nanosecond, _) | (_, Nanosecond) => Nanosecond,
        (Microsecond, _) | (_, Microsecond) => Microsecond,
        (Millisecond, _) | (_, Millisecond) => Millisecond,
        _ => Second,
    }
}

/// Merge struct fields from two schemas.
fn merge_struct_fields(fields1: &Fields, fields2: &Fields) -> Fields {
    let mut merged: Vec<Field> = Vec::new();
    let mut seen: HashSet<&str> = HashSet::new();

    // First, add all fields from fields1, potentially widened
    let fields2_map: HashMap<&str, &Field> =
        fields2.iter().map(|f| (f.name().as_str(), f.as_ref())).collect();

    for f1 in fields1.iter() {
        seen.insert(f1.name());
        if let Some(f2) = fields2_map.get(f1.name().as_str()) {
            // Field exists in both - merge types
            let wider = wider_type(f1.data_type(), f2.data_type());
            let nullable = f1.is_nullable() || f2.is_nullable();
            merged.push(Field::new(f1.name(), wider, nullable));
        } else {
            // Field only in fields1 - make nullable (new data won't have it)
            merged.push(Field::new(f1.name(), f1.data_type().clone(), true));
        }
    }

    // Add fields that only exist in fields2
    for f2 in fields2.iter() {
        if !seen.contains(f2.name().as_str()) {
            // New field - must be nullable (existing data doesn't have it)
            merged.push(Field::new(f2.name(), f2.data_type().clone(), true));
        }
    }

    Fields::from(merged)
}

/// Compare two schemas and compute the evolution needed.
///
/// Returns a `SchemaEvolution` describing what changes are needed to make the
/// existing schema compatible with the incoming schema.
pub fn compare_schemas(
    existing: &Schema,
    incoming: &Schema,
) -> Result<SchemaEvolution, EvolutionError> {
    let mut evolution = SchemaEvolution::default();
    let mut merged_fields: Vec<Field> = Vec::new();
    let mut seen_fields: HashSet<&str> = HashSet::new();

    // Build a map of incoming fields
    let incoming_map: HashMap<&str, &Field> = incoming
        .fields()
        .iter()
        .map(|f| (f.name().as_str(), f.as_ref()))
        .collect();

    // Process existing fields
    for existing_field in existing.fields().iter() {
        let field_name = existing_field.name();
        seen_fields.insert(field_name);

        if let Some(incoming_field) = incoming_map.get(field_name.as_str()) {
            // Field exists in both schemas
            let merged_field =
                merge_field(existing_field, incoming_field, &mut evolution)?;
            merged_fields.push(merged_field);
        } else {
            // Field only in existing schema - keep it nullable
            let field = if existing_field.is_nullable() {
                existing_field.as_ref().clone()
            } else {
                Field::new(field_name, existing_field.data_type().clone(), true)
            };
            merged_fields.push(field);
        }
    }

    // Process new fields (in incoming but not in existing)
    for incoming_field in incoming.fields().iter() {
        if !seen_fields.contains(incoming_field.name().as_str()) {
            // New field - must be nullable
            let new_field = Field::new(
                incoming_field.name(),
                incoming_field.data_type().clone(),
                true,
            );
            evolution.new_fields.push(new_field.clone());
            merged_fields.push(new_field);
        }
    }

    // Check if evolution is needed
    evolution.needs_evolution =
        !evolution.new_fields.is_empty() || !evolution.widened_fields.is_empty();

    evolution.merged_schema = Some(Arc::new(Schema::new(merged_fields)));

    Ok(evolution)
}

/// Merge a single field from existing and incoming schemas.
fn merge_field(
    existing: &Field,
    incoming: &Field,
    evolution: &mut SchemaEvolution,
) -> Result<Field, EvolutionError> {
    let field_name = existing.name();
    let existing_type = existing.data_type();
    let incoming_type = incoming.data_type();

    // Check type compatibility
    if existing_type == incoming_type {
        // Same type - keep existing (with possibly updated nullability)
        let nullable = existing.is_nullable() || incoming.is_nullable();
        return Ok(Field::new(field_name, existing_type.clone(), nullable));
    }

    // Check if widening is possible
    if can_widen(existing_type, incoming_type) {
        let wider = wider_type(existing_type, incoming_type);
        if &wider != existing_type {
            evolution
                .widened_fields
                .insert(field_name.clone(), wider.clone());
        }
        let nullable = existing.is_nullable() || incoming.is_nullable();
        return Ok(Field::new(field_name, wider, nullable));
    }

    // Check reverse widening (incoming can be widened to existing)
    if can_widen(incoming_type, existing_type) {
        let wider = wider_type(existing_type, incoming_type);
        if &wider != existing_type {
            evolution
                .widened_fields
                .insert(field_name.clone(), wider.clone());
        }
        let nullable = existing.is_nullable() || incoming.is_nullable();
        return Ok(Field::new(field_name, wider, nullable));
    }

    // Incompatible types
    Err(EvolutionError::IncompatibleType {
        field: field_name.clone(),
        existing: format!("{:?}", existing_type),
        incoming: format!("{:?}", incoming_type),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    mod can_widen_tests {
        use super::*;

        #[test]
        fn test_same_type() {
            assert!(can_widen(&DataType::Int32, &DataType::Int32));
            assert!(can_widen(&DataType::Utf8, &DataType::Utf8));
            assert!(can_widen(&DataType::Boolean, &DataType::Boolean));
        }

        #[test]
        fn test_signed_integer_widening() {
            assert!(can_widen(&DataType::Int8, &DataType::Int16));
            assert!(can_widen(&DataType::Int8, &DataType::Int32));
            assert!(can_widen(&DataType::Int8, &DataType::Int64));
            assert!(can_widen(&DataType::Int16, &DataType::Int32));
            assert!(can_widen(&DataType::Int16, &DataType::Int64));
            assert!(can_widen(&DataType::Int32, &DataType::Int64));

            // Cannot narrow
            assert!(!can_widen(&DataType::Int64, &DataType::Int32));
            assert!(!can_widen(&DataType::Int32, &DataType::Int16));
        }

        #[test]
        fn test_unsigned_integer_widening() {
            assert!(can_widen(&DataType::UInt8, &DataType::UInt16));
            assert!(can_widen(&DataType::UInt8, &DataType::UInt32));
            assert!(can_widen(&DataType::UInt8, &DataType::UInt64));
            assert!(can_widen(&DataType::UInt16, &DataType::UInt32));
            assert!(can_widen(&DataType::UInt32, &DataType::UInt64));

            // Cannot narrow
            assert!(!can_widen(&DataType::UInt64, &DataType::UInt32));
        }

        #[test]
        fn test_float_widening() {
            assert!(can_widen(&DataType::Float32, &DataType::Float64));
            assert!(!can_widen(&DataType::Float64, &DataType::Float32));
        }

        #[test]
        fn test_incompatible_types() {
            // Signed vs unsigned
            assert!(!can_widen(&DataType::Int32, &DataType::UInt32));
            assert!(!can_widen(&DataType::UInt32, &DataType::Int32));

            // Integer vs float
            assert!(!can_widen(&DataType::Int32, &DataType::Float32));
            assert!(!can_widen(&DataType::Float32, &DataType::Int32));

            // String vs numeric
            assert!(!can_widen(&DataType::Utf8, &DataType::Int32));
            assert!(!can_widen(&DataType::Int32, &DataType::Utf8));
        }

        #[test]
        fn test_duration_widening() {
            use TimeUnit::*;
            assert!(can_widen(
                &DataType::Duration(Second),
                &DataType::Duration(Nanosecond)
            ));
            assert!(can_widen(
                &DataType::Duration(Millisecond),
                &DataType::Duration(Microsecond)
            ));
            // Cannot go to lower precision
            assert!(!can_widen(
                &DataType::Duration(Nanosecond),
                &DataType::Duration(Second)
            ));
        }

        #[test]
        fn test_list_widening() {
            let list_i32 = DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
            let list_i64 = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
            let list_str = DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));

            assert!(can_widen(&list_i32, &list_i64));
            assert!(!can_widen(&list_i64, &list_i32));
            assert!(!can_widen(&list_i32, &list_str));
        }

        #[test]
        fn test_fixed_size_list_widening() {
            let fsl_i32 =
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Int32, true)), 3);
            let fsl_i64 =
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Int64, true)), 3);
            let fsl_i32_diff_size =
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Int32, true)), 4);

            assert!(can_widen(&fsl_i32, &fsl_i64));
            assert!(!can_widen(&fsl_i32, &fsl_i32_diff_size)); // Different sizes
        }

        #[test]
        fn test_struct_widening() {
            let struct1 = DataType::Struct(Fields::from(vec![
                Field::new("a", DataType::Int32, false),
                Field::new("b", DataType::Utf8, false),
            ]));
            let struct2 = DataType::Struct(Fields::from(vec![
                Field::new("a", DataType::Int64, false), // Widened
                Field::new("b", DataType::Utf8, false),
            ]));
            let struct3 = DataType::Struct(Fields::from(vec![
                Field::new("a", DataType::Utf8, false), // Incompatible change
                Field::new("b", DataType::Utf8, false),
            ]));

            assert!(can_widen(&struct1, &struct2));
            assert!(!can_widen(&struct1, &struct3));
        }
    }

    mod wider_type_tests {
        use super::*;

        #[test]
        fn test_same_type() {
            assert_eq!(wider_type(&DataType::Int32, &DataType::Int32), DataType::Int32);
        }

        #[test]
        fn test_integer_widening() {
            assert_eq!(wider_type(&DataType::Int8, &DataType::Int32), DataType::Int32);
            assert_eq!(wider_type(&DataType::Int32, &DataType::Int8), DataType::Int32);
            assert_eq!(wider_type(&DataType::Int32, &DataType::Int64), DataType::Int64);
        }

        #[test]
        fn test_float_widening() {
            assert_eq!(
                wider_type(&DataType::Float32, &DataType::Float64),
                DataType::Float64
            );
        }
    }

    mod compare_schemas_tests {
        use super::*;

        #[test]
        fn test_identical_schemas() {
            let schema = Schema::new(vec![
                Field::new("a", DataType::Int32, false),
                Field::new("b", DataType::Utf8, true),
            ]);

            let evolution = compare_schemas(&schema, &schema).unwrap();
            assert!(!evolution.needs_evolution);
            assert!(evolution.new_fields.is_empty());
            assert!(evolution.widened_fields.is_empty());
        }

        #[test]
        fn test_new_field_in_incoming() {
            let existing = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
            let incoming = Schema::new(vec![
                Field::new("a", DataType::Int32, false),
                Field::new("b", DataType::Utf8, true),
            ]);

            let evolution = compare_schemas(&existing, &incoming).unwrap();
            assert!(evolution.needs_evolution);
            assert_eq!(evolution.new_fields.len(), 1);
            assert_eq!(evolution.new_fields[0].name(), "b");
            assert!(evolution.new_fields[0].is_nullable()); // Must be nullable
        }

        #[test]
        fn test_field_removed_in_incoming() {
            let existing = Schema::new(vec![
                Field::new("a", DataType::Int32, false),
                Field::new("b", DataType::Utf8, false),
            ]);
            let incoming = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

            let evolution = compare_schemas(&existing, &incoming).unwrap();
            // Field b still exists in merged schema but is nullable
            let merged = evolution.merged_schema.unwrap();
            let field_b = merged.field_with_name("b").unwrap();
            assert!(field_b.is_nullable());
        }

        #[test]
        fn test_type_widening() {
            let existing = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
            let incoming = Schema::new(vec![Field::new("a", DataType::Int64, false)]);

            let evolution = compare_schemas(&existing, &incoming).unwrap();
            assert!(evolution.needs_evolution);
            assert!(evolution.widened_fields.contains_key("a"));
            assert_eq!(evolution.widened_fields.get("a"), Some(&DataType::Int64));

            let merged = evolution.merged_schema.unwrap();
            assert_eq!(merged.field_with_name("a").unwrap().data_type(), &DataType::Int64);
        }

        #[test]
        fn test_reverse_type_widening() {
            // Incoming has narrower type - we still widen to the existing
            let existing = Schema::new(vec![Field::new("a", DataType::Int64, false)]);
            let incoming = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

            let evolution = compare_schemas(&existing, &incoming).unwrap();
            // No widening needed - existing is already wider
            assert!(!evolution.needs_evolution);

            let merged = evolution.merged_schema.unwrap();
            assert_eq!(merged.field_with_name("a").unwrap().data_type(), &DataType::Int64);
        }

        #[test]
        fn test_incompatible_type_change() {
            let existing = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
            let incoming = Schema::new(vec![Field::new("a", DataType::Utf8, false)]);

            let result = compare_schemas(&existing, &incoming);
            assert!(matches!(result, Err(EvolutionError::IncompatibleType { .. })));
        }

        #[test]
        fn test_complex_evolution() {
            let existing = Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("name", DataType::Utf8, true),
                Field::new("old_field", DataType::Float32, false),
            ]);
            let incoming = Schema::new(vec![
                Field::new("id", DataType::Int64, false),     // Widened
                Field::new("name", DataType::Utf8, false),    // Same
                Field::new("new_field", DataType::Boolean, true), // New
                // old_field is missing
            ]);

            let evolution = compare_schemas(&existing, &incoming).unwrap();
            assert!(evolution.needs_evolution);

            // Check widened fields
            assert_eq!(evolution.widened_fields.get("id"), Some(&DataType::Int64));

            // Check new fields
            assert_eq!(evolution.new_fields.len(), 1);
            assert_eq!(evolution.new_fields[0].name(), "new_field");

            // Check merged schema
            let merged = evolution.merged_schema.unwrap();
            assert_eq!(merged.fields().len(), 4); // id, name, old_field, new_field
            assert!(merged.field_with_name("old_field").unwrap().is_nullable());
            assert!(merged.field_with_name("new_field").unwrap().is_nullable());
        }

        #[test]
        fn test_nested_struct_evolution() {
            let existing = Schema::new(vec![Field::new(
                "point",
                DataType::Struct(Fields::from(vec![
                    Field::new("x", DataType::Float32, false),
                    Field::new("y", DataType::Float32, false),
                ])),
                false,
            )]);
            let incoming = Schema::new(vec![Field::new(
                "point",
                DataType::Struct(Fields::from(vec![
                    Field::new("x", DataType::Float64, false), // Widened
                    Field::new("y", DataType::Float64, false), // Widened
                    Field::new("z", DataType::Float64, true),  // New field
                ])),
                false,
            )]);

            let evolution = compare_schemas(&existing, &incoming).unwrap();
            assert!(evolution.needs_evolution);

            let merged = evolution.merged_schema.unwrap();
            let point_field = merged.field_with_name("point").unwrap();
            if let DataType::Struct(fields) = point_field.data_type() {
                assert_eq!(fields.len(), 3);
                assert_eq!(
                    fields.iter().find(|f| f.name() == "x").unwrap().data_type(),
                    &DataType::Float64
                );
                assert!(fields.iter().find(|f| f.name() == "z").is_some());
            } else {
                panic!("Expected struct type");
            }
        }

        #[test]
        fn test_list_element_type_evolution() {
            let existing = Schema::new(vec![Field::new(
                "values",
                DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                false,
            )]);
            let incoming = Schema::new(vec![Field::new(
                "values",
                DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                false,
            )]);

            let evolution = compare_schemas(&existing, &incoming).unwrap();
            assert!(evolution.needs_evolution);

            let merged = evolution.merged_schema.unwrap();
            let values_field = merged.field_with_name("values").unwrap();
            if let DataType::List(inner) = values_field.data_type() {
                assert_eq!(inner.data_type(), &DataType::Int64);
            } else {
                panic!("Expected list type");
            }
        }
    }
}
