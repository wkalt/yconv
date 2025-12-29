//! Convert Arrow schemas back to ROS1 message definitions.
//!
//! This is the reverse of `to_arrow.rs` - it takes an Arrow Schema and generates
//! a ROS1 `.msg` format definition string.

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use thiserror::Error;

use super::to_arrow::{ROS1_ELEMENT_TYPE_METADATA_KEY, ROS1_TYPE_METADATA_KEY};

/// Errors that can occur during Arrow to ROS1 conversion.
#[derive(Debug, Error)]
pub enum FromArrowError {
    #[error("unsupported Arrow type for ROS1: {0:?}")]
    UnsupportedType(DataType),

    #[error("struct fields required for nested message")]
    MissingStructFields,

    #[error("missing ROS1 type metadata for struct field '{0}'")]
    MissingTypeMetadata(String),
}

/// Convert an Arrow Schema to a full ROS1 message definition string.
///
/// Returns the message definition in MCAP ROS1 format, including all nested
/// type definitions separated by `================================================================================`
/// and `MSG: type/Name` markers.
pub fn arrow_schema_to_msg(schema: &Schema) -> Result<String, FromArrowError> {
    let mut sections = Vec::new();
    let mut collected_types: std::collections::HashMap<String, String> = std::collections::HashMap::new();

    // Generate root message definition
    let mut root_lines = Vec::new();
    for field in schema.fields() {
        let ros_type = arrow_field_to_ros1(field)?;
        root_lines.push(format!("{} {}", ros_type, field.name()));
        // Collect nested type definitions
        collect_nested_types(field, &mut collected_types)?;
    }
    sections.push(root_lines.join("\n"));

    // Add dependency definitions
    // Sort by type name for consistent output
    let mut types: Vec<_> = collected_types.into_iter().collect();
    types.sort_by(|a, b| a.0.cmp(&b.0));

    for (type_name, definition) in types {
        sections.push(format!(
            "================================================================================\nMSG: {}\n{}",
            type_name, definition
        ));
    }

    Ok(sections.join("\n"))
}

/// Recursively collect nested type definitions from a field.
fn collect_nested_types(
    field: &Field,
    collected: &mut std::collections::HashMap<String, String>,
) -> Result<(), FromArrowError> {
    match field.data_type() {
        DataType::Struct(fields) => {
            // Get the type name from metadata
            if let Some(type_name) = field.metadata().get(ROS1_TYPE_METADATA_KEY) {
                if !collected.contains_key(type_name) {
                    // Generate definition for this nested type
                    let mut lines = Vec::new();
                    for nested_field in fields.iter() {
                        let ros_type = arrow_field_to_ros1(nested_field)?;
                        lines.push(format!("{} {}", ros_type, nested_field.name()));
                    }
                    collected.insert(type_name.clone(), lines.join("\n"));

                    // Recursively collect from nested fields
                    for nested_field in fields.iter() {
                        collect_nested_types(nested_field, collected)?;
                    }
                }
            }
        }
        DataType::List(inner) | DataType::LargeList(inner) => {
            // Check for element type metadata
            if let Some(element_type) = field.metadata().get(ROS1_ELEMENT_TYPE_METADATA_KEY) {
                // The inner field is a struct - generate its definition
                if let DataType::Struct(fields) = inner.data_type() {
                    if !collected.contains_key(element_type) {
                        let mut lines = Vec::new();
                        for nested_field in fields.iter() {
                            let ros_type = arrow_field_to_ros1(nested_field)?;
                            lines.push(format!("{} {}", ros_type, nested_field.name()));
                        }
                        collected.insert(element_type.clone(), lines.join("\n"));

                        // Recursively collect from nested fields
                        for nested_field in fields.iter() {
                            collect_nested_types(nested_field, collected)?;
                        }
                    }
                }
            }
            // Recurse into inner field for any nested types
            collect_nested_types(inner, collected)?;
        }
        DataType::FixedSizeList(inner, _) => {
            // Same logic as List
            if let Some(element_type) = field.metadata().get(ROS1_ELEMENT_TYPE_METADATA_KEY) {
                if let DataType::Struct(fields) = inner.data_type() {
                    if !collected.contains_key(element_type) {
                        let mut lines = Vec::new();
                        for nested_field in fields.iter() {
                            let ros_type = arrow_field_to_ros1(nested_field)?;
                            lines.push(format!("{} {}", ros_type, nested_field.name()));
                        }
                        collected.insert(element_type.clone(), lines.join("\n"));

                        for nested_field in fields.iter() {
                            collect_nested_types(nested_field, collected)?;
                        }
                    }
                }
            }
            collect_nested_types(inner, collected)?;
        }
        _ => {}
    }
    Ok(())
}

/// Convert an Arrow Field to a ROS1 type string.
/// This reads metadata to get the original ROS1 type name for struct fields.
fn arrow_field_to_ros1(field: &Field) -> Result<String, FromArrowError> {
    match field.data_type() {
        DataType::Boolean => Ok("bool".to_string()),
        DataType::Int8 => Ok("int8".to_string()),
        DataType::Int16 => Ok("int16".to_string()),
        DataType::Int32 => Ok("int32".to_string()),
        DataType::Int64 => Ok("int64".to_string()),
        DataType::UInt8 => Ok("uint8".to_string()),
        DataType::UInt16 => Ok("uint16".to_string()),
        DataType::UInt32 => Ok("uint32".to_string()),
        DataType::UInt64 => Ok("uint64".to_string()),
        DataType::Float32 => Ok("float32".to_string()),
        DataType::Float64 => Ok("float64".to_string()),
        DataType::Utf8 | DataType::LargeUtf8 => Ok("string".to_string()),

        // time and duration are stored as Timestamp/Duration with Nanosecond precision
        DataType::Timestamp(TimeUnit::Nanosecond, None) => Ok("time".to_string()),
        DataType::Duration(TimeUnit::Nanosecond) => Ok("duration".to_string()),

        // Variable-length arrays - check for element type metadata first, then inner field
        DataType::List(inner) | DataType::LargeList(inner) => {
            // For List<Struct>, the element type is stored in ros1_element_type on the list field
            let element_type = if let Some(element_type_name) =
                field.metadata().get(ROS1_ELEMENT_TYPE_METADATA_KEY)
            {
                element_type_name.clone()
            } else {
                // Fall back to inner field for primitive elements
                arrow_field_to_ros1(inner)?
            };
            Ok(format!("{}[]", element_type))
        }

        // Fixed-size arrays - same logic as variable arrays
        DataType::FixedSizeList(inner, size) => {
            let element_type = if let Some(element_type_name) =
                field.metadata().get(ROS1_ELEMENT_TYPE_METADATA_KEY)
            {
                element_type_name.clone()
            } else {
                arrow_field_to_ros1(inner)?
            };
            Ok(format!("{}[{}]", element_type, size))
        }

        // Nested structs - read ROS1 type name from field metadata
        DataType::Struct(_) => {
            if let Some(ros1_type) = field.metadata().get(ROS1_TYPE_METADATA_KEY) {
                Ok(ros1_type.clone())
            } else {
                // Fallback for schemas without metadata (e.g., created without round-trip support)
                Err(FromArrowError::MissingTypeMetadata(field.name().to_string()))
            }
        }

        _ => Err(FromArrowError::UnsupportedType(field.data_type().clone())),
    }
}

/// Convert an Arrow DataType to a ROS1 type string (for non-field contexts).
fn arrow_type_to_ros1(dt: &DataType) -> Result<String, FromArrowError> {
    match dt {
        DataType::Boolean => Ok("bool".to_string()),
        DataType::Int8 => Ok("int8".to_string()),
        DataType::Int16 => Ok("int16".to_string()),
        DataType::Int32 => Ok("int32".to_string()),
        DataType::Int64 => Ok("int64".to_string()),
        DataType::UInt8 => Ok("uint8".to_string()),
        DataType::UInt16 => Ok("uint16".to_string()),
        DataType::UInt32 => Ok("uint32".to_string()),
        DataType::UInt64 => Ok("uint64".to_string()),
        DataType::Float32 => Ok("float32".to_string()),
        DataType::Float64 => Ok("float64".to_string()),
        DataType::Utf8 | DataType::LargeUtf8 => Ok("string".to_string()),
        DataType::Timestamp(TimeUnit::Nanosecond, None) => Ok("time".to_string()),
        DataType::Duration(TimeUnit::Nanosecond) => Ok("duration".to_string()),
        // Struct without field context - cannot get metadata
        DataType::Struct(_) => Ok("_struct_".to_string()),
        _ => Err(FromArrowError::UnsupportedType(dt.clone())),
    }
}

/// Information needed to serialize Arrow data back to ROS1 binary format.
#[derive(Debug, Clone)]
pub struct Ros1FieldInfo {
    /// Field name
    pub name: String,
    /// ROS1 type string
    pub ros_type: String,
    /// Arrow data type
    pub arrow_type: DataType,
    /// For structs, the nested field info
    pub nested: Option<Vec<Ros1FieldInfo>>,
    /// For arrays, the element info
    pub element: Option<Box<Ros1FieldInfo>>,
    /// Fixed array size (None for variable-length)
    pub fixed_size: Option<usize>,
}

impl Ros1FieldInfo {
    /// Build field info from an Arrow field.
    pub fn from_arrow_field(field: &Field) -> Result<Self, FromArrowError> {
        Self::from_arrow_field_inner(field.name(), field.data_type(), field.metadata())
    }

    /// Build field info from an Arrow data type with optional metadata.
    fn from_arrow_field_inner(
        name: &str,
        dt: &DataType,
        metadata: &std::collections::HashMap<String, String>,
    ) -> Result<Self, FromArrowError> {
        match dt {
            DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Timestamp(TimeUnit::Nanosecond, None)
            | DataType::Duration(TimeUnit::Nanosecond) => Ok(Self {
                name: name.to_string(),
                ros_type: arrow_type_to_ros1(dt)?,
                arrow_type: dt.clone(),
                nested: None,
                element: None,
                fixed_size: None,
            }),

            DataType::List(inner) | DataType::LargeList(inner) => {
                let element_info = Self::from_arrow_field(inner)?;
                // Check for element type metadata on the list field itself
                let element_ros_type = metadata
                    .get(ROS1_ELEMENT_TYPE_METADATA_KEY)
                    .cloned()
                    .unwrap_or_else(|| element_info.ros_type.clone());
                Ok(Self {
                    name: name.to_string(),
                    ros_type: format!("{}[]", element_ros_type),
                    arrow_type: dt.clone(),
                    nested: None,
                    element: Some(Box::new(element_info)),
                    fixed_size: None,
                })
            }

            DataType::FixedSizeList(inner, size) => {
                let element_info = Self::from_arrow_field(inner)?;
                // Check for element type metadata on the list field itself
                let element_ros_type = metadata
                    .get(ROS1_ELEMENT_TYPE_METADATA_KEY)
                    .cloned()
                    .unwrap_or_else(|| element_info.ros_type.clone());
                Ok(Self {
                    name: name.to_string(),
                    ros_type: format!("{}[{}]", element_ros_type, size),
                    arrow_type: dt.clone(),
                    nested: None,
                    element: Some(Box::new(element_info)),
                    fixed_size: Some(*size as usize),
                })
            }

            DataType::Struct(fields) => {
                let nested: Result<Vec<_>, _> = fields
                    .iter()
                    .map(|f| Self::from_arrow_field(f))
                    .collect();
                // Get ROS1 type name from metadata
                let ros_type = metadata
                    .get(ROS1_TYPE_METADATA_KEY)
                    .cloned()
                    .unwrap_or_else(|| "_struct_".to_string());
                Ok(Self {
                    name: name.to_string(),
                    ros_type,
                    arrow_type: dt.clone(),
                    nested: Some(nested?),
                    element: None,
                    fixed_size: None,
                })
            }

            _ => Err(FromArrowError::UnsupportedType(dt.clone())),
        }
    }
}

/// Schema info for serializing an entire message.
#[derive(Debug, Clone)]
pub struct Ros1SchemaInfo {
    /// Fields in order
    pub fields: Vec<Ros1FieldInfo>,
}

impl Ros1SchemaInfo {
    /// Build schema info from an Arrow schema.
    pub fn from_arrow_schema(schema: &Schema) -> Result<Self, FromArrowError> {
        let fields: Result<Vec<_>, _> = schema
            .fields()
            .iter()
            .map(|f| Ros1FieldInfo::from_arrow_field(f))
            .collect();
        Ok(Self { fields: fields? })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Fields;
    use std::sync::Arc;

    #[test]
    fn test_primitive_types() {
        assert_eq!(arrow_type_to_ros1(&DataType::Boolean).unwrap(), "bool");
        assert_eq!(arrow_type_to_ros1(&DataType::Int32).unwrap(), "int32");
        assert_eq!(arrow_type_to_ros1(&DataType::Float64).unwrap(), "float64");
        assert_eq!(arrow_type_to_ros1(&DataType::Utf8).unwrap(), "string");
    }

    #[test]
    fn test_temporal_types() {
        assert_eq!(
            arrow_type_to_ros1(&DataType::Timestamp(TimeUnit::Nanosecond, None)).unwrap(),
            "time"
        );
        assert_eq!(
            arrow_type_to_ros1(&DataType::Duration(TimeUnit::Nanosecond)).unwrap(),
            "duration"
        );
    }

    #[test]
    fn test_array_types() {
        // Test via Field since arrow_field_to_ros1 is the preferred API
        let list_field = Field::new(
            "data",
            DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
            true,
        );
        assert_eq!(arrow_field_to_ros1(&list_field).unwrap(), "float64[]");

        let fixed_list_field = Field::new(
            "data",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Int32, true)), 10),
            true,
        );
        assert_eq!(arrow_field_to_ros1(&fixed_list_field).unwrap(), "int32[10]");
    }

    #[test]
    fn test_schema_to_msg() {
        let schema = Schema::new(vec![
            Field::new("x", DataType::Float64, true),
            Field::new("y", DataType::Float64, true),
            Field::new("z", DataType::Float64, true),
        ]);

        let msg = arrow_schema_to_msg(&schema).unwrap();
        assert_eq!(msg, "float64 x\nfloat64 y\nfloat64 z");
    }

    #[test]
    fn test_field_info_primitive() {
        let field = Field::new("value", DataType::Int32, true);
        let info = Ros1FieldInfo::from_arrow_field(&field).unwrap();
        assert_eq!(info.name, "value");
        assert_eq!(info.ros_type, "int32");
        assert!(info.nested.is_none());
        assert!(info.element.is_none());
    }

    #[test]
    fn test_field_info_array() {
        let field = Field::new(
            "data",
            DataType::List(Arc::new(Field::new("item", DataType::UInt8, true))),
            true,
        );
        let info = Ros1FieldInfo::from_arrow_field(&field).unwrap();
        assert_eq!(info.name, "data");
        assert_eq!(info.ros_type, "uint8[]");
        assert!(info.element.is_some());
        assert!(info.fixed_size.is_none());
    }

    #[test]
    fn test_field_info_struct() {
        let struct_type = DataType::Struct(Fields::from(vec![
            Field::new("sec", DataType::UInt32, true),
            Field::new("nsec", DataType::UInt32, true),
        ]));
        let field = Field::new("stamp", struct_type, true);
        let info = Ros1FieldInfo::from_arrow_field(&field).unwrap();
        assert_eq!(info.name, "stamp");
        assert!(info.nested.is_some());
        let nested = info.nested.unwrap();
        assert_eq!(nested.len(), 2);
        assert_eq!(nested[0].name, "sec");
        assert_eq!(nested[1].name, "nsec");
    }
}
