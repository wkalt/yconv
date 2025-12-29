use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use thiserror::Error;

use crate::ros1::{FieldType, MessageDefinition, Primitive};

/// Metadata key for storing ROS1 message type names on Arrow struct fields.
/// This enables round-trip conversion back to ROS1 format.
pub const ROS1_TYPE_METADATA_KEY: &str = "ros1_type";

/// Metadata key for storing ROS1 element type names on List/FixedSizeList fields.
/// Used when the list contains structs (nested message types).
pub const ROS1_ELEMENT_TYPE_METADATA_KEY: &str = "ros1_element_type";

/// Lance encoding metadata key for compression scheme.
/// Setting this enables compression (zstd/lz4) and unlocks BSS for floats.
const LANCE_COMPRESSION_META_KEY: &str = "lance-encoding:compression";

// Note: Lance supports packed struct encoding via "lance-encoding:packed" metadata,
// but Arrow's array builders don't preserve field metadata, causing schema mismatches.
// TODO: Implement packed struct support by recasting arrays after building.

/// Errors that can occur during schema conversion.
#[derive(Debug, Error)]
pub enum ConversionError {
    #[error("unresolved nested type: {0}")]
    UnresolvedType(String),

    #[error("failed to parse message definition: {0}")]
    ParseError(#[from] crate::ros1::ParseError),

    #[error("invalid schema format: {0}")]
    InvalidFormat(String),
}

/// Registry of message definitions for resolving nested types.
#[derive(Debug, Default)]
pub struct MessageRegistry {
    /// Map from fully-qualified type name to parsed definition
    definitions: HashMap<String, MessageDefinition>,
    /// Map from short type name to full name(s) for fallback lookup
    short_names: HashMap<String, String>,
}

impl MessageRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a message definition with its fully-qualified name.
    pub fn register(&mut self, full_name: &str, definition: MessageDefinition) {
        // Extract short name (after the /) for fallback lookup
        if let Some(short_name) = full_name.rsplit('/').next() {
            self.short_names
                .insert(short_name.to_string(), full_name.to_string());
        }
        self.definitions.insert(full_name.to_string(), definition);
    }

    /// Look up a message definition by name.
    /// Tries the exact name first, then falls back to short name lookup.
    pub fn get(&self, name: &str) -> Option<&MessageDefinition> {
        // Try exact match first
        if let Some(def) = self.definitions.get(name) {
            return Some(def);
        }

        // Fall back to short name lookup
        if let Some(full_name) = self.short_names.get(name) {
            return self.definitions.get(full_name);
        }

        None
    }

    /// Parse an MCAP-style schema string which contains the root message
    /// followed by dependencies separated by `===` lines.
    ///
    /// Format:
    /// ```text
    /// # Root message fields
    /// field1 type1
    /// ================================================================================
    /// MSG: package/DependencyType
    /// # Dependency fields
    /// field2 type2
    /// ```
    pub fn parse_mcap_schema(
        &mut self,
        root_type_name: &str,
        schema_text: &str,
    ) -> Result<(), ConversionError> {
        let sections: Vec<&str> = schema_text.split("\n===").collect();

        // First section is the root message
        let root_def = MessageDefinition::parse(sections[0])?;
        self.register(root_type_name, root_def);

        // Remaining sections are dependencies
        for section in sections.iter().skip(1) {
            // Find the MSG: line that declares the type name
            let mut type_name: Option<&str> = None;
            let mut definition_lines = Vec::new();

            for line in section.lines() {
                let trimmed = line.trim();

                // Skip separator lines (all = signs)
                if trimmed.chars().all(|c| c == '=') && !trimmed.is_empty() {
                    continue;
                }

                // Check for MSG: declaration
                if trimmed.starts_with("MSG:") {
                    type_name = Some(trimmed.strip_prefix("MSG:").unwrap().trim());
                } else {
                    definition_lines.push(line);
                }
            }

            if let Some(name) = type_name {
                let definition_text = definition_lines.join("\n");
                let def = MessageDefinition::parse(&definition_text)?;
                self.register(name, def);
            }
        }

        Ok(())
    }

    /// Get the number of registered definitions.
    pub fn len(&self) -> usize {
        self.definitions.len()
    }

    /// Check if registry is empty.
    pub fn is_empty(&self) -> bool {
        self.definitions.is_empty()
    }
}

/// Convert a ROS1 primitive type to an Arrow DataType.
pub fn primitive_to_arrow(primitive: Primitive) -> DataType {
    match primitive {
        Primitive::Bool => DataType::Boolean,
        Primitive::Int8 => DataType::Int8,
        Primitive::Int16 => DataType::Int16,
        Primitive::Int32 => DataType::Int32,
        Primitive::Int64 => DataType::Int64,
        Primitive::UInt8 => DataType::UInt8,
        Primitive::UInt16 => DataType::UInt16,
        Primitive::UInt32 => DataType::UInt32,
        Primitive::UInt64 => DataType::UInt64,
        Primitive::Float32 => DataType::Float32,
        Primitive::Float64 => DataType::Float64,
        Primitive::String => DataType::Utf8,
        Primitive::Time => DataType::Timestamp(TimeUnit::Nanosecond, None),
        Primitive::Duration => DataType::Duration(TimeUnit::Nanosecond),
    }
}

/// Result of converting a ROS1 field type to Arrow.
/// Contains the DataType and optionally the ROS1 message type name (for nested types).
struct ArrowTypeResult {
    data_type: DataType,
    /// ROS1 type name for struct types (stored as ros1_type metadata)
    ros1_type: Option<String>,
    /// ROS1 element type name for list types containing structs (stored as ros1_element_type metadata)
    ros1_element_type: Option<String>,
    /// True if this is an empty struct that should be skipped (Lance doesn't support empty nullable structs)
    is_empty_struct: bool,
}

/// Convert a ROS1 field type to an Arrow DataType.
///
/// For nested types, requires a registry to resolve the nested message definitions.
pub fn field_type_to_arrow(
    field_type: &FieldType,
    registry: &MessageRegistry,
) -> Result<DataType, ConversionError> {
    Ok(field_type_to_arrow_with_info(field_type, registry)?.data_type)
}

/// Convert a ROS1 field type to an Arrow DataType, preserving ROS1 type info.
fn field_type_to_arrow_with_info(
    field_type: &FieldType,
    registry: &MessageRegistry,
) -> Result<ArrowTypeResult, ConversionError> {
    match field_type {
        FieldType::Primitive(p) => Ok(ArrowTypeResult {
            data_type: primitive_to_arrow(*p),
            ros1_type: None,
            ros1_element_type: None,
            is_empty_struct: false,
        }),

        FieldType::Array { element } => {
            let element_result = field_type_to_arrow_with_info(element, registry)?;
            // Note: We don't add packed metadata here because Arrow's array builders
            // don't preserve field metadata, causing schema mismatches during construction.
            // TODO: Implement packed struct support by recasting arrays after building.
            let inner_field = Field::new("item", element_result.data_type, true);
            Ok(ArrowTypeResult {
                data_type: DataType::List(Arc::new(inner_field)),
                ros1_type: None,
                // Propagate element type (either ros1_type for Struct elements, or ros1_element_type for nested lists)
                ros1_element_type: element_result.ros1_type.or(element_result.ros1_element_type),
                is_empty_struct: false,
            })
        }

        FieldType::FixedArray { element, length } => {
            let element_result = field_type_to_arrow_with_info(element, registry)?;
            let inner_field = Field::new("item", element_result.data_type, true);
            Ok(ArrowTypeResult {
                data_type: DataType::FixedSizeList(Arc::new(inner_field), *length as i32),
                ros1_type: None,
                // Propagate element type
                ros1_element_type: element_result.ros1_type.or(element_result.ros1_element_type),
                is_empty_struct: false,
            })
        }

        FieldType::Nested { package, name } => {
            let full_name = match package {
                Some(pkg) => format!("{}/{}", pkg, name),
                None => name.clone(),
            };

            // Look up the nested type definition
            let nested_def = registry
                .get(&full_name)
                .ok_or_else(|| ConversionError::UnresolvedType(full_name.clone()))?;

            // Convert the nested message to a struct type
            let fields = message_to_arrow_fields(nested_def, registry)?;
            let is_empty = fields.is_empty();
            Ok(ArrowTypeResult {
                data_type: DataType::Struct(fields.into()),
                ros1_type: Some(full_name),
                ros1_element_type: None,
                is_empty_struct: is_empty,
            })
        }
    }
}

/// Check if a data type should have Lance compression enabled.
/// Compression enables BSS (Byte Stream Split) for floats and improves storage for most types.
fn should_enable_compression(dt: &DataType) -> bool {
    match dt {
        // Numeric types benefit from compression
        DataType::Float32 | DataType::Float64 |
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 |
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => true,

        // Timestamps and durations
        DataType::Timestamp(_, _) | DataType::Duration(_) => true,

        // Fixed-size lists with numeric elements (e.g., covariance matrices, vectors)
        DataType::FixedSizeList(inner, _) => should_enable_compression(inner.data_type()),

        // Strings and binary don't benefit as much (Lance uses FSST for strings)
        // Structs are handled recursively via their fields
        _ => false,
    }
}

/// Create an Arrow Field with optional ROS1 type metadata.
fn create_field_with_metadata(name: &str, result: ArrowTypeResult) -> Field {
    // Add metadata based on what type info we have
    let mut metadata = std::collections::HashMap::new();

    // Add Lance compression metadata for types that benefit from it.
    // This enables ZSTD compression and unlocks BSS (Byte Stream Split) for floats.
    if should_enable_compression(&result.data_type) {
        metadata.insert(LANCE_COMPRESSION_META_KEY.to_string(), "zstd".to_string());
    }

    if let Some(type_name) = result.ros1_type {
        metadata.insert(ROS1_TYPE_METADATA_KEY.to_string(), type_name);
    }
    if let Some(element_type) = result.ros1_element_type {
        metadata.insert(ROS1_ELEMENT_TYPE_METADATA_KEY.to_string(), element_type);
    }

    let field = Field::new(name, result.data_type, true);
    if metadata.is_empty() {
        field
    } else {
        field.with_metadata(metadata)
    }
}

/// Convert a ROS1 message definition to Arrow Fields.
/// Note: Empty struct fields are skipped because Lance doesn't support nullable empty structs.
pub fn message_to_arrow_fields(
    definition: &MessageDefinition,
    registry: &MessageRegistry,
) -> Result<Vec<Field>, ConversionError> {
    definition
        .fields
        .iter()
        .filter_map(|f| {
            let result = match field_type_to_arrow_with_info(&f.field_type, registry) {
                Ok(r) => r,
                Err(e) => return Some(Err(e)),
            };
            // Skip empty struct fields - Lance doesn't support nullable empty structs
            if result.is_empty_struct {
                return None;
            }
            Some(Ok(create_field_with_metadata(&f.name, result)))
        })
        .collect()
}

/// Convert a ROS1 message definition to an Arrow Schema.
///
/// This is the main entry point for schema conversion.
pub fn message_to_arrow_schema(
    definition: &MessageDefinition,
    registry: &MessageRegistry,
) -> Result<Schema, ConversionError> {
    let fields = message_to_arrow_fields(definition, registry)?;
    Ok(Schema::new(fields))
}

/// Convenience function to convert a standalone message (no nested types) to Arrow Schema.
pub fn simple_message_to_arrow_schema(
    definition: &MessageDefinition,
) -> Result<Schema, ConversionError> {
    let registry = MessageRegistry::new();
    message_to_arrow_schema(definition, &registry)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ros1::MessageDefinition;

    mod primitive_conversion {
        use super::*;

        #[test]
        fn test_boolean() {
            assert_eq!(primitive_to_arrow(Primitive::Bool), DataType::Boolean);
        }

        #[test]
        fn test_integers() {
            assert_eq!(primitive_to_arrow(Primitive::Int8), DataType::Int8);
            assert_eq!(primitive_to_arrow(Primitive::Int16), DataType::Int16);
            assert_eq!(primitive_to_arrow(Primitive::Int32), DataType::Int32);
            assert_eq!(primitive_to_arrow(Primitive::Int64), DataType::Int64);
            assert_eq!(primitive_to_arrow(Primitive::UInt8), DataType::UInt8);
            assert_eq!(primitive_to_arrow(Primitive::UInt16), DataType::UInt16);
            assert_eq!(primitive_to_arrow(Primitive::UInt32), DataType::UInt32);
            assert_eq!(primitive_to_arrow(Primitive::UInt64), DataType::UInt64);
        }

        #[test]
        fn test_floats() {
            assert_eq!(primitive_to_arrow(Primitive::Float32), DataType::Float32);
            assert_eq!(primitive_to_arrow(Primitive::Float64), DataType::Float64);
        }

        #[test]
        fn test_string() {
            assert_eq!(primitive_to_arrow(Primitive::String), DataType::Utf8);
        }

        #[test]
        fn test_time() {
            assert_eq!(
                primitive_to_arrow(Primitive::Time),
                DataType::Timestamp(TimeUnit::Nanosecond, None)
            );
        }

        #[test]
        fn test_duration() {
            assert_eq!(
                primitive_to_arrow(Primitive::Duration),
                DataType::Duration(TimeUnit::Nanosecond)
            );
        }
    }

    mod field_type_conversion {
        use super::*;

        #[test]
        fn test_primitive_field() {
            let registry = MessageRegistry::new();
            let ft = FieldType::Primitive(Primitive::Float64);
            let arrow_type = field_type_to_arrow(&ft, &registry).unwrap();
            assert_eq!(arrow_type, DataType::Float64);
        }

        #[test]
        fn test_variable_array() {
            let registry = MessageRegistry::new();
            let ft = FieldType::Array {
                element: Box::new(FieldType::Primitive(Primitive::UInt8)),
            };
            let arrow_type = field_type_to_arrow(&ft, &registry).unwrap();

            match arrow_type {
                DataType::List(field) => {
                    assert_eq!(field.data_type(), &DataType::UInt8);
                }
                _ => panic!("Expected List type"),
            }
        }

        #[test]
        fn test_fixed_array() {
            let registry = MessageRegistry::new();
            let ft = FieldType::FixedArray {
                element: Box::new(FieldType::Primitive(Primitive::Float32)),
                length: 3,
            };
            let arrow_type = field_type_to_arrow(&ft, &registry).unwrap();

            match arrow_type {
                DataType::FixedSizeList(field, size) => {
                    assert_eq!(field.data_type(), &DataType::Float32);
                    assert_eq!(size, 3);
                }
                _ => panic!("Expected FixedSizeList type"),
            }
        }

        #[test]
        fn test_nested_type_resolved() {
            let mut registry = MessageRegistry::new();

            // Register a nested type
            let point_def = MessageDefinition::parse(
                r#"
                float64 x
                float64 y
                float64 z
            "#,
            )
            .unwrap();
            registry.register("geometry_msgs/Point", point_def);

            let ft = FieldType::Nested {
                package: Some("geometry_msgs".to_string()),
                name: "Point".to_string(),
            };
            let arrow_type = field_type_to_arrow(&ft, &registry).unwrap();

            match arrow_type {
                DataType::Struct(fields) => {
                    assert_eq!(fields.len(), 3);
                    assert_eq!(fields[0].name(), "x");
                    assert_eq!(fields[0].data_type(), &DataType::Float64);
                    assert_eq!(fields[1].name(), "y");
                    assert_eq!(fields[2].name(), "z");
                }
                _ => panic!("Expected Struct type"),
            }
        }

        #[test]
        fn test_nested_type_unresolved() {
            let registry = MessageRegistry::new();
            let ft = FieldType::Nested {
                package: Some("geometry_msgs".to_string()),
                name: "Point".to_string(),
            };
            let result = field_type_to_arrow(&ft, &registry);

            assert!(result.is_err());
            match result {
                Err(ConversionError::UnresolvedType(name)) => {
                    assert_eq!(name, "geometry_msgs/Point");
                }
                _ => panic!("Expected UnresolvedType error"),
            }
        }

        #[test]
        fn test_array_of_nested_types() {
            let mut registry = MessageRegistry::new();

            let point_def = MessageDefinition::parse("float64 x\nfloat64 y").unwrap();
            registry.register("Point", point_def);

            let ft = FieldType::Array {
                element: Box::new(FieldType::Nested {
                    package: None,
                    name: "Point".to_string(),
                }),
            };
            let arrow_type = field_type_to_arrow(&ft, &registry).unwrap();

            match arrow_type {
                DataType::List(field) => match field.data_type() {
                    DataType::Struct(fields) => {
                        assert_eq!(fields.len(), 2);
                    }
                    _ => panic!("Expected Struct inside List"),
                },
                _ => panic!("Expected List type"),
            }
        }
    }

    mod message_conversion {
        use super::*;

        #[test]
        fn test_simple_message() {
            let def = MessageDefinition::parse(
                r#"
                float64 x
                float64 y
                float64 z
            "#,
            )
            .unwrap();

            let schema = simple_message_to_arrow_schema(&def).unwrap();

            assert_eq!(schema.fields().len(), 3);
            assert_eq!(schema.field(0).name(), "x");
            assert_eq!(schema.field(0).data_type(), &DataType::Float64);
            assert!(schema.field(0).is_nullable());
        }

        #[test]
        fn test_std_msgs_header() {
            let def = MessageDefinition::parse(
                r#"
                uint32 seq
                time stamp
                string frame_id
            "#,
            )
            .unwrap();

            let schema = simple_message_to_arrow_schema(&def).unwrap();

            assert_eq!(schema.fields().len(), 3);
            assert_eq!(schema.field(0).name(), "seq");
            assert_eq!(schema.field(0).data_type(), &DataType::UInt32);
            assert_eq!(schema.field(1).name(), "stamp");
            assert_eq!(
                schema.field(1).data_type(),
                &DataType::Timestamp(TimeUnit::Nanosecond, None)
            );
            assert_eq!(schema.field(2).name(), "frame_id");
            assert_eq!(schema.field(2).data_type(), &DataType::Utf8);
        }

        #[test]
        fn test_message_with_arrays() {
            let def = MessageDefinition::parse(
                r#"
                float64[3] position
                float64[4] orientation
                uint8[] data
            "#,
            )
            .unwrap();

            let schema = simple_message_to_arrow_schema(&def).unwrap();

            assert_eq!(schema.fields().len(), 3);

            // Fixed array
            match schema.field(0).data_type() {
                DataType::FixedSizeList(_, 3) => {}
                _ => panic!("Expected FixedSizeList(3)"),
            }

            // Variable array
            match schema.field(2).data_type() {
                DataType::List(_) => {}
                _ => panic!("Expected List"),
            }
        }

        #[test]
        fn test_message_with_nested_type() {
            let mut registry = MessageRegistry::new();

            // Register Header
            let header_def = MessageDefinition::parse(
                r#"
                uint32 seq
                time stamp
                string frame_id
            "#,
            )
            .unwrap();
            registry.register("std_msgs/Header", header_def);

            // Parse a message that uses Header
            let def = MessageDefinition::parse(
                r#"
                std_msgs/Header header
                float64[] data
            "#,
            )
            .unwrap();

            let schema = message_to_arrow_schema(&def, &registry).unwrap();

            assert_eq!(schema.fields().len(), 2);

            // Check nested struct
            match schema.field(0).data_type() {
                DataType::Struct(fields) => {
                    assert_eq!(fields.len(), 3);
                    assert_eq!(fields[0].name(), "seq");
                    assert_eq!(fields[1].name(), "stamp");
                    assert_eq!(fields[2].name(), "frame_id");
                }
                _ => panic!("Expected Struct type for header"),
            }
        }

        #[test]
        fn test_deeply_nested_types() {
            let mut registry = MessageRegistry::new();

            // Point
            let point_def = MessageDefinition::parse("float64 x\nfloat64 y\nfloat64 z").unwrap();
            registry.register("geometry_msgs/Point", point_def);

            // Quaternion
            let quat_def =
                MessageDefinition::parse("float64 x\nfloat64 y\nfloat64 z\nfloat64 w").unwrap();
            registry.register("geometry_msgs/Quaternion", quat_def);

            // Pose (uses Point and Quaternion)
            let pose_def = MessageDefinition::parse(
                r#"
                geometry_msgs/Point position
                geometry_msgs/Quaternion orientation
            "#,
            )
            .unwrap();
            registry.register("geometry_msgs/Pose", pose_def);

            // PoseStamped (uses Pose and Header)
            let header_def =
                MessageDefinition::parse("uint32 seq\ntime stamp\nstring frame_id").unwrap();
            registry.register("std_msgs/Header", header_def);

            let pose_stamped_def = MessageDefinition::parse(
                r#"
                std_msgs/Header header
                geometry_msgs/Pose pose
            "#,
            )
            .unwrap();

            let schema = message_to_arrow_schema(&pose_stamped_def, &registry).unwrap();

            assert_eq!(schema.fields().len(), 2);

            // Check pose.position.x exists in nested structure
            match schema.field(1).data_type() {
                DataType::Struct(pose_fields) => {
                    assert_eq!(pose_fields.len(), 2);
                    match pose_fields[0].data_type() {
                        DataType::Struct(point_fields) => {
                            assert_eq!(point_fields.len(), 3);
                            assert_eq!(point_fields[0].name(), "x");
                        }
                        _ => panic!("Expected Struct for position"),
                    }
                }
                _ => panic!("Expected Struct for pose"),
            }
        }
    }

    mod registry {
        use super::*;

        #[test]
        fn test_register_and_lookup() {
            let mut registry = MessageRegistry::new();
            let def = MessageDefinition::parse("float64 x").unwrap();
            registry.register("test/Point", def.clone());

            assert!(registry.get("test/Point").is_some());
            assert!(registry.get("test/Other").is_none());
        }

        #[test]
        fn test_len_and_is_empty() {
            let mut registry = MessageRegistry::new();
            assert!(registry.is_empty());
            assert_eq!(registry.len(), 0);

            let def = MessageDefinition::parse("float64 x").unwrap();
            registry.register("test/Point", def);

            assert!(!registry.is_empty());
            assert_eq!(registry.len(), 1);
        }

        #[test]
        fn test_parse_mcap_schema_simple() {
            let mut registry = MessageRegistry::new();

            let schema_text = r#"
uint32 seq
time stamp
string frame_id
"#;

            registry
                .parse_mcap_schema("std_msgs/Header", schema_text)
                .unwrap();

            assert_eq!(registry.len(), 1);
            let def = registry.get("std_msgs/Header").unwrap();
            assert_eq!(def.fields.len(), 3);
        }

        #[test]
        fn test_parse_mcap_schema_with_dependencies() {
            let mut registry = MessageRegistry::new();

            // This is how MCAP stores ROS1 schemas - root message first,
            // then dependencies separated by === and MSG: lines
            let schema_text = r#"
std_msgs/Header header
float64[] data
================================================================================
MSG: std_msgs/Header
uint32 seq
time stamp
string frame_id
"#;

            registry
                .parse_mcap_schema("sensor_msgs/Custom", schema_text)
                .unwrap();

            assert_eq!(registry.len(), 2);

            // Check root message
            let root = registry.get("sensor_msgs/Custom").unwrap();
            assert_eq!(root.fields.len(), 2);

            // Check dependency
            let header = registry.get("std_msgs/Header").unwrap();
            assert_eq!(header.fields.len(), 3);
        }

        #[test]
        fn test_parse_mcap_schema_multiple_dependencies() {
            let mut registry = MessageRegistry::new();

            let schema_text = r#"
std_msgs/Header header
geometry_msgs/Pose pose
================================================================================
MSG: std_msgs/Header
uint32 seq
time stamp
string frame_id
================================================================================
MSG: geometry_msgs/Pose
geometry_msgs/Point position
geometry_msgs/Quaternion orientation
================================================================================
MSG: geometry_msgs/Point
float64 x
float64 y
float64 z
================================================================================
MSG: geometry_msgs/Quaternion
float64 x
float64 y
float64 z
float64 w
"#;

            registry
                .parse_mcap_schema("nav_msgs/Custom", schema_text)
                .unwrap();

            assert_eq!(registry.len(), 5);
            assert!(registry.get("nav_msgs/Custom").is_some());
            assert!(registry.get("std_msgs/Header").is_some());
            assert!(registry.get("geometry_msgs/Pose").is_some());
            assert!(registry.get("geometry_msgs/Point").is_some());
            assert!(registry.get("geometry_msgs/Quaternion").is_some());
        }

        #[test]
        fn test_parse_mcap_schema_and_convert() {
            let mut registry = MessageRegistry::new();

            let schema_text = r#"
std_msgs/Header header
geometry_msgs/Point[] points
================================================================================
MSG: std_msgs/Header
uint32 seq
time stamp
string frame_id
================================================================================
MSG: geometry_msgs/Point
float64 x
float64 y
float64 z
"#;

            registry
                .parse_mcap_schema("custom_msgs/PointList", schema_text)
                .unwrap();

            let root = registry.get("custom_msgs/PointList").unwrap();
            let schema = message_to_arrow_schema(root, &registry).unwrap();

            assert_eq!(schema.fields().len(), 2);

            // header should be a struct
            match schema.field(0).data_type() {
                DataType::Struct(fields) => {
                    assert_eq!(fields.len(), 3);
                }
                _ => panic!("Expected Struct for header"),
            }

            // points should be a list of structs
            match schema.field(1).data_type() {
                DataType::List(field) => match field.data_type() {
                    DataType::Struct(fields) => {
                        assert_eq!(fields.len(), 3);
                    }
                    _ => panic!("Expected Struct inside List"),
                },
                _ => panic!("Expected List for points"),
            }
        }
    }

    mod real_world_messages {
        use super::*;

        #[test]
        fn test_sensor_msgs_image_schema() {
            let mut registry = MessageRegistry::new();

            let schema_text = r#"
std_msgs/Header header
uint32 height
uint32 width
string encoding
uint8 is_bigendian
uint32 step
uint8[] data
================================================================================
MSG: std_msgs/Header
uint32 seq
time stamp
string frame_id
"#;

            registry
                .parse_mcap_schema("sensor_msgs/Image", schema_text)
                .unwrap();

            let root = registry.get("sensor_msgs/Image").unwrap();
            let schema = message_to_arrow_schema(root, &registry).unwrap();

            assert_eq!(schema.fields().len(), 7);
            assert_eq!(schema.field(0).name(), "header");
            assert_eq!(schema.field(1).name(), "height");
            assert_eq!(schema.field(6).name(), "data");

            // Verify data is List<UInt8>
            match schema.field(6).data_type() {
                DataType::List(field) => {
                    assert_eq!(field.data_type(), &DataType::UInt8);
                }
                _ => panic!("Expected List<UInt8> for data"),
            }
        }

        #[test]
        fn test_geometry_msgs_pose_with_covariance() {
            let mut registry = MessageRegistry::new();

            let schema_text = r#"
geometry_msgs/Pose pose
float64[36] covariance
================================================================================
MSG: geometry_msgs/Pose
geometry_msgs/Point position
geometry_msgs/Quaternion orientation
================================================================================
MSG: geometry_msgs/Point
float64 x
float64 y
float64 z
================================================================================
MSG: geometry_msgs/Quaternion
float64 x
float64 y
float64 z
float64 w
"#;

            registry
                .parse_mcap_schema("geometry_msgs/PoseWithCovariance", schema_text)
                .unwrap();

            let root = registry.get("geometry_msgs/PoseWithCovariance").unwrap();
            let schema = message_to_arrow_schema(root, &registry).unwrap();

            assert_eq!(schema.fields().len(), 2);

            // Check covariance is FixedSizeList(36)
            match schema.field(1).data_type() {
                DataType::FixedSizeList(field, 36) => {
                    assert_eq!(field.data_type(), &DataType::Float64);
                }
                _ => panic!("Expected FixedSizeList(36) for covariance"),
            }
        }

        #[test]
        fn test_nav_msgs_odometry() {
            let mut registry = MessageRegistry::new();

            let schema_text = r#"
std_msgs/Header header
string child_frame_id
geometry_msgs/PoseWithCovariance pose
geometry_msgs/TwistWithCovariance twist
================================================================================
MSG: std_msgs/Header
uint32 seq
time stamp
string frame_id
================================================================================
MSG: geometry_msgs/PoseWithCovariance
geometry_msgs/Pose pose
float64[36] covariance
================================================================================
MSG: geometry_msgs/Pose
geometry_msgs/Point position
geometry_msgs/Quaternion orientation
================================================================================
MSG: geometry_msgs/Point
float64 x
float64 y
float64 z
================================================================================
MSG: geometry_msgs/Quaternion
float64 x
float64 y
float64 z
float64 w
================================================================================
MSG: geometry_msgs/TwistWithCovariance
geometry_msgs/Twist twist
float64[36] covariance
================================================================================
MSG: geometry_msgs/Twist
geometry_msgs/Vector3 linear
geometry_msgs/Vector3 angular
================================================================================
MSG: geometry_msgs/Vector3
float64 x
float64 y
float64 z
"#;

            registry
                .parse_mcap_schema("nav_msgs/Odometry", schema_text)
                .unwrap();

            // Should have 9 types registered
            assert_eq!(registry.len(), 9);

            let root = registry.get("nav_msgs/Odometry").unwrap();
            let schema = message_to_arrow_schema(root, &registry).unwrap();

            assert_eq!(schema.fields().len(), 4);
            assert_eq!(schema.field(0).name(), "header");
            assert_eq!(schema.field(1).name(), "child_frame_id");
            assert_eq!(schema.field(2).name(), "pose");
            assert_eq!(schema.field(3).name(), "twist");
        }

        #[test]
        fn test_sensor_msgs_pointcloud2() {
            let mut registry = MessageRegistry::new();

            let schema_text = r#"
std_msgs/Header header
uint32 height
uint32 width
sensor_msgs/PointField[] fields
bool is_bigendian
uint32 point_step
uint32 row_step
uint8[] data
bool is_dense
================================================================================
MSG: std_msgs/Header
uint32 seq
time stamp
string frame_id
================================================================================
MSG: sensor_msgs/PointField
uint8 INT8=1
uint8 UINT8=2
uint8 INT16=3
uint8 UINT16=4
uint8 INT32=5
uint8 UINT32=6
uint8 FLOAT32=7
uint8 FLOAT64=8
string name
uint32 offset
uint8 datatype
uint32 count
"#;

            registry
                .parse_mcap_schema("sensor_msgs/PointCloud2", schema_text)
                .unwrap();

            let root = registry.get("sensor_msgs/PointCloud2").unwrap();
            let schema = message_to_arrow_schema(root, &registry).unwrap();

            assert_eq!(schema.fields().len(), 9);

            // Check that fields is a List of Struct
            match schema.field(3).data_type() {
                DataType::List(field) => match field.data_type() {
                    DataType::Struct(struct_fields) => {
                        // PointField has 4 fields (constants are not fields)
                        assert_eq!(struct_fields.len(), 4);
                    }
                    _ => panic!("Expected Struct inside List"),
                },
                _ => panic!("Expected List for fields"),
            }
        }
    }
}
