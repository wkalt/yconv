use std::fmt;
use thiserror::Error;

/// Errors that can occur during message definition parsing.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ParseError {
    #[error("invalid primitive type: {0}")]
    InvalidPrimitive(String),

    #[error("invalid field definition: {0}")]
    InvalidField(String),

    #[error("invalid array syntax: {0}")]
    InvalidArray(String),

    #[error("invalid constant definition: {0}")]
    InvalidConstant(String),

    #[error("empty field name")]
    EmptyFieldName,

    #[error("empty type name")]
    EmptyTypeName,
}

/// ROS1 primitive types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Primitive {
    Bool,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,
    String,
    Time,
    Duration,
}

impl Primitive {
    /// Parse a primitive type from its ROS1 string representation.
    pub fn from_str(s: &str) -> Option<Primitive> {
        match s {
            "bool" => Some(Primitive::Bool),
            "int8" | "byte" => Some(Primitive::Int8),
            "int16" => Some(Primitive::Int16),
            "int32" => Some(Primitive::Int32),
            "int64" => Some(Primitive::Int64),
            "uint8" | "char" => Some(Primitive::UInt8),
            "uint16" => Some(Primitive::UInt16),
            "uint32" => Some(Primitive::UInt32),
            "uint64" => Some(Primitive::UInt64),
            "float32" => Some(Primitive::Float32),
            "float64" => Some(Primitive::Float64),
            "string" => Some(Primitive::String),
            "time" => Some(Primitive::Time),
            "duration" => Some(Primitive::Duration),
            _ => None,
        }
    }
}

impl fmt::Display for Primitive {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Primitive::Bool => "bool",
            Primitive::Int8 => "int8",
            Primitive::Int16 => "int16",
            Primitive::Int32 => "int32",
            Primitive::Int64 => "int64",
            Primitive::UInt8 => "uint8",
            Primitive::UInt16 => "uint16",
            Primitive::UInt32 => "uint32",
            Primitive::UInt64 => "uint64",
            Primitive::Float32 => "float32",
            Primitive::Float64 => "float64",
            Primitive::String => "string",
            Primitive::Time => "time",
            Primitive::Duration => "duration",
        };
        write!(f, "{}", s)
    }
}

/// The type of a field in a ROS1 message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FieldType {
    /// A primitive type (bool, int32, string, etc.)
    Primitive(Primitive),

    /// A variable-length array (e.g., `uint8[]`)
    Array { element: Box<FieldType> },

    /// A fixed-length array (e.g., `uint8[16]`)
    FixedArray {
        element: Box<FieldType>,
        length: usize,
    },

    /// A nested message type (e.g., `std_msgs/Header` or `Header`)
    Nested {
        package: Option<String>,
        name: String,
    },
}

impl fmt::Display for FieldType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FieldType::Primitive(p) => write!(f, "{}", p),
            FieldType::Array { element } => write!(f, "{}[]", element),
            FieldType::FixedArray { element, length } => write!(f, "{}[{}]", element, length),
            FieldType::Nested {
                package: Some(pkg),
                name,
            } => write!(f, "{}/{}", pkg, name),
            FieldType::Nested {
                package: None,
                name,
            } => write!(f, "{}", name),
        }
    }
}

/// A field in a ROS1 message definition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Field {
    pub name: String,
    pub field_type: FieldType,
}

impl Field {
    pub fn new(name: impl Into<String>, field_type: FieldType) -> Self {
        Self {
            name: name.into(),
            field_type,
        }
    }
}

/// A constant definition in a ROS1 message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Constant {
    pub name: String,
    pub constant_type: Primitive,
    pub value: String,
}

/// A complete ROS1 message definition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageDefinition {
    pub fields: Vec<Field>,
    pub constants: Vec<Constant>,
}

impl MessageDefinition {
    /// Parse a ROS1 message definition from its text representation.
    pub fn parse(input: &str) -> Result<Self, ParseError> {
        let mut fields = Vec::new();
        let mut constants = Vec::new();

        for line in input.lines() {
            // Remove comments
            let line = if let Some(idx) = line.find('#') {
                &line[..idx]
            } else {
                line
            };

            let line = line.trim();
            if line.is_empty() {
                continue;
            }

            // Check if this is a constant definition (contains '=')
            if line.contains('=') {
                let constant = parse_constant(line)?;
                constants.push(constant);
            } else {
                let field = parse_field(line)?;
                fields.push(field);
            }
        }

        Ok(MessageDefinition { fields, constants })
    }
}

/// Parse a field definition line (e.g., "float64 x" or "std_msgs/Header header")
fn parse_field(line: &str) -> Result<Field, ParseError> {
    // Split on whitespace, taking the first token as type and second as name
    let mut parts = line.split_whitespace();

    let type_str = parts
        .next()
        .ok_or_else(|| ParseError::InvalidField(line.to_string()))?;

    let name = parts
        .next()
        .ok_or_else(|| ParseError::InvalidField(line.to_string()))?;

    // Verify no extra tokens (besides potential default value which we ignore for now)
    // ROS1 doesn't really have default values in .msg, but be lenient

    if name.is_empty() {
        return Err(ParseError::EmptyFieldName);
    }

    let field_type = parse_type(type_str)?;

    Ok(Field {
        name: name.to_string(),
        field_type,
    })
}

/// Parse a type string (e.g., "float64", "uint8[]", "uint8[16]", "std_msgs/Header")
fn parse_type(type_str: &str) -> Result<FieldType, ParseError> {
    if type_str.is_empty() {
        return Err(ParseError::EmptyTypeName);
    }

    // Check for array syntax
    if let Some(bracket_start) = type_str.find('[') {
        let base_type = &type_str[..bracket_start];
        let bracket_content = &type_str[bracket_start..];

        // Parse the array bounds
        if !bracket_content.ends_with(']') {
            return Err(ParseError::InvalidArray(type_str.to_string()));
        }

        let inner = &bracket_content[1..bracket_content.len() - 1];
        let element = Box::new(parse_type(base_type)?);

        if inner.is_empty() {
            // Variable-length array
            Ok(FieldType::Array { element })
        } else {
            // Fixed-length array
            let length: usize = inner
                .parse()
                .map_err(|_| ParseError::InvalidArray(type_str.to_string()))?;
            Ok(FieldType::FixedArray { element, length })
        }
    } else if let Some(primitive) = Primitive::from_str(type_str) {
        Ok(FieldType::Primitive(primitive))
    } else {
        // Nested message type
        parse_nested_type(type_str)
    }
}

/// Parse a nested message type (e.g., "std_msgs/Header" or "Header")
fn parse_nested_type(type_str: &str) -> Result<FieldType, ParseError> {
    if type_str.contains('/') {
        let mut parts = type_str.splitn(2, '/');
        let package = parts.next().unwrap().to_string();
        let name = parts.next().unwrap().to_string();

        if package.is_empty() || name.is_empty() {
            return Err(ParseError::InvalidField(type_str.to_string()));
        }

        Ok(FieldType::Nested {
            package: Some(package),
            name,
        })
    } else {
        Ok(FieldType::Nested {
            package: None,
            name: type_str.to_string(),
        })
    }
}

/// Parse a constant definition (e.g., "int32 FOO=42" or "string BAR=hello world")
fn parse_constant(line: &str) -> Result<Constant, ParseError> {
    // Split on '=' first
    let eq_idx = line
        .find('=')
        .ok_or_else(|| ParseError::InvalidConstant(line.to_string()))?;

    let before_eq = line[..eq_idx].trim();
    let value = line[eq_idx + 1..].trim().to_string();

    // Parse the type and name before '='
    let mut parts = before_eq.split_whitespace();

    let type_str = parts
        .next()
        .ok_or_else(|| ParseError::InvalidConstant(line.to_string()))?;

    let name = parts
        .next()
        .ok_or_else(|| ParseError::InvalidConstant(line.to_string()))?
        .to_string();

    // Constants must be primitive types
    let constant_type = Primitive::from_str(type_str).ok_or_else(|| {
        ParseError::InvalidConstant(format!("non-primitive constant type: {}", type_str))
    })?;

    Ok(Constant {
        name,
        constant_type,
        value,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    mod primitive_parsing {
        use super::*;

        #[test]
        fn test_all_primitives() {
            assert_eq!(Primitive::from_str("bool"), Some(Primitive::Bool));
            assert_eq!(Primitive::from_str("int8"), Some(Primitive::Int8));
            assert_eq!(Primitive::from_str("int16"), Some(Primitive::Int16));
            assert_eq!(Primitive::from_str("int32"), Some(Primitive::Int32));
            assert_eq!(Primitive::from_str("int64"), Some(Primitive::Int64));
            assert_eq!(Primitive::from_str("uint8"), Some(Primitive::UInt8));
            assert_eq!(Primitive::from_str("uint16"), Some(Primitive::UInt16));
            assert_eq!(Primitive::from_str("uint32"), Some(Primitive::UInt32));
            assert_eq!(Primitive::from_str("uint64"), Some(Primitive::UInt64));
            assert_eq!(Primitive::from_str("float32"), Some(Primitive::Float32));
            assert_eq!(Primitive::from_str("float64"), Some(Primitive::Float64));
            assert_eq!(Primitive::from_str("string"), Some(Primitive::String));
            assert_eq!(Primitive::from_str("time"), Some(Primitive::Time));
            assert_eq!(Primitive::from_str("duration"), Some(Primitive::Duration));
        }

        #[test]
        fn test_legacy_aliases() {
            // ROS1 has 'byte' as alias for int8 and 'char' as alias for uint8
            assert_eq!(Primitive::from_str("byte"), Some(Primitive::Int8));
            assert_eq!(Primitive::from_str("char"), Some(Primitive::UInt8));
        }

        #[test]
        fn test_invalid_primitives() {
            assert_eq!(Primitive::from_str("int"), None);
            assert_eq!(Primitive::from_str("float"), None);
            assert_eq!(Primitive::from_str("double"), None);
            assert_eq!(Primitive::from_str(""), None);
            assert_eq!(Primitive::from_str("Int32"), None); // case sensitive
        }
    }

    mod type_parsing {
        use super::*;

        #[test]
        fn test_primitive_type() {
            let ft = parse_type("float64").unwrap();
            assert_eq!(ft, FieldType::Primitive(Primitive::Float64));
        }

        #[test]
        fn test_variable_array() {
            let ft = parse_type("uint8[]").unwrap();
            assert_eq!(
                ft,
                FieldType::Array {
                    element: Box::new(FieldType::Primitive(Primitive::UInt8))
                }
            );
        }

        #[test]
        fn test_fixed_array() {
            let ft = parse_type("float32[3]").unwrap();
            assert_eq!(
                ft,
                FieldType::FixedArray {
                    element: Box::new(FieldType::Primitive(Primitive::Float32)),
                    length: 3
                }
            );
        }

        #[test]
        fn test_large_fixed_array() {
            let ft = parse_type("uint8[1024]").unwrap();
            assert_eq!(
                ft,
                FieldType::FixedArray {
                    element: Box::new(FieldType::Primitive(Primitive::UInt8)),
                    length: 1024
                }
            );
        }

        #[test]
        fn test_nested_with_package() {
            let ft = parse_type("std_msgs/Header").unwrap();
            assert_eq!(
                ft,
                FieldType::Nested {
                    package: Some("std_msgs".to_string()),
                    name: "Header".to_string()
                }
            );
        }

        #[test]
        fn test_nested_without_package() {
            let ft = parse_type("Header").unwrap();
            assert_eq!(
                ft,
                FieldType::Nested {
                    package: None,
                    name: "Header".to_string()
                }
            );
        }

        #[test]
        fn test_nested_array() {
            let ft = parse_type("geometry_msgs/Point[]").unwrap();
            assert_eq!(
                ft,
                FieldType::Array {
                    element: Box::new(FieldType::Nested {
                        package: Some("geometry_msgs".to_string()),
                        name: "Point".to_string()
                    })
                }
            );
        }

        #[test]
        fn test_nested_fixed_array() {
            let ft = parse_type("geometry_msgs/Point[10]").unwrap();
            assert_eq!(
                ft,
                FieldType::FixedArray {
                    element: Box::new(FieldType::Nested {
                        package: Some("geometry_msgs".to_string()),
                        name: "Point".to_string()
                    }),
                    length: 10
                }
            );
        }

        #[test]
        fn test_string_array() {
            let ft = parse_type("string[]").unwrap();
            assert_eq!(
                ft,
                FieldType::Array {
                    element: Box::new(FieldType::Primitive(Primitive::String))
                }
            );
        }

        #[test]
        fn test_invalid_array_syntax() {
            assert!(parse_type("uint8[").is_err());
            assert!(parse_type("uint8[abc]").is_err());
            assert!(parse_type("uint8[-1]").is_err());
        }

        #[test]
        fn test_empty_type() {
            assert!(parse_type("").is_err());
        }
    }

    mod field_parsing {
        use super::*;

        #[test]
        fn test_simple_field() {
            let field = parse_field("float64 x").unwrap();
            assert_eq!(field.name, "x");
            assert_eq!(field.field_type, FieldType::Primitive(Primitive::Float64));
        }

        #[test]
        fn test_field_with_extra_whitespace() {
            let field = parse_field("  float64   position  ").unwrap();
            assert_eq!(field.name, "position");
            assert_eq!(field.field_type, FieldType::Primitive(Primitive::Float64));
        }

        #[test]
        fn test_array_field() {
            let field = parse_field("uint8[] data").unwrap();
            assert_eq!(field.name, "data");
            assert_eq!(
                field.field_type,
                FieldType::Array {
                    element: Box::new(FieldType::Primitive(Primitive::UInt8))
                }
            );
        }

        #[test]
        fn test_nested_field() {
            let field = parse_field("std_msgs/Header header").unwrap();
            assert_eq!(field.name, "header");
            assert_eq!(
                field.field_type,
                FieldType::Nested {
                    package: Some("std_msgs".to_string()),
                    name: "Header".to_string()
                }
            );
        }

        #[test]
        fn test_missing_name() {
            assert!(parse_field("float64").is_err());
        }

        #[test]
        fn test_missing_type() {
            assert!(parse_field("x").is_err());
        }
    }

    mod constant_parsing {
        use super::*;

        #[test]
        fn test_integer_constant() {
            let c = parse_constant("int32 FOO=42").unwrap();
            assert_eq!(c.name, "FOO");
            assert_eq!(c.constant_type, Primitive::Int32);
            assert_eq!(c.value, "42");
        }

        #[test]
        fn test_negative_constant() {
            let c = parse_constant("int32 NEG=-100").unwrap();
            assert_eq!(c.name, "NEG");
            assert_eq!(c.constant_type, Primitive::Int32);
            assert_eq!(c.value, "-100");
        }

        #[test]
        fn test_string_constant() {
            let c = parse_constant("string GREETING=hello world").unwrap();
            assert_eq!(c.name, "GREETING");
            assert_eq!(c.constant_type, Primitive::String);
            assert_eq!(c.value, "hello world");
        }

        #[test]
        fn test_string_constant_with_equals() {
            // String values can contain '=' signs
            let c = parse_constant("string EQ=a=b=c").unwrap();
            assert_eq!(c.name, "EQ");
            assert_eq!(c.value, "a=b=c");
        }

        #[test]
        fn test_float_constant() {
            let c = parse_constant("float64 PI=3.14159").unwrap();
            assert_eq!(c.name, "PI");
            assert_eq!(c.constant_type, Primitive::Float64);
            assert_eq!(c.value, "3.14159");
        }

        #[test]
        fn test_constant_with_spaces() {
            let c = parse_constant("  int32  VALUE  =  123  ").unwrap();
            assert_eq!(c.name, "VALUE");
            assert_eq!(c.value, "123");
        }
    }

    mod message_definition_parsing {
        use super::*;

        #[test]
        fn test_empty_definition() {
            let def = MessageDefinition::parse("").unwrap();
            assert!(def.fields.is_empty());
            assert!(def.constants.is_empty());
        }

        #[test]
        fn test_comments_only() {
            let def = MessageDefinition::parse("# This is a comment\n# Another comment").unwrap();
            assert!(def.fields.is_empty());
            assert!(def.constants.is_empty());
        }

        #[test]
        fn test_simple_message() {
            let input = r#"
                float64 x
                float64 y
                float64 z
            "#;
            let def = MessageDefinition::parse(input).unwrap();
            assert_eq!(def.fields.len(), 3);
            assert_eq!(def.fields[0].name, "x");
            assert_eq!(def.fields[1].name, "y");
            assert_eq!(def.fields[2].name, "z");
        }

        #[test]
        fn test_message_with_comments() {
            let input = r#"
                # Position in 3D space
                float64 x  # X coordinate
                float64 y  # Y coordinate
                float64 z  # Z coordinate
            "#;
            let def = MessageDefinition::parse(input).unwrap();
            assert_eq!(def.fields.len(), 3);
        }

        #[test]
        fn test_message_with_constants() {
            let input = r#"
                int32 TYPE_A=1
                int32 TYPE_B=2
                int32 type
            "#;
            let def = MessageDefinition::parse(input).unwrap();
            assert_eq!(def.constants.len(), 2);
            assert_eq!(def.fields.len(), 1);
            assert_eq!(def.constants[0].name, "TYPE_A");
            assert_eq!(def.constants[0].value, "1");
            assert_eq!(def.constants[1].name, "TYPE_B");
            assert_eq!(def.constants[1].value, "2");
        }

        #[test]
        fn test_std_msgs_header() {
            // Real-world: std_msgs/Header
            let input = r#"
                # Standard metadata for higher-level stamped data types.
                uint32 seq
                time stamp
                string frame_id
            "#;
            let def = MessageDefinition::parse(input).unwrap();
            assert_eq!(def.fields.len(), 3);
            assert_eq!(def.fields[0].name, "seq");
            assert_eq!(
                def.fields[0].field_type,
                FieldType::Primitive(Primitive::UInt32)
            );
            assert_eq!(def.fields[1].name, "stamp");
            assert_eq!(
                def.fields[1].field_type,
                FieldType::Primitive(Primitive::Time)
            );
            assert_eq!(def.fields[2].name, "frame_id");
            assert_eq!(
                def.fields[2].field_type,
                FieldType::Primitive(Primitive::String)
            );
        }

        #[test]
        fn test_sensor_msgs_image() {
            // Real-world: sensor_msgs/Image (simplified)
            let input = r#"
                std_msgs/Header header
                uint32 height
                uint32 width
                string encoding
                uint8 is_bigendian
                uint32 step
                uint8[] data
            "#;
            let def = MessageDefinition::parse(input).unwrap();
            assert_eq!(def.fields.len(), 7);
            assert_eq!(
                def.fields[0].field_type,
                FieldType::Nested {
                    package: Some("std_msgs".to_string()),
                    name: "Header".to_string()
                }
            );
            assert_eq!(
                def.fields[6].field_type,
                FieldType::Array {
                    element: Box::new(FieldType::Primitive(Primitive::UInt8))
                }
            );
        }

        #[test]
        fn test_geometry_msgs_pose() {
            // Real-world: geometry_msgs/Pose
            let input = r#"
                geometry_msgs/Point position
                geometry_msgs/Quaternion orientation
            "#;
            let def = MessageDefinition::parse(input).unwrap();
            assert_eq!(def.fields.len(), 2);
            assert_eq!(def.fields[0].name, "position");
            assert_eq!(def.fields[1].name, "orientation");
        }

        #[test]
        fn test_nav_msgs_path() {
            // Real-world: nav_msgs/Path
            let input = r#"
                std_msgs/Header header
                geometry_msgs/PoseStamped[] poses
            "#;
            let def = MessageDefinition::parse(input).unwrap();
            assert_eq!(def.fields.len(), 2);
            assert_eq!(
                def.fields[1].field_type,
                FieldType::Array {
                    element: Box::new(FieldType::Nested {
                        package: Some("geometry_msgs".to_string()),
                        name: "PoseStamped".to_string()
                    })
                }
            );
        }

        #[test]
        fn test_sensor_msgs_pointcloud2() {
            // Real-world: sensor_msgs/PointCloud2 (simplified)
            let input = r#"
                std_msgs/Header header
                uint32 height
                uint32 width
                sensor_msgs/PointField[] fields
                bool is_bigendian
                uint32 point_step
                uint32 row_step
                uint8[] data
                bool is_dense
            "#;
            let def = MessageDefinition::parse(input).unwrap();
            assert_eq!(def.fields.len(), 9);
        }

        #[test]
        fn test_fixed_size_arrays() {
            let input = r#"
                float64[3] position
                float64[4] orientation
                float64[36] covariance
            "#;
            let def = MessageDefinition::parse(input).unwrap();
            assert_eq!(def.fields.len(), 3);
            assert_eq!(
                def.fields[0].field_type,
                FieldType::FixedArray {
                    element: Box::new(FieldType::Primitive(Primitive::Float64)),
                    length: 3
                }
            );
            assert_eq!(
                def.fields[2].field_type,
                FieldType::FixedArray {
                    element: Box::new(FieldType::Primitive(Primitive::Float64)),
                    length: 36
                }
            );
        }

        #[test]
        fn test_actionlib_goal_status() {
            // Real-world: actionlib_msgs/GoalStatus
            let input = r#"
                uint8 PENDING=0
                uint8 ACTIVE=1
                uint8 PREEMPTED=2
                uint8 SUCCEEDED=3
                uint8 ABORTED=4
                uint8 REJECTED=5
                uint8 PREEMPTING=6
                uint8 RECALLING=7
                uint8 RECALLED=8
                uint8 LOST=9

                actionlib_msgs/GoalID goal_id
                uint8 status
                string text
            "#;
            let def = MessageDefinition::parse(input).unwrap();
            assert_eq!(def.constants.len(), 10);
            assert_eq!(def.fields.len(), 3);
            assert_eq!(def.constants[0].name, "PENDING");
            assert_eq!(def.constants[0].value, "0");
            assert_eq!(def.constants[9].name, "LOST");
            assert_eq!(def.constants[9].value, "9");
        }

        #[test]
        fn test_legacy_byte_and_char() {
            let input = r#"
                byte old_byte
                char old_char
            "#;
            let def = MessageDefinition::parse(input).unwrap();
            assert_eq!(def.fields.len(), 2);
            assert_eq!(
                def.fields[0].field_type,
                FieldType::Primitive(Primitive::Int8)
            );
            assert_eq!(
                def.fields[1].field_type,
                FieldType::Primitive(Primitive::UInt8)
            );
        }
    }

    mod display {
        use super::*;

        #[test]
        fn test_primitive_display() {
            assert_eq!(format!("{}", Primitive::Float64), "float64");
            assert_eq!(format!("{}", Primitive::String), "string");
            assert_eq!(format!("{}", Primitive::Time), "time");
        }

        #[test]
        fn test_field_type_display() {
            assert_eq!(
                format!("{}", FieldType::Primitive(Primitive::Float64)),
                "float64"
            );
            assert_eq!(
                format!(
                    "{}",
                    FieldType::Array {
                        element: Box::new(FieldType::Primitive(Primitive::UInt8))
                    }
                ),
                "uint8[]"
            );
            assert_eq!(
                format!(
                    "{}",
                    FieldType::FixedArray {
                        element: Box::new(FieldType::Primitive(Primitive::Float32)),
                        length: 3
                    }
                ),
                "float32[3]"
            );
            assert_eq!(
                format!(
                    "{}",
                    FieldType::Nested {
                        package: Some("std_msgs".to_string()),
                        name: "Header".to_string()
                    }
                ),
                "std_msgs/Header"
            );
            assert_eq!(
                format!(
                    "{}",
                    FieldType::Nested {
                        package: None,
                        name: "Header".to_string()
                    }
                ),
                "Header"
            );
        }
    }
}
