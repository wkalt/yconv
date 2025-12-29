//! CDR (Common Data Representation) deserialization for ROS2 messages.
//!
//! CDR is the serialization format used by DDS/ROS2. Key differences from ROS1:
//! - Alignment: primitives are aligned to their natural boundaries (2/4/8 bytes)
//! - Encapsulation header: 4-byte header at start of message
//! - Strings: length includes null terminator

mod deserializer;
mod transcoder;

// Re-export deserializer types
pub use deserializer::{
    deserialize, deserialize_no_header, deserialize_simple, CdrCursor, CdrDeserializeError,
    CdrTypeRegistry, CdrValue,
};

// Re-export transcoder (only the transcode function and error type)
pub use transcoder::{transcode, CdrTranscodeError};
