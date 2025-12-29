//! Protobuf message encoding support for MCAP conversion.
//!
//! This module provides transcoding between protobuf wire format and Arrow,
//! enabling full support for protobuf-encoded MCAP files.

mod schema;
mod transcoder;
mod writer;

pub use schema::{ProtobufSchema, ProtobufSchemaError};
pub use transcoder::{transcode, TranscodeError};
pub use writer::{ProtobufWriter, WriteError};
