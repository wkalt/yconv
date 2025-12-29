//! Output format abstraction for writing MCAP data to various formats.
//!
//! This module provides a common interface for writing Arrow RecordBatches
//! to different output formats (LanceDB, Parquet, Vortex, DuckDB).

pub mod duckdb;
pub mod parquet;
pub mod vortex;

use std::path::Path;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use thiserror::Error;

/// Errors that can occur during output operations.
#[derive(Debug, Error)]
pub enum OutputError {
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Parquet error: {0}")]
    Parquet(#[from] ::parquet::errors::ParquetError),

    #[error("Vortex error: {0}")]
    Vortex(String),

    #[error("DuckDB error: {0}")]
    DuckDb(String),

    #[error("topic '{0}' already exists")]
    TopicExists(String),
}

/// Writer for a single topic's data.
///
/// Implementations handle batching and writing to the specific output format.
pub trait TopicWriter: Send {
    /// Write a RecordBatch to this topic.
    fn write_batch(&mut self, batch: RecordBatch) -> Result<(), OutputError>;

    /// Finish writing and flush any pending data.
    fn finish(self: Box<Self>) -> Result<(), OutputError>;
}

/// Factory for creating topic writers for a specific output format.
///
/// Each output format (Parquet, Vortex, etc.) implements this trait to
/// create writers for individual topics.
pub trait OutputDatabase: Send {
    /// Create a writer for a topic with the given schema.
    ///
    /// The schema should include metadata columns (e.g., `_log_time`).
    fn create_topic_writer(
        &mut self,
        topic: &str,
        schema: Arc<Schema>,
    ) -> Result<Box<dyn TopicWriter>, OutputError>;

    /// Finish all writes and close the database.
    fn finish(self: Box<Self>) -> Result<(), OutputError>;
}

/// Create an output database for the given format and path.
pub fn create_output(
    format: OutputFormat,
    path: &Path,
) -> Result<Box<dyn OutputDatabase>, OutputError> {
    match format {
        OutputFormat::Parquet => Ok(Box::new(parquet::ParquetOutput::new(path)?)),
        OutputFormat::Vortex => Ok(Box::new(vortex::VortexOutput::new(path)?)),
        OutputFormat::DuckDb => Ok(Box::new(duckdb::DuckDbOutput::new(path)?)),
    }
}

/// Supported output formats (excluding LanceDB which has its own async path).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputFormat {
    Parquet,
    Vortex,
    DuckDb,
}
