//! Input format abstraction for reading data from various columnar formats.
//!
//! This module provides a common interface for reading Arrow RecordBatches
//! from different input formats (Parquet, Vortex, DuckDB).

pub mod duckdb;
pub mod parquet;
pub mod vortex;

use std::path::Path;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use thiserror::Error;

/// Errors that can occur during input operations.
#[derive(Debug, Error)]
pub enum InputError {
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Parquet error: {0}")]
    Parquet(#[from] ::parquet::errors::ParquetError),

    #[error("Vortex error: {0}")]
    Vortex(String),

    #[error("table '{0}' not found")]
    TableNotFound(String),

    #[error("no tables found in input")]
    NoTables,
}

/// Reader for a single table's data.
///
/// Implementations handle reading batches from the specific input format.
pub trait TableReader: Send {
    /// Get the schema of this table.
    fn schema(&self) -> Arc<Schema>;

    /// Read the next batch from this table.
    /// Returns None when all data has been read.
    fn next_batch(&mut self) -> Result<Option<RecordBatch>, InputError>;
}

/// Factory for creating table readers for a specific input format.
///
/// Each input format (Parquet, Vortex) implements this trait to
/// provide access to tables within the input.
pub trait InputDatabase: Send {
    /// List all table names in this input.
    fn table_names(&self) -> Result<Vec<String>, InputError>;

    /// Open a table for reading.
    fn open_table(&mut self, name: &str) -> Result<Box<dyn TableReader>, InputError>;
}

/// Supported input formats.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputFormat {
    Parquet,
    Vortex,
    DuckDb,
}

/// Open an input database for the given format and path.
pub fn open_input(
    format: InputFormat,
    path: &Path,
) -> Result<Box<dyn InputDatabase>, InputError> {
    match format {
        InputFormat::Parquet => Ok(Box::new(parquet::ParquetInput::new(path)?)),
        InputFormat::Vortex => Ok(Box::new(vortex::VortexInput::new(path)?)),
        InputFormat::DuckDb => Ok(Box::new(duckdb::DuckDbInput::new(path)?)),
    }
}
