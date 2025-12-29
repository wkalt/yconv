//! Parquet input format implementation.

use std::collections::HashMap;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use super::{InputDatabase, InputError, TableReader};

/// Parquet input database.
///
/// Reads from a directory containing one Parquet file per table.
pub struct ParquetInput {
    /// Map of table name to file path
    tables: HashMap<String, PathBuf>,
}

impl ParquetInput {
    /// Create a new Parquet input from the given directory path.
    pub fn new(path: &Path) -> Result<Self, InputError> {
        let mut tables = HashMap::new();

        // Scan directory for .parquet files
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let file_path = entry.path();

            if file_path.extension().map(|e| e == "parquet").unwrap_or(false) {
                // Extract table name from filename (without extension)
                if let Some(stem) = file_path.file_stem() {
                    let table_name = stem.to_string_lossy().to_string();
                    tables.insert(table_name, file_path);
                }
            }
        }

        if tables.is_empty() {
            return Err(InputError::NoTables);
        }

        Ok(Self { tables })
    }
}

impl InputDatabase for ParquetInput {
    fn table_names(&self) -> Result<Vec<String>, InputError> {
        let mut names: Vec<_> = self.tables.keys().cloned().collect();
        names.sort();
        Ok(names)
    }

    fn open_table(&mut self, name: &str) -> Result<Box<dyn TableReader>, InputError> {
        let path = self
            .tables
            .get(name)
            .ok_or_else(|| InputError::TableNotFound(name.to_string()))?;

        let reader = ParquetTableReader::new(path)?;
        Ok(Box::new(reader))
    }
}

/// Reader for a single Parquet file.
pub struct ParquetTableReader {
    schema: Arc<Schema>,
    reader: parquet::arrow::arrow_reader::ParquetRecordBatchReader,
}

impl ParquetTableReader {
    fn new(path: &Path) -> Result<Self, InputError> {
        let file = File::open(path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let schema = builder.schema().clone();
        let reader = builder.with_batch_size(1000).build()?;

        Ok(Self { schema, reader })
    }
}

impl TableReader for ParquetTableReader {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>, InputError> {
        match self.reader.next() {
            Some(Ok(batch)) => Ok(Some(batch)),
            Some(Err(e)) => Err(InputError::Arrow(e)),
            None => Ok(None),
        }
    }
}
