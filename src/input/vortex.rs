//! Vortex input format implementation.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use vortex::array::arrow::IntoArrowArray;
use vortex::session::VortexSession;
use vortex::VortexSessionDefault;
use vortex_file::OpenOptionsSessionExt;
use vortex_io::runtime::single::SingleThreadRuntime;
use vortex_io::runtime::BlockingRuntime;
use vortex_io::session::RuntimeSessionExt;

use super::{InputDatabase, InputError, TableReader};

/// Vortex input database.
///
/// Reads from a directory containing one Vortex file per table.
pub struct VortexInput {
    /// Map of table name to file path
    tables: HashMap<String, PathBuf>,
}

impl VortexInput {
    /// Create a new Vortex input from the given directory path.
    pub fn new(path: &Path) -> Result<Self, InputError> {
        let mut tables = HashMap::new();

        // Scan directory for .vortex files
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let file_path = entry.path();

            if file_path
                .extension()
                .map(|e| e == "vortex")
                .unwrap_or(false)
            {
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

impl InputDatabase for VortexInput {
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

        let reader = VortexTableReader::new(path)?;
        Ok(Box::new(reader))
    }
}

/// Reader for a single Vortex file.
///
/// Vortex files contain a single array, so we read it all at once
/// and return it as a single batch.
pub struct VortexTableReader {
    schema: Arc<Schema>,
    /// The batch to return (consumed on first read)
    batch: Option<RecordBatch>,
}

impl VortexTableReader {
    fn new(path: &Path) -> Result<Self, InputError> {
        // Set up vortex runtime and session
        let runtime = SingleThreadRuntime::default();
        let handle = runtime.handle();
        let session = VortexSession::default().with_handle(handle);

        // Open the vortex file (pass path directly, not File)
        let vortex_file = runtime
            .block_on(session.open_options().open(path))
            .map_err(|e| InputError::Vortex(e.to_string()))?;

        // Use into_iter to read all arrays
        let iter = vortex_file
            .scan()
            .map_err(|e| InputError::Vortex(e.to_string()))?
            .into_iter(&runtime)
            .map_err(|e| InputError::Vortex(e.to_string()))?;

        // Collect all arrays and convert to Arrow
        let mut batches = Vec::new();
        for array_result in iter {
            let vortex_array = array_result.map_err(|e| InputError::Vortex(e.to_string()))?;
            // Convert to Arrow using preferred data type
            let arrow_array = vortex_array
                .into_arrow_preferred()
                .map_err(|e: vortex::error::VortexError| InputError::Vortex(e.to_string()))?;

            // Convert StructArray to RecordBatch
            if let Some(struct_array) = arrow_array
                .as_any()
                .downcast_ref::<arrow::array::StructArray>()
            {
                batches.push(RecordBatch::from(struct_array));
            } else {
                return Err(InputError::Vortex(
                    "Expected struct array at top level".to_string(),
                ));
            }
        }

        if batches.is_empty() {
            return Err(InputError::Vortex("No data in file".to_string()));
        }

        let schema = batches[0].schema();
        let batch = if batches.len() == 1 {
            batches.pop().unwrap()
        } else {
            // Concatenate all batches
            arrow::compute::concat_batches(&schema, &batches)?
        };

        Ok(Self {
            schema,
            batch: Some(batch),
        })
    }
}

impl TableReader for VortexTableReader {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>, InputError> {
        // Return the batch once, then None
        Ok(self.batch.take())
    }
}
