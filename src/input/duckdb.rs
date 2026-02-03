//! DuckDB input format for reading data.
//!
//! Reads tables from a DuckDB database file.

use std::path::Path;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use duckdb::Connection;

use super::{InputDatabase, InputError, TableReader};

/// DuckDB input database.
pub struct DuckDbInput {
    conn: Connection,
}

impl DuckDbInput {
    /// Open a DuckDB database for reading.
    pub fn new(path: &Path) -> Result<Self, InputError> {
        let conn = Connection::open(path)
            .map_err(|e| InputError::Io(std::io::Error::other(e)))?;
        Ok(Self { conn })
    }
}

impl InputDatabase for DuckDbInput {
    fn table_names(&self) -> Result<Vec<String>, InputError> {
        let mut stmt = self
            .conn
            .prepare("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'")
            .map_err(|e| InputError::Io(std::io::Error::other(e)))?;

        let names: Vec<String> = stmt
            .query_map([], |row| row.get(0))
            .map_err(|e| InputError::Io(std::io::Error::other(e)))?
            .filter_map(|r| r.ok())
            .collect();

        if names.is_empty() {
            return Err(InputError::NoTables);
        }

        Ok(names)
    }

    fn open_table(&mut self, name: &str) -> Result<Box<dyn TableReader>, InputError> {
        // Query all data from the table as Arrow
        let query = format!("SELECT * FROM \"{}\"", name);
        let mut stmt = self
            .conn
            .prepare(&query)
            .map_err(|e| InputError::Io(std::io::Error::other(e)))?;

        let arrow_result = stmt
            .query_arrow([])
            .map_err(|e| InputError::Io(std::io::Error::other(e)))?;

        // Collect all batches (DuckDB streams them)
        let batches: Vec<RecordBatch> = arrow_result.collect();

        if batches.is_empty() {
            return Err(InputError::TableNotFound(name.to_string()));
        }

        let schema = batches[0].schema();

        Ok(Box::new(DuckDbTableReader {
            schema,
            batches,
            index: 0,
        }))
    }
}

/// Reader for a single DuckDB table.
pub struct DuckDbTableReader {
    schema: Arc<Schema>,
    batches: Vec<RecordBatch>,
    index: usize,
}

impl TableReader for DuckDbTableReader {
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>, InputError> {
        if self.index >= self.batches.len() {
            return Ok(None);
        }

        let batch = self.batches[self.index].clone();
        self.index += 1;
        Ok(Some(batch))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, StringArray};
    use arrow::datatypes::{DataType, Field};
    use tempfile::tempdir;

    #[test]
    fn test_read_duckdb() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.duckdb");

        // Create a test database
        {
            let conn = Connection::open(&db_path).unwrap();
            conn.execute(
                "CREATE TABLE test_table (x DOUBLE, y DOUBLE, name VARCHAR)",
                [],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO test_table VALUES (1.0, 2.0, 'a'), (3.0, 4.0, 'b')",
                [],
            )
            .unwrap();
        }

        // Read it back
        let mut input = DuckDbInput::new(&db_path).unwrap();

        let tables = input.table_names().unwrap();
        assert_eq!(tables, vec!["test_table"]);

        let mut reader = input.open_table("test_table").unwrap();
        let schema = reader.schema();
        assert_eq!(schema.fields().len(), 3);

        let batch = reader.next_batch().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2);

        assert!(reader.next_batch().unwrap().is_none());
    }
}
