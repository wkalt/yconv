//! DuckDB output format for writing MCAP data.
//!
//! Creates a DuckDB database file with one table per topic.

use std::path::Path;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use duckdb::Connection;

use super::{OutputDatabase, OutputError, TopicWriter};

/// DuckDB output database.
pub struct DuckDbOutput {
    conn: Connection,
}

impl DuckDbOutput {
    /// Create a new DuckDB output at the given path.
    pub fn new(path: &Path) -> Result<Self, OutputError> {
        // Remove existing file if present (overwrite mode)
        if path.exists() {
            std::fs::remove_file(path)?;
        }

        let conn = Connection::open(path).map_err(|e| OutputError::DuckDb(e.to_string()))?;

        Ok(Self { conn })
    }
}

impl OutputDatabase for DuckDbOutput {
    fn create_topic_writer(
        &mut self,
        topic: &str,
        _schema: Arc<Schema>,
    ) -> Result<Box<dyn TopicWriter>, OutputError> {
        // Sanitize topic name for use as table name
        let table_name = sanitize_table_name(topic);

        Ok(Box::new(DuckDbTopicWriter {
            conn: self
                .conn
                .try_clone()
                .map_err(|e| OutputError::DuckDb(e.to_string()))?,
            table_name,
            table_created: false,
        }))
    }

    fn finish(self: Box<Self>) -> Result<(), OutputError> {
        // Connection closes automatically on drop
        Ok(())
    }
}

/// Writer for a single topic to DuckDB.
pub struct DuckDbTopicWriter {
    conn: Connection,
    table_name: String,
    table_created: bool,
}

impl TopicWriter for DuckDbTopicWriter {
    fn write_batch(&mut self, batch: RecordBatch) -> Result<(), OutputError> {
        if !self.table_created {
            // Create table from first batch using Arrow schema
            self.create_table(&batch)?;
            self.table_created = true;
        }

        // Insert the batch
        self.insert_batch(&batch)?;

        Ok(())
    }

    fn finish(self: Box<Self>) -> Result<(), OutputError> {
        // Nothing special to do - data is already written
        Ok(())
    }
}

impl DuckDbTopicWriter {
    /// Create the table from the Arrow schema.
    fn create_table(&self, batch: &RecordBatch) -> Result<(), OutputError> {
        // Use DuckDB's ability to create table from Arrow data
        // First, create an empty table with the right schema
        let create_sql = arrow_schema_to_create_table(&self.table_name, batch.schema().as_ref());
        self.conn
            .execute(&create_sql, [])
            .map_err(|e| OutputError::DuckDb(format!("Failed to create table: {}", e)))?;

        Ok(())
    }

    /// Insert a RecordBatch into the table.
    fn insert_batch(&self, batch: &RecordBatch) -> Result<(), OutputError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        // Use DuckDB's Arrow appender for efficient bulk insert
        let mut appender = self
            .conn
            .appender(&self.table_name)
            .map_err(|e| OutputError::DuckDb(format!("Failed to create appender: {}", e)))?;

        appender
            .append_record_batch(batch.clone())
            .map_err(|e| OutputError::DuckDb(format!("Failed to append batch: {}", e)))?;

        Ok(())
    }
}

/// Convert topic name to valid SQL table name.
fn sanitize_table_name(topic: &str) -> String {
    topic
        .trim_start_matches('/')
        .replace(['/', '-', '.'], "_")
}

/// Generate CREATE TABLE SQL from Arrow schema.
fn arrow_schema_to_create_table(table_name: &str, schema: &Schema) -> String {
    let columns: Vec<String> = schema
        .fields()
        .iter()
        .map(|field| {
            let sql_type = arrow_type_to_duckdb(field.data_type());
            format!("\"{}\" {}", field.name(), sql_type)
        })
        .collect();

    format!("CREATE TABLE \"{}\" ({})", table_name, columns.join(", "))
}

/// Convert Arrow DataType to DuckDB SQL type.
fn arrow_type_to_duckdb(dt: &arrow::datatypes::DataType) -> &'static str {
    use arrow::datatypes::DataType;

    match dt {
        DataType::Boolean => "BOOLEAN",
        DataType::Int8 => "TINYINT",
        DataType::Int16 => "SMALLINT",
        DataType::Int32 => "INTEGER",
        DataType::Int64 => "BIGINT",
        DataType::UInt8 => "UTINYINT",
        DataType::UInt16 => "USMALLINT",
        DataType::UInt32 => "UINTEGER",
        DataType::UInt64 => "UBIGINT",
        DataType::Float32 => "FLOAT",
        DataType::Float64 => "DOUBLE",
        DataType::Utf8 | DataType::LargeUtf8 => "VARCHAR",
        DataType::Binary | DataType::LargeBinary => "BLOB",
        DataType::Timestamp(_, _) => "TIMESTAMP",
        DataType::Duration(_) => "BIGINT", // Store as nanoseconds
        DataType::Date32 | DataType::Date64 => "DATE",
        DataType::Time32(_) | DataType::Time64(_) => "TIME",
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => "JSON", // DuckDB handles lists via JSON or native lists
        DataType::Struct(_) => "JSON", // Nested structs as JSON
        _ => "VARCHAR",                // Fallback
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int32Array, StringArray};
    use arrow::datatypes::Field;
    use tempfile::tempdir;

    #[test]
    fn test_sanitize_table_name() {
        assert_eq!(sanitize_table_name("/camera/image_raw"), "camera_image_raw");
        assert_eq!(sanitize_table_name("/tf"), "tf");
        assert_eq!(sanitize_table_name("topic.name"), "topic_name");
    }

    #[test]
    fn test_write_simple_batch() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.duckdb");

        let schema = Arc::new(Schema::new(vec![
            Field::new("x", arrow::datatypes::DataType::Float64, false),
            Field::new("y", arrow::datatypes::DataType::Float64, false),
            Field::new("name", arrow::datatypes::DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])),
                Arc::new(Float64Array::from(vec![4.0, 5.0, 6.0])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();

        let mut output = DuckDbOutput::new(&db_path).unwrap();
        let mut writer = output.create_topic_writer("/test/topic", schema).unwrap();
        writer.write_batch(batch).unwrap();
        writer.finish().unwrap();
        Box::new(output).finish().unwrap();

        // Verify by reading back
        let conn = Connection::open(&db_path).unwrap();
        let mut stmt = conn.prepare("SELECT COUNT(*) FROM test_topic").unwrap();
        let count: i64 = stmt.query_row([], |row| row.get(0)).unwrap();
        assert_eq!(count, 3);
    }
}
