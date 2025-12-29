//! Parquet output format implementation.

use std::collections::HashSet;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

use super::{OutputDatabase, OutputError, TopicWriter};
use crate::lance::sanitize_table_name;

/// Parquet output database.
///
/// Creates one Parquet file per topic in the output directory.
pub struct ParquetOutput {
    output_dir: PathBuf,
    created_topics: HashSet<String>,
}

impl ParquetOutput {
    /// Create a new Parquet output at the given directory path.
    pub fn new(path: &Path) -> Result<Self, OutputError> {
        // Create output directory if it doesn't exist
        fs::create_dir_all(path)?;

        Ok(Self {
            output_dir: path.to_path_buf(),
            created_topics: HashSet::new(),
        })
    }
}

impl OutputDatabase for ParquetOutput {
    fn create_topic_writer(
        &mut self,
        topic: &str,
        schema: Arc<Schema>,
    ) -> Result<Box<dyn TopicWriter>, OutputError> {
        let table_name = sanitize_table_name(topic);

        if self.created_topics.contains(&table_name) {
            return Err(OutputError::TopicExists(topic.to_string()));
        }

        let file_path = self.output_dir.join(format!("{}.parquet", table_name));
        let writer = ParquetTopicWriter::new(&file_path, schema)?;

        self.created_topics.insert(table_name);
        Ok(Box::new(writer))
    }

    fn finish(self: Box<Self>) -> Result<(), OutputError> {
        // Nothing to do - individual writers handle their own cleanup
        Ok(())
    }
}

/// Writer for a single topic to a Parquet file.
pub struct ParquetTopicWriter {
    writer: ArrowWriter<File>,
}

impl ParquetTopicWriter {
    fn new(path: &Path, schema: Arc<Schema>) -> Result<Self, OutputError> {
        let file = File::create(path)?;

        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(Default::default()))
            .build();

        let writer = ArrowWriter::try_new(file, schema, Some(props))?;

        Ok(Self { writer })
    }
}

impl TopicWriter for ParquetTopicWriter {
    fn write_batch(&mut self, batch: RecordBatch) -> Result<(), OutputError> {
        self.writer.write(&batch)?;
        Ok(())
    }

    fn finish(self: Box<Self>) -> Result<(), OutputError> {
        self.writer.close()?;
        Ok(())
    }
}
