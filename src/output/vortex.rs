//! Vortex output format implementation.

use std::collections::HashSet;
use std::fs::{self, File};
use std::io::BufWriter;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use arrow::array::{Array, AsArray, RecordBatch};
use arrow::datatypes::{DataType, Field, Int64Type, Schema};
use vortex::arrow::FromArrowArray;
use vortex::session::VortexSession;
use vortex::ArrayRef;
use vortex::VortexSessionDefault;
use vortex_file::VortexWriteOptions;
use vortex_io::runtime::single::SingleThreadRuntime;
use vortex_io::runtime::BlockingRuntime;
use vortex_io::session::RuntimeSessionExt;

use super::{OutputDatabase, OutputError, TopicWriter};
use crate::lance::sanitize_table_name;

/// Vortex output database.
///
/// Creates one Vortex file per topic in the output directory.
pub struct VortexOutput {
    output_dir: PathBuf,
    created_topics: HashSet<String>,
}

impl VortexOutput {
    /// Create a new Vortex output at the given directory path.
    pub fn new(path: &Path) -> Result<Self, OutputError> {
        // Create output directory if it doesn't exist
        fs::create_dir_all(path)?;

        Ok(Self {
            output_dir: path.to_path_buf(),
            created_topics: HashSet::new(),
        })
    }
}

impl OutputDatabase for VortexOutput {
    fn create_topic_writer(
        &mut self,
        topic: &str,
        schema: Arc<Schema>,
    ) -> Result<Box<dyn TopicWriter>, OutputError> {
        let table_name = sanitize_table_name(topic);

        if self.created_topics.contains(&table_name) {
            return Err(OutputError::TopicExists(topic.to_string()));
        }

        let file_path = self.output_dir.join(format!("{}.vortex", table_name));
        let writer = VortexTopicWriter::new(file_path, schema)?;

        self.created_topics.insert(table_name);
        Ok(Box::new(writer))
    }

    fn finish(self: Box<Self>) -> Result<(), OutputError> {
        Ok(())
    }
}

/// Message sent to the writer thread.
enum WriterMessage {
    /// Write a vortex array to the file.
    Write(ArrayRef),
    /// Finish writing and close the file.
    Finish,
}

/// Writer for a single topic to a Vortex file.
///
/// Uses a background thread for actual I/O since vortex's blocking runtime
/// is not Send-safe. Batches are converted to vortex arrays on the calling
/// thread and sent to the writer thread via a channel.
pub struct VortexTopicWriter {
    /// Channel to send arrays to the writer thread.
    sender: Option<Sender<WriterMessage>>,
    /// Handle to the writer thread.
    writer_thread: Option<JoinHandle<Result<(), String>>>,
    /// The converted schema (with Duration -> Int64) for validation.
    converted_schema: Option<Arc<Schema>>,
    /// Path for error messages.
    path: PathBuf,
}

impl VortexTopicWriter {
    fn new(path: PathBuf, _schema: Arc<Schema>) -> Result<Self, OutputError> {
        let (sender, receiver) = mpsc::channel::<WriterMessage>();

        let writer_path = path.clone();
        let writer_thread = thread::spawn(move || writer_thread_main(writer_path, receiver));

        Ok(Self {
            sender: Some(sender),
            writer_thread: Some(writer_thread),
            converted_schema: None,
            path,
        })
    }
}

/// Main function for the writer thread.
fn writer_thread_main(path: PathBuf, receiver: Receiver<WriterMessage>) -> Result<(), String> {
    let file =
        File::create(&path).map_err(|e| format!("Failed to create {}: {}", path.display(), e))?;
    let buf_writer = BufWriter::new(file);

    let runtime = SingleThreadRuntime::default();
    let handle = runtime.handle();
    let session = VortexSession::default().with_handle(handle);

    // We need to receive the first array to get the dtype
    let first_msg = receiver
        .recv()
        .map_err(|_| "Channel closed before receiving first array")?;

    let first_array = match first_msg {
        WriterMessage::Write(arr) => arr,
        WriterMessage::Finish => return Ok(()), // No data, nothing to write
    };

    let dtype = first_array.dtype().clone();
    let mut writer = VortexWriteOptions::new(session)
        .blocking(&runtime)
        .writer(buf_writer, dtype);

    // Write the first array
    writer
        .push(first_array)
        .map_err(|e| format!("Failed to write array: {}", e))?;

    // Process remaining messages
    loop {
        match receiver.recv() {
            Ok(WriterMessage::Write(array)) => {
                writer
                    .push(array)
                    .map_err(|e| format!("Failed to write array: {}", e))?;
            }
            Ok(WriterMessage::Finish) => {
                break;
            }
            Err(_) => {
                // Channel closed, finish up
                break;
            }
        }
    }

    writer
        .finish()
        .map_err(|e| format!("Failed to finish vortex file: {}", e))?;

    Ok(())
}

/// Recursively convert Duration types to Int64 (nanoseconds) since Vortex doesn't support Duration.
fn convert_duration_field(field: &Field) -> Field {
    let new_type = convert_duration_type(field.data_type());
    Field::new(field.name(), new_type, field.is_nullable()).with_metadata(field.metadata().clone())
}

fn convert_duration_type(dt: &DataType) -> DataType {
    match dt {
        DataType::Duration(_) => DataType::Int64,
        DataType::List(f) => DataType::List(Arc::new(convert_duration_field(f))),
        DataType::LargeList(f) => DataType::LargeList(Arc::new(convert_duration_field(f))),
        DataType::FixedSizeList(f, size) => {
            DataType::FixedSizeList(Arc::new(convert_duration_field(f)), *size)
        }
        DataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .map(|f| Arc::new(convert_duration_field(f)))
                .collect(),
        ),
        DataType::Map(f, sorted) => DataType::Map(Arc::new(convert_duration_field(f)), *sorted),
        other => other.clone(),
    }
}

/// Convert Duration arrays to Int64 recursively.
fn convert_duration_array(
    array: &dyn Array,
    target_type: &DataType,
) -> arrow::error::Result<Arc<dyn Array>> {
    match (array.data_type(), target_type) {
        (DataType::Duration(_), DataType::Int64) => {
            // All Duration types are stored as i64, just reinterpret as Int64.
            // Note: yconv only uses Duration(Nanosecond), so no unit conversion needed.
            let primitive = array.as_primitive::<arrow::datatypes::DurationNanosecondType>();
            Ok(Arc::new(primitive.reinterpret_cast::<Int64Type>()))
        }
        (DataType::List(_), DataType::List(target_field)) => {
            let list = array.as_list::<i32>();
            let converted_values = convert_duration_array(list.values(), target_field.data_type())?;
            Ok(Arc::new(arrow::array::ListArray::try_new(
                target_field.clone(),
                list.offsets().clone(),
                converted_values,
                list.nulls().cloned(),
            )?))
        }
        (DataType::LargeList(_), DataType::LargeList(target_field)) => {
            let list = array.as_list::<i64>();
            let converted_values = convert_duration_array(list.values(), target_field.data_type())?;
            Ok(Arc::new(arrow::array::LargeListArray::try_new(
                target_field.clone(),
                list.offsets().clone(),
                converted_values,
                list.nulls().cloned(),
            )?))
        }
        (DataType::FixedSizeList(_, size), DataType::FixedSizeList(target_field, _)) => {
            let list = array.as_fixed_size_list();
            let converted_values = convert_duration_array(list.values(), target_field.data_type())?;
            Ok(Arc::new(arrow::array::FixedSizeListArray::try_new(
                target_field.clone(),
                *size,
                converted_values,
                list.nulls().cloned(),
            )?))
        }
        (DataType::Struct(_), DataType::Struct(target_fields)) => {
            let struct_arr = array.as_struct();
            let converted_columns: Vec<Arc<dyn Array>> = struct_arr
                .columns()
                .iter()
                .zip(target_fields.iter())
                .map(|(col, field)| convert_duration_array(col.as_ref(), field.data_type()))
                .collect::<arrow::error::Result<_>>()?;
            Ok(Arc::new(arrow::array::StructArray::try_new(
                target_fields.clone(),
                converted_columns,
                struct_arr.nulls().cloned(),
            )?))
        }
        _ => Ok(arrow::array::make_array(array.to_data())),
    }
}

/// Convert a RecordBatch, replacing Duration columns with Int64.
fn convert_batch_durations(batch: &RecordBatch) -> arrow::error::Result<RecordBatch> {
    let schema = batch.schema();

    // Check if any conversion is needed
    let needs_conversion = schema
        .fields()
        .iter()
        .any(|f| contains_duration(f.data_type()));
    if !needs_conversion {
        return Ok(batch.clone());
    }

    // Build new schema and convert arrays
    let new_fields: Vec<Arc<Field>> = schema
        .fields()
        .iter()
        .map(|f| Arc::new(convert_duration_field(f)))
        .collect();
    let new_schema = Arc::new(Schema::new_with_metadata(
        new_fields.clone(),
        schema.metadata().clone(),
    ));

    let new_columns: Vec<Arc<dyn Array>> = batch
        .columns()
        .iter()
        .zip(new_fields.iter())
        .map(|(col, field)| convert_duration_array(col.as_ref(), field.data_type()))
        .collect::<arrow::error::Result<_>>()?;

    RecordBatch::try_new(new_schema, new_columns)
}

fn contains_duration(dt: &DataType) -> bool {
    match dt {
        DataType::Duration(_) => true,
        DataType::List(f)
        | DataType::LargeList(f)
        | DataType::FixedSizeList(f, _)
        | DataType::Map(f, _) => contains_duration(f.data_type()),
        DataType::Struct(fields) => fields.iter().any(|f| contains_duration(f.data_type())),
        _ => false,
    }
}

impl TopicWriter for VortexTopicWriter {
    fn write_batch(&mut self, batch: RecordBatch) -> Result<(), OutputError> {
        // Convert Duration types to Int64
        let converted = convert_batch_durations(&batch)?;

        // Validate schema consistency
        if let Some(ref expected_schema) = self.converted_schema {
            if converted.schema() != *expected_schema {
                return Err(OutputError::Vortex(format!(
                    "{}: Schema mismatch: expected {:?}, got {:?}",
                    self.path.display(),
                    expected_schema.fields(),
                    converted.schema().fields()
                )));
            }
        } else {
            self.converted_schema = Some(converted.schema());
        }

        // Convert to vortex array (non-nullable top-level struct)
        let struct_array = arrow::array::StructArray::from(converted);
        let vortex_array: ArrayRef = ArrayRef::from_arrow(&struct_array, false);

        // Send to writer thread
        if let Some(ref sender) = self.sender {
            sender
                .send(WriterMessage::Write(vortex_array))
                .map_err(|_| {
                    OutputError::Vortex(format!(
                        "{}: Writer thread died unexpectedly",
                        self.path.display()
                    ))
                })?;
        }

        Ok(())
    }

    fn finish(mut self: Box<Self>) -> Result<(), OutputError> {
        // Signal the writer thread to finish
        if let Some(sender) = self.sender.take() {
            let _ = sender.send(WriterMessage::Finish);
            // Drop sender to close the channel
            drop(sender);
        }

        // Wait for the writer thread to complete
        if let Some(handle) = self.writer_thread.take() {
            match handle.join() {
                Ok(Ok(())) => Ok(()),
                Ok(Err(e)) => Err(OutputError::Vortex(e)),
                Err(_) => Err(OutputError::Vortex(format!(
                    "{}: Writer thread panicked",
                    self.path.display()
                ))),
            }
        } else {
            Ok(())
        }
    }
}
