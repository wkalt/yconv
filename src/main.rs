use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{self, BufWriter};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::{Parser, Subcommand, ValueEnum};
use lancedb::connect;

use yconv::lance::{
    connect_with_options, convert_cdr_mcap_to_lance, convert_lance_to_mcap,
    convert_mcap_to_lance_fast, convert_protobuf_mcap_to_lance, ConvertOptions,
    ConvertToMcapOptions, LanceToMcapError, WriteMode,
};

/// Check if an error is a broken pipe (EPIPE).
/// This happens when piping to commands like `head` that close early.
fn is_broken_pipe<E: std::error::Error>(err: &E) -> bool {
    // Check the error description for broken pipe indicators
    let err_str = err.to_string().to_lowercase();
    if err_str.contains("broken pipe") {
        return true;
    }
    // Check source chain
    let mut source = err.source();
    while let Some(e) = source {
        let e_str = e.to_string().to_lowercase();
        if e_str.contains("broken pipe") {
            return true;
        }
        source = e.source();
    }
    false
}
use yconv::mcap::StreamReader;
use yconv::output::{self, OutputFormat as OutputFmt};
use yconv::shell::Shell;

#[derive(Debug, Clone, Copy, PartialEq, ValueEnum)]
enum OutputFormat {
    /// MCAP file format
    Mcap,
    /// LanceDB database
    Lancedb,
    /// Parquet files (one per topic)
    Parquet,
    /// Vortex files (one per topic)
    Vortex,
    /// DuckDB database
    Duckdb,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum CliWriteMode {
    /// Error if table already exists
    Error,
    /// Overwrite existing tables
    Overwrite,
    /// Append to existing tables (with schema evolution)
    Append,
}

impl From<CliWriteMode> for WriteMode {
    fn from(mode: CliWriteMode) -> Self {
        match mode {
            CliWriteMode::Error => WriteMode::ErrorIfExists,
            CliWriteMode::Overwrite => WriteMode::Overwrite,
            CliWriteMode::Append => WriteMode::Append,
        }
    }
}

/// Universal format conversion tool for robotics and analytics data
#[derive(Parser, Debug)]
#[command(name = "yconv", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Convert between data formats
    #[command(
        after_help = "Output format is inferred from -o extension, or specify --output-format with --stdout."
    )]
    Convert(ConvertArgs),

    /// Open an interactive SQL shell
    Shell(ShellArgs),

    /// Analyze compression across formats
    Analyze(AnalyzeArgs),
}

/// Convert between data formats
#[derive(Parser, Debug)]
struct ConvertArgs {
    /// Input path(s)
    #[arg(required = true, num_args = 1..)]
    input: Vec<PathBuf>,

    /// Output path or URI (supports s3://bucket/path for Lance output)
    #[arg(short, long, conflicts_with = "stdout")]
    output: Option<String>,

    /// Write to stdout
    #[arg(long, conflicts_with = "output")]
    stdout: bool,

    /// Output format (inferred from -o if not specified)
    #[arg(long, value_enum)]
    output_format: Option<OutputFormat>,

    /// Write mode for existing outputs
    #[arg(short, long, value_enum, default_value = "error")]
    mode: CliWriteMode,

    /// Open interactive SQL shell after conversion
    #[arg(long, conflicts_with = "stdout")]
    shell: bool,
}

/// Check if a path string is an S3 URI
fn is_s3_uri(path: &str) -> bool {
    path.starts_with("s3://")
}

/// Check if a path string is a GCS URI
fn is_gcs_uri(path: &str) -> bool {
    path.starts_with("gs://")
}

/// Check if a path string is an Azure Blob URI
fn is_azure_uri(path: &str) -> bool {
    path.starts_with("az://") || path.starts_with("azure://")
}

/// Check if a path string is a cloud storage URI
fn is_cloud_uri(path: &str) -> bool {
    is_s3_uri(path) || is_gcs_uri(path) || is_azure_uri(path)
}

/// Get file extension from a path string (works with both local paths and URIs)
fn get_extension(path: &str) -> Option<&str> {
    // For URIs, parse the path component
    if is_cloud_uri(path) {
        path.rsplit('/')
            .next()
            .and_then(|filename| filename.rsplit('.').next())
            .filter(|ext| !ext.contains('/'))
    } else {
        Path::new(path).extension().and_then(|e| e.to_str())
    }
}

/// Open an interactive SQL shell
#[derive(Parser, Debug)]
struct ShellArgs {
    /// Data source path (parquet, vortex, lance, or duckdb)
    #[arg(required = true)]
    source: PathBuf,
}

/// Analyze compression across formats
#[derive(Parser, Debug)]
struct AnalyzeArgs {
    /// Input MCAP file
    #[arg(required = true)]
    input: PathBuf,

    /// Output directory for converted formats (default: {input_stem}_analysis/)
    #[arg(short, long)]
    output: Option<PathBuf>,

    /// Delete output directory after analysis
    #[arg(long)]
    clean: bool,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum InputFormat {
    Mcap,
    Lance,
    Parquet,
    Vortex,
    DuckDb,
}

fn detect_input_format(inputs: &[PathBuf]) -> Result<InputFormat, String> {
    if inputs.is_empty() {
        return Err("No input files specified".to_string());
    }

    let first = &inputs[0];

    // Check if it's a .mcap file
    if first
        .extension()
        .map(|e| e.eq_ignore_ascii_case("mcap"))
        .unwrap_or(false)
    {
        // Validate all inputs are .mcap files
        for input in inputs {
            if !input
                .extension()
                .map(|e| e.eq_ignore_ascii_case("mcap"))
                .unwrap_or(false)
            {
                return Err(format!(
                    "Mixed input types: {} is not an MCAP file",
                    input.display()
                ));
            }
        }
        return Ok(InputFormat::Mcap);
    }

    // Check if it's a directory
    if first.is_dir() {
        if inputs.len() > 1 {
            return Err("Only one directory input is supported".to_string());
        }

        // Check what kind of files are in the directory
        if let Ok(entries) = std::fs::read_dir(first) {
            for entry in entries.flatten() {
                let path = entry.path();
                if let Some(ext) = path.extension() {
                    let ext = ext.to_string_lossy().to_lowercase();
                    match ext.as_str() {
                        "parquet" => return Ok(InputFormat::Parquet),
                        "vortex" => return Ok(InputFormat::Vortex),
                        "lance" => return Ok(InputFormat::Lance),
                        _ => {}
                    }
                }
            }
        }

        // Default to Lance for directories without recognized files
        return Ok(InputFormat::Lance);
    }

    // Check for DuckDB file by extension
    if let Some(ext) = first.extension() {
        let ext = ext.to_string_lossy().to_lowercase();
        if ext == "duckdb" || ext == "db" {
            if inputs.len() > 1 {
                return Err("Only one DuckDB input is supported".to_string());
            }
            return Ok(InputFormat::DuckDb);
        }
    }

    // Check if parent exists and path looks like a lance db path
    if first.exists() || first.to_string_lossy().contains("lance") {
        if inputs.len() > 1 {
            return Err("Only one LanceDB input is supported".to_string());
        }
        return Ok(InputFormat::Lance);
    }

    Err(format!(
        "Cannot determine input format: {}",
        first.display()
    ))
}

fn infer_output_format(path: &str) -> Option<OutputFormat> {
    // For cloud URIs or local paths, try to get extension
    if let Some(ext) = get_extension(path) {
        let ext = ext.to_lowercase();
        match ext.as_str() {
            "mcap" => return Some(OutputFormat::Mcap),
            "parquet" => return Some(OutputFormat::Parquet),
            "vortex" => return Some(OutputFormat::Vortex),
            "duckdb" | "db" => return Some(OutputFormat::Duckdb),
            "lance" => return Some(OutputFormat::Lancedb),
            _ => {}
        }
    }
    // Cloud URIs or directories without recognized extension are assumed to be Lance
    if is_cloud_uri(path) || Path::new(path).is_dir() || !path.contains('.') {
        return Some(OutputFormat::Lancedb);
    }
    None
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();

    match cli.command {
        Commands::Shell(args) => run_shell(args),
        Commands::Convert(args) => run_convert(args).await,
        Commands::Analyze(args) => run_analyze(args).await,
    }
}

fn run_shell(args: ShellArgs) -> ExitCode {
    match Shell::open(&args.source) {
        Ok(shell) => {
            if let Err(e) = shell.run() {
                eprintln!("Shell error: {}", e);
                return ExitCode::FAILURE;
            }
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("Error: {}", e);
            ExitCode::FAILURE
        }
    }
}

async fn run_convert(args: ConvertArgs) -> ExitCode {
    // Validate output is specified
    if args.output.is_none() && !args.stdout {
        eprintln!("Error: specify -o <path> or --stdout");
        return ExitCode::FAILURE;
    }

    // Save output path for potential shell opening later
    let output_path = args.output.clone();
    let open_shell = args.shell;

    // Determine output format
    let output_format = if let Some(fmt) = args.output_format {
        fmt
    } else if let Some(ref path) = args.output {
        match infer_output_format(path) {
            Some(fmt) => fmt,
            None => {
                eprintln!("Error: cannot infer output format from path, use --output-format");
                return ExitCode::FAILURE;
            }
        }
    } else {
        // stdout requires explicit format
        eprintln!("Error: --output-format is required with --stdout");
        return ExitCode::FAILURE;
    };

    // Validate stdout is only used with mcap output
    if args.stdout && output_format != OutputFormat::Mcap {
        eprintln!("Error: --stdout only supports mcap output format");
        return ExitCode::FAILURE;
    }

    // Detect input format
    let input_format = match detect_input_format(&args.input) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("Error: {e}");
            return ExitCode::FAILURE;
        }
    };

    // Validate conversion is supported
    let result = match (input_format, output_format) {
        // MCAP → other formats
        (InputFormat::Mcap, OutputFormat::Lancedb) => {
            let output = args.output.unwrap();
            run_to_lance(args.input, output, args.mode).await
        }
        (InputFormat::Mcap, OutputFormat::Parquet) => {
            let output = args.output.unwrap();
            run_to_output_format(args.input, output, OutputFmt::Parquet).await
        }
        (InputFormat::Mcap, OutputFormat::Vortex) => {
            let output = args.output.unwrap();
            run_to_output_format(args.input, output, OutputFmt::Vortex).await
        }
        (InputFormat::Mcap, OutputFormat::Duckdb) => {
            let output = args.output.unwrap();
            run_to_output_format(args.input, output, OutputFmt::DuckDb).await
        }
        (InputFormat::Mcap, OutputFormat::Mcap) => {
            Err("mcap to mcap conversion not yet supported".into())
        }

        // Lance → other formats
        (InputFormat::Lance, OutputFormat::Mcap) => {
            run_to_mcap(args.input[0].clone(), args.output, args.stdout).await
        }
        (InputFormat::Lance, OutputFormat::Parquet) => {
            let output = args.output.unwrap();
            run_lance_to_output(args.input[0].clone(), output, OutputFmt::Parquet).await
        }
        (InputFormat::Lance, OutputFormat::Vortex) => {
            let output = args.output.unwrap();
            run_lance_to_output(args.input[0].clone(), output, OutputFmt::Vortex).await
        }
        (InputFormat::Lance, OutputFormat::Duckdb) => {
            let output = args.output.unwrap();
            run_lance_to_output(args.input[0].clone(), output, OutputFmt::DuckDb).await
        }
        (InputFormat::Lance, OutputFormat::Lancedb) => {
            Err("lance to lance conversion not yet supported".into())
        }

        // Parquet → other formats
        (InputFormat::Parquet, OutputFormat::Mcap) => run_input_to_mcap(
            yconv::input::InputFormat::Parquet,
            args.input[0].clone(),
            args.output,
            args.stdout,
        ),
        (InputFormat::Parquet, OutputFormat::Lancedb) => {
            let output = args.output.unwrap();
            run_input_to_lance(
                yconv::input::InputFormat::Parquet,
                args.input[0].clone(),
                output,
                args.mode,
            )
            .await
        }
        (InputFormat::Parquet, OutputFormat::Vortex) => {
            let output = args.output.unwrap();
            run_input_to_output(
                yconv::input::InputFormat::Parquet,
                args.input[0].clone(),
                OutputFmt::Vortex,
                output,
            )
        }
        (InputFormat::Parquet, OutputFormat::Duckdb) => {
            let output = args.output.unwrap();
            run_input_to_output(
                yconv::input::InputFormat::Parquet,
                args.input[0].clone(),
                OutputFmt::DuckDb,
                output,
            )
        }
        (InputFormat::Parquet, OutputFormat::Parquet) => {
            Err("parquet to parquet conversion not yet supported".into())
        }

        // Vortex → other formats
        (InputFormat::Vortex, OutputFormat::Mcap) => run_input_to_mcap(
            yconv::input::InputFormat::Vortex,
            args.input[0].clone(),
            args.output,
            args.stdout,
        ),
        (InputFormat::Vortex, OutputFormat::Lancedb) => {
            let output = args.output.unwrap();
            run_input_to_lance(
                yconv::input::InputFormat::Vortex,
                args.input[0].clone(),
                output,
                args.mode,
            )
            .await
        }
        (InputFormat::Vortex, OutputFormat::Parquet) => {
            let output = args.output.unwrap();
            run_input_to_output(
                yconv::input::InputFormat::Vortex,
                args.input[0].clone(),
                OutputFmt::Parquet,
                output,
            )
        }
        (InputFormat::Vortex, OutputFormat::Duckdb) => {
            let output = args.output.unwrap();
            run_input_to_output(
                yconv::input::InputFormat::Vortex,
                args.input[0].clone(),
                OutputFmt::DuckDb,
                output,
            )
        }
        (InputFormat::Vortex, OutputFormat::Vortex) => {
            Err("vortex to vortex conversion not yet supported".into())
        }

        // DuckDB → other formats
        (InputFormat::DuckDb, OutputFormat::Mcap) => run_input_to_mcap(
            yconv::input::InputFormat::DuckDb,
            args.input[0].clone(),
            args.output,
            args.stdout,
        ),
        (InputFormat::DuckDb, OutputFormat::Lancedb) => {
            let output = args.output.unwrap();
            run_input_to_lance(
                yconv::input::InputFormat::DuckDb,
                args.input[0].clone(),
                output,
                args.mode,
            )
            .await
        }
        (InputFormat::DuckDb, OutputFormat::Parquet) => {
            let output = args.output.unwrap();
            run_input_to_output(
                yconv::input::InputFormat::DuckDb,
                args.input[0].clone(),
                OutputFmt::Parquet,
                output,
            )
        }
        (InputFormat::DuckDb, OutputFormat::Vortex) => {
            let output = args.output.unwrap();
            run_input_to_output(
                yconv::input::InputFormat::DuckDb,
                args.input[0].clone(),
                OutputFmt::Vortex,
                output,
            )
        }
        (InputFormat::DuckDb, OutputFormat::Duckdb) => {
            Err("duckdb to duckdb conversion not yet supported".into())
        }
    };

    if let Err(e) = result {
        eprintln!("Error: {e}");
        return ExitCode::FAILURE;
    }

    // Open shell if requested
    if open_shell {
        if let Some(ref path) = output_path {
            // Shell only works with local paths
            if is_cloud_uri(path) {
                eprintln!("Note: --shell not supported for cloud storage output");
            } else {
                let path = PathBuf::from(path);
                eprintln!();
                eprintln!("Opening shell for {}...", path.display());
                match Shell::open(&path) {
                    Ok(shell) => {
                        if let Err(e) = shell.run() {
                            eprintln!("Shell error: {}", e);
                            return ExitCode::FAILURE;
                        }
                    }
                    Err(e) => {
                        eprintln!("Failed to open shell: {}", e);
                        return ExitCode::FAILURE;
                    }
                }
            }
        }
    }

    ExitCode::SUCCESS
}

/// Detects the message encoding used in an MCAP file by reading the first channel.
/// Returns "ros1", "protobuf", or "unknown".
fn detect_mcap_encoding(path: &PathBuf) -> Result<String, Box<dyn std::error::Error>> {
    use std::io::BufReader;

    let file = File::open(path)?;
    let mut reader = StreamReader::new(BufReader::new(file));

    // Read until we find a channel
    while let Some(_msg) = reader.next_message()? {
        // Check channels we've discovered
        if let Some(channel) = reader.channels().values().next() {
            return Ok(channel.message_encoding.clone());
        }
    }

    Ok("unknown".to_string())
}

async fn run_to_lance(
    inputs: Vec<PathBuf>,
    output: String,
    mode: CliWriteMode,
) -> Result<(), Box<dyn std::error::Error>> {
    // Connect to LanceDB with default storage version (supports local paths and s3:// URIs)
    let default_options = ConvertOptions::default();
    let db = connect_with_options(&output, default_options.data_storage_version)
        .await
        .map_err(|e| format!("Failed to connect to LanceDB at {}: {e}", output))?;

    let mut total_messages = 0usize;
    let mut total_tables = 0usize;
    let mut all_topics: std::collections::BTreeMap<String, usize> =
        std::collections::BTreeMap::new();

    for (i, input) in inputs.iter().enumerate() {
        // Detect encoding from the MCAP file
        let encoding = detect_mcap_encoding(input)?;

        // Open input file (need to open again after detection)
        let file =
            File::open(input).map_err(|e| format!("Failed to open {}: {e}", input.display()))?;

        // First file uses specified mode, subsequent files use Append
        let write_mode = if i == 0 {
            mode.into()
        } else {
            WriteMode::Append
        };

        // Build options
        let options = ConvertOptions {
            write_mode,
            ..Default::default()
        };

        // Convert based on detected encoding
        eprintln!(
            "[{}/{}] Converting {} ({} encoding) to {}...",
            i + 1,
            inputs.len(),
            input.display(),
            encoding,
            output
        );

        let stats = match encoding.as_str() {
            "ros1" => convert_mcap_to_lance_fast(file, &db, options).await?,
            "protobuf" => convert_protobuf_mcap_to_lance(file, &db, options).await?,
            "cdr" => convert_cdr_mcap_to_lance(file, &db, options).await?,
            other => return Err(format!("Unsupported MCAP encoding: {}", other).into()),
        };

        total_messages += stats.total_messages;
        if i == 0 {
            total_tables = stats.tables_created;
        }
        for (topic, count) in stats.messages_per_topic {
            *all_topics.entry(topic).or_insert(0) += count;
        }
    }

    // Print results
    eprintln!();
    eprintln!("Conversion complete:");
    eprintln!("  Files processed: {}", inputs.len());
    eprintln!("  Total messages: {}", total_messages);
    eprintln!("  Tables created: {}", total_tables);
    eprintln!();
    eprintln!("Messages per topic:");

    for (topic, count) in &all_topics {
        eprintln!("  {}: {}", topic, count);
    }

    Ok(())
}

async fn run_to_mcap(
    input: PathBuf,
    output: Option<String>,
    use_stdout: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    // MCAP output doesn't support cloud storage (it's a streaming format)
    if let Some(ref out) = output {
        if is_cloud_uri(out) {
            return Err("MCAP output to cloud storage is not supported".into());
        }
    }

    // Connect to LanceDB
    let db = connect(input.to_str().ok_or("Invalid input path")?)
        .execute()
        .await
        .map_err(|e| format!("Failed to connect to LanceDB at {}: {e}", input.display()))?;

    let options = ConvertToMcapOptions::default();

    let output_desc = if use_stdout {
        "stdout".to_string()
    } else {
        output.as_ref().unwrap().clone()
    };

    if !use_stdout {
        eprintln!("Converting {} to {}...", input.display(), output_desc);
    }

    // Create writer and convert
    let result: Result<_, LanceToMcapError> = if use_stdout {
        let writer = BufWriter::new(io::stdout().lock());
        convert_lance_to_mcap(&db, writer, options).await
    } else {
        let output_path = PathBuf::from(output.unwrap());
        let file = File::create(&output_path)
            .map_err(|e| format!("Failed to create {}: {e}", output_path.display()))?;
        let writer = BufWriter::new(file);
        convert_lance_to_mcap(&db, writer, options).await
    };

    // Handle broken pipe gracefully (e.g., when piping to `head`)
    let stats = match result {
        Ok(stats) => stats,
        Err(e) if use_stdout && is_broken_pipe(&e) => return Ok(()),
        Err(e) => return Err(e.into()),
    };

    // Print results (skip for stdout)
    if !use_stdout {
        eprintln!();
        eprintln!("Conversion complete:");
        eprintln!("  Total messages: {}", stats.total_messages);
        eprintln!("  Topics converted: {}", stats.topics_converted);
        eprintln!();
        eprintln!("Messages per topic:");

        let mut topics: Vec<_> = stats.messages_per_topic.iter().collect();
        topics.sort_by_key(|(name, _)| *name);

        for (topic, count) in topics {
            eprintln!("  {}: {}", topic, count);
        }
    }

    Ok(())
}

async fn run_to_output_format(
    inputs: Vec<PathBuf>,
    output: String,
    format: OutputFmt,
) -> Result<(), Box<dyn std::error::Error>> {
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use std::collections::{BTreeMap, HashMap};
    use std::sync::Arc;
    use yconv::arrow::ArrowRowSink;
    use yconv::mcap::Ros1Reader;
    use yconv::ros1::CompiledTranscoder;

    // For now, Parquet/Vortex/DuckDB only support local paths
    if is_cloud_uri(&output) {
        return Err("Cloud storage output is only supported for Lance format. Use -o with a .lance path for S3 output.".to_string().into());
    }
    let output_path = PathBuf::from(&output);

    // Create the output database
    let mut db = output::create_output(format, &output_path)?;

    let mut total_messages = 0usize;
    let mut topic_count = 0usize;
    let mut all_topics: BTreeMap<String, usize> = BTreeMap::new();

    // Track writers and their schemas per topic
    let mut writers: HashMap<String, Box<dyn output::TopicWriter>> = HashMap::new();
    let mut sinks: HashMap<String, ArrowRowSink> = HashMap::new();
    let mut transcoders: HashMap<String, CompiledTranscoder> = HashMap::new();
    let mut log_times: HashMap<String, Vec<i64>> = HashMap::new();
    let mut pending_counts: HashMap<String, usize> = HashMap::new();

    const BATCH_SIZE: usize = 1000;

    for (i, input) in inputs.iter().enumerate() {
        eprintln!(
            "[{}/{}] Converting {} to {}...",
            i + 1,
            inputs.len(),
            input.display(),
            output
        );

        let file =
            File::open(input).map_err(|e| format!("Failed to open {}: {e}", input.display()))?;

        let mut reader = Ros1Reader::new(file);

        // Read all messages
        while let Some(raw_msg) = reader.next_raw_message()? {
            // Get or create writer for this topic
            if !writers.contains_key(&raw_msg.topic) {
                let schema = reader
                    .topic_schema(&raw_msg.topic)
                    .expect("schema should exist after reading message")
                    .clone();

                // Compile the transcoder once per topic
                let definition = reader.definition(raw_msg.schema_id).unwrap();
                let registry = reader.registry(raw_msg.schema_id).unwrap();
                let transcoder = CompiledTranscoder::compile(definition, registry)?;
                transcoders.insert(raw_msg.topic.clone(), transcoder);

                // Add _log_time column to schema
                let mut fields: Vec<Arc<Field>> = schema.fields().iter().cloned().collect();
                fields.push(Arc::new(Field::new(
                    "_log_time",
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    false,
                )));
                let full_schema = Arc::new(Schema::new(fields));

                let writer = db.create_topic_writer(&raw_msg.topic, full_schema.clone())?;
                writers.insert(raw_msg.topic.clone(), writer);

                // Create the sink (without _log_time, we add it separately)
                let sink = ArrowRowSink::new(schema)?;
                sinks.insert(raw_msg.topic.clone(), sink);
                log_times.insert(raw_msg.topic.clone(), Vec::new());
                pending_counts.insert(raw_msg.topic.clone(), 0);
                topic_count += 1;
            }

            let transcoder = transcoders.get(&raw_msg.topic).unwrap();
            let sink = sinks.get_mut(&raw_msg.topic).unwrap();
            let times = log_times.get_mut(&raw_msg.topic).unwrap();
            let pending = pending_counts.get_mut(&raw_msg.topic).unwrap();

            // Transcode using pre-compiled transcoder (zero lookups per message)
            transcoder.transcode(&raw_msg.data, sink)?;
            times.push(raw_msg.log_time as i64);
            *pending += 1;

            // Flush if batch is full
            if *pending >= BATCH_SIZE {
                let batch = sink.finish()?;
                // Add _log_time column
                let time_array: arrow::array::ArrayRef = Arc::new(
                    arrow::array::TimestampNanosecondArray::from(std::mem::take(times)),
                );
                let mut columns: Vec<arrow::array::ArrayRef> = batch.columns().to_vec();
                columns.push(time_array);
                let mut fields: Vec<Arc<Field>> = batch.schema().fields().iter().cloned().collect();
                fields.push(Arc::new(Field::new(
                    "_log_time",
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    false,
                )));
                let schema = Arc::new(Schema::new(fields));
                let batch = arrow::array::RecordBatch::try_new(schema, columns)?;
                writers
                    .get_mut(&raw_msg.topic)
                    .unwrap()
                    .write_batch(batch)?;
                *pending = 0;
            }

            *all_topics.entry(raw_msg.topic).or_insert(0) += 1;
            total_messages += 1;
        }
    }

    // Flush remaining batches
    for (topic, sink) in sinks.iter_mut() {
        let times = log_times.get_mut(topic).unwrap();
        if !times.is_empty() {
            let batch = sink.finish()?;
            let time_array: arrow::array::ArrayRef = Arc::new(
                arrow::array::TimestampNanosecondArray::from(std::mem::take(times)),
            );
            let mut columns: Vec<arrow::array::ArrayRef> = batch.columns().to_vec();
            columns.push(time_array);
            let mut fields: Vec<Arc<Field>> = batch.schema().fields().iter().cloned().collect();
            fields.push(Arc::new(Field::new(
                "_log_time",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            )));
            let schema = Arc::new(Schema::new(fields));
            let batch = arrow::array::RecordBatch::try_new(schema, columns)?;
            writers.get_mut(topic).unwrap().write_batch(batch)?;
        }
    }

    // Finish all writers
    for (_topic, writer) in writers {
        writer.finish()?;
    }

    // Finish the database
    db.finish()?;

    // Print results
    eprintln!();
    eprintln!("Conversion complete:");
    eprintln!("  Files processed: {}", inputs.len());
    eprintln!("  Total messages: {}", total_messages);
    eprintln!("  Topics created: {}", topic_count);
    eprintln!();
    eprintln!("Messages per topic:");

    for (topic, count) in &all_topics {
        eprintln!("  {}: {}", topic, count);
    }

    Ok(())
}

async fn run_lance_to_output(
    input: PathBuf,
    output: String,
    format: OutputFmt,
) -> Result<(), Box<dyn std::error::Error>> {
    use futures::TryStreamExt;
    use lancedb::query::ExecutableQuery;
    use std::collections::BTreeMap;

    // For now, Parquet/Vortex/DuckDB only support local paths
    if is_cloud_uri(&output) {
        return Err("Cloud storage output is only supported for Lance format. Use -o with a .lance path for S3 output.".to_string().into());
    }
    let output_path = PathBuf::from(&output);

    // Connect to LanceDB
    let db = connect(input.to_str().ok_or("Invalid input path")?)
        .execute()
        .await
        .map_err(|e| format!("Failed to connect to LanceDB at {}: {e}", input.display()))?;

    // Create output database
    let mut output_db = output::create_output(format, &output_path)?;

    eprintln!("Converting {} to {}...", input.display(), output);

    // Get all table names
    let table_names: Vec<String> = db.table_names().execute().await?;
    let mut total_messages = 0usize;
    let mut all_topics: BTreeMap<String, usize> = BTreeMap::new();

    for table_name in &table_names {
        // Open the table
        let table = db.open_table(table_name).execute().await?;

        // Query all data
        let batches: Vec<arrow::array::RecordBatch> =
            table.query().execute().await?.try_collect().await?;

        if batches.is_empty() {
            continue;
        }

        // Get schema from first batch
        let schema = batches[0].schema();

        // Create writer for this topic
        let mut writer = output_db.create_topic_writer(table_name, schema)?;

        let mut topic_count = 0usize;
        for batch in batches {
            topic_count += batch.num_rows();
            writer.write_batch(batch)?;
        }

        writer.finish()?;
        all_topics.insert(table_name.to_string(), topic_count);
        total_messages += topic_count;
    }

    // Finish output database
    output_db.finish()?;

    // Print results
    eprintln!();
    eprintln!("Conversion complete:");
    eprintln!("  Total messages: {}", total_messages);
    eprintln!("  Tables converted: {}", table_names.len());
    eprintln!();
    eprintln!("Messages per topic:");

    for (topic, count) in &all_topics {
        eprintln!("  {}: {}", topic, count);
    }

    Ok(())
}

fn run_input_to_mcap(
    input_format: yconv::input::InputFormat,
    input: PathBuf,
    output: Option<String>,
    use_stdout: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    use arrow::array::ArrayRef;
    use std::collections::BTreeMap;
    use yconv::arrow::ArrowRowSource;
    use yconv::input::open_input;
    use yconv::ros1::{arrow_schema_to_msg, Ros1SchemaInfo, Ros1Writer};

    // MCAP output doesn't support cloud storage (it's a streaming format)
    if let Some(ref out) = output {
        if is_cloud_uri(out) {
            return Err("MCAP output to cloud storage is not supported".into());
        }
    }

    // Open input database
    let mut input_db = open_input(input_format, &input)?;

    let output_desc = if use_stdout {
        "stdout".to_string()
    } else {
        output.as_ref().unwrap().clone()
    };

    if !use_stdout {
        eprintln!("Converting {} to {}...", input.display(), output_desc);
    }

    // Create MCAP writer with no-seek wrapper
    let write_options = mcap::write::WriteOptions::default().disable_seeking(true);
    let writer: Box<dyn std::io::Write> = if use_stdout {
        Box::new(BufWriter::new(io::stdout().lock()))
    } else {
        let output_path = PathBuf::from(output.unwrap());
        let file = File::create(&output_path)
            .map_err(|e| format!("Failed to create {}: {e}", output_path.display()))?;
        Box::new(BufWriter::new(file))
    };
    let mut mcap_writer =
        mcap::write::Writer::with_options(mcap::write::NoSeek::new(writer), write_options)?;

    let table_names = input_db.table_names()?;
    let mut total_messages = 0usize;
    let mut all_topics: BTreeMap<String, usize> = BTreeMap::new();

    // Track channel info per topic
    struct TopicInfo {
        channel_id: u16,
        schema_info: Ros1SchemaInfo,
        message_column_indices: Vec<usize>,
        log_time_column_idx: Option<usize>,
        sequence: u32,
    }
    let mut topic_infos: std::collections::HashMap<String, TopicInfo> =
        std::collections::HashMap::new();
    let mut ros1_writer = Ros1Writer::new();

    for table_name in &table_names {
        let mut reader = input_db.open_table(table_name)?;
        let full_schema = reader.schema();

        // Find _log_time column if present
        let log_time_column_idx = full_schema
            .fields()
            .iter()
            .position(|f| f.name() == "_log_time");

        // Build message schema (without _log_time column)
        let message_fields: Vec<_> = full_schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(i, _)| Some(*i) != log_time_column_idx)
            .map(|(_, f)| f.as_ref().clone())
            .collect();
        let message_schema = arrow::datatypes::Schema::new(message_fields);

        // Get column indices for message fields
        let message_column_indices: Vec<usize> = (0..full_schema.fields().len())
            .filter(|i| Some(*i) != log_time_column_idx)
            .collect();

        // Convert Arrow schema to ROS1 msg format
        let msg_schema = arrow_schema_to_msg(&message_schema)?;

        // Build field info for serialization
        let schema_info = Ros1SchemaInfo::from_arrow_schema(&message_schema)?;

        // Convert table name to topic format
        let topic_name = format!(
            "/{}",
            table_name
                .strip_prefix('_')
                .unwrap_or(table_name)
                .replace('_', "/")
        );

        // Add schema to MCAP
        let schema_id = mcap_writer.add_schema(table_name, "ros1msg", msg_schema.as_bytes())?;

        // Add channel to MCAP
        let channel_id =
            mcap_writer.add_channel(schema_id, &topic_name, "ros1msg", &BTreeMap::new())?;

        topic_infos.insert(
            table_name.clone(),
            TopicInfo {
                channel_id,
                schema_info,
                message_column_indices,
                log_time_column_idx,
                sequence: 0,
            },
        );

        let mut topic_count = 0usize;

        // Read batches and write to MCAP
        while let Some(batch) = reader.next_batch()? {
            let info = topic_infos.get_mut(table_name).unwrap();
            let num_rows = batch.num_rows();

            for row_idx in 0..num_rows {
                // Get log time from column or use 0
                let log_time = if let Some(idx) = info.log_time_column_idx {
                    use arrow::array::Array;
                    let col = batch.column(idx);
                    if let Some(ts_array) = col
                        .as_any()
                        .downcast_ref::<arrow::array::TimestampNanosecondArray>()
                    {
                        ts_array.value(row_idx) as u64
                    } else if let Some(ts_array) =
                        col.as_any().downcast_ref::<arrow::array::UInt64Array>()
                    {
                        ts_array.value(row_idx)
                    } else {
                        0
                    }
                } else {
                    0
                };

                // Serialize the message (excluding _log_time column)
                ros1_writer.clear();
                let arrays: Vec<ArrayRef> = info
                    .message_column_indices
                    .iter()
                    .map(|&i| batch.column(i).clone())
                    .collect();
                let mut source = ArrowRowSource::from_columns_row(arrays, row_idx);
                ros1_writer.write_row(&mut source, &info.schema_info.fields)?;

                // Write to MCAP
                if let Err(e) = mcap_writer.write_to_known_channel(
                    &mcap::records::MessageHeader {
                        channel_id: info.channel_id,
                        sequence: info.sequence,
                        log_time,
                        publish_time: log_time,
                    },
                    ros1_writer.as_bytes(),
                ) {
                    // Handle broken pipe gracefully (e.g., when piping to `head`)
                    if use_stdout && is_broken_pipe(&e) {
                        return Ok(());
                    }
                    return Err(e.into());
                }

                info.sequence += 1;
                topic_count += 1;
            }
        }

        all_topics.insert(table_name.clone(), topic_count);
        total_messages += topic_count;
    }

    if let Err(e) = mcap_writer.finish() {
        // Handle broken pipe gracefully (e.g., when piping to `head`)
        if use_stdout && is_broken_pipe(&e) {
            return Ok(());
        }
        return Err(e.into());
    }

    // Print results (skip for stdout)
    if !use_stdout {
        eprintln!();
        eprintln!("Conversion complete:");
        eprintln!("  Total messages: {}", total_messages);
        eprintln!("  Topics converted: {}", table_names.len());
        eprintln!();
        eprintln!("Messages per topic:");

        for (topic, count) in &all_topics {
            eprintln!("  {}: {}", topic, count);
        }
    }

    Ok(())
}

async fn run_input_to_lance(
    input_format: yconv::input::InputFormat,
    input: PathBuf,
    output: String,
    mode: CliWriteMode,
) -> Result<(), Box<dyn std::error::Error>> {
    use arrow::record_batch::RecordBatchIterator;
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use yconv::input::open_input;

    // Open input database
    let mut input_db = open_input(input_format, &input)?;

    // Connect to LanceDB with default storage version (supports local paths and s3:// URIs)
    let default_options = ConvertOptions::default();
    let db = connect_with_options(&output, default_options.data_storage_version)
        .await
        .map_err(|e| format!("Failed to connect to LanceDB at {}: {e}", output))?;

    eprintln!("Converting {} to {}...", input.display(), output);

    let table_names = input_db.table_names()?;
    let existing_tables: Vec<String> = db.table_names().execute().await?;
    let mut total_messages = 0usize;
    let mut all_topics: BTreeMap<String, usize> = BTreeMap::new();

    for (i, table_name) in table_names.iter().enumerate() {
        let mut reader = input_db.open_table(table_name)?;
        let schema = reader.schema();

        // Collect all batches for this table
        let mut batches = Vec::new();
        while let Some(batch) = reader.next_batch()? {
            batches.push(batch);
        }

        if batches.is_empty() {
            continue;
        }

        let topic_count: usize = batches.iter().map(|b| b.num_rows()).sum();

        // First table uses specified mode, subsequent tables always create/append
        let write_mode: WriteMode = if i == 0 {
            mode.into()
        } else {
            WriteMode::Append
        };

        // Check if table exists
        let table_exists = existing_tables.contains(table_name);

        // Convert Vec<RecordBatch> to RecordBatchIterator
        let batch_iter = RecordBatchIterator::new(batches.into_iter().map(Ok), Arc::clone(&schema));

        if table_exists {
            match write_mode {
                WriteMode::ErrorIfExists => {
                    return Err(format!("Table '{}' already exists", table_name).into());
                }
                WriteMode::Overwrite => {
                    db.drop_table(table_name, &[]).await?;
                    db.create_table(table_name, batch_iter).execute().await?;
                }
                WriteMode::Append => {
                    let table = db.open_table(table_name).execute().await?;
                    table.add(batch_iter).execute().await?;
                }
            }
        } else {
            db.create_table(table_name, batch_iter).execute().await?;
        }

        all_topics.insert(table_name.clone(), topic_count);
        total_messages += topic_count;
    }

    // Print results
    eprintln!();
    eprintln!("Conversion complete:");
    eprintln!("  Total messages: {}", total_messages);
    eprintln!("  Tables created: {}", table_names.len());
    eprintln!();
    eprintln!("Messages per topic:");

    for (topic, count) in &all_topics {
        eprintln!("  {}: {}", topic, count);
    }

    Ok(())
}

fn run_input_to_output(
    input_format: yconv::input::InputFormat,
    input: PathBuf,
    output_format: OutputFmt,
    output: String,
) -> Result<(), Box<dyn std::error::Error>> {
    use std::collections::BTreeMap;
    use yconv::input::open_input;

    // For now, Parquet/Vortex/DuckDB only support local paths
    if is_cloud_uri(&output) {
        return Err("Cloud storage output is only supported for Lance format. Use -o with a .lance path for S3 output.".to_string().into());
    }
    let output_path = PathBuf::from(&output);

    // Open input database
    let mut input_db = open_input(input_format, &input)?;

    // Create output database
    let mut output_db = output::create_output(output_format, &output_path)?;

    eprintln!("Converting {} to {}...", input.display(), output);

    let table_names = input_db.table_names()?;
    let mut total_messages = 0usize;
    let mut all_topics: BTreeMap<String, usize> = BTreeMap::new();

    for table_name in &table_names {
        let mut reader = input_db.open_table(table_name)?;
        let schema = reader.schema();

        // Create writer for this topic
        let mut writer = output_db.create_topic_writer(table_name, schema)?;

        let mut topic_count = 0usize;

        // Read batches and write to output
        while let Some(batch) = reader.next_batch()? {
            topic_count += batch.num_rows();
            writer.write_batch(batch)?;
        }

        writer.finish()?;
        all_topics.insert(table_name.clone(), topic_count);
        total_messages += topic_count;
    }

    // Finish output database
    output_db.finish()?;

    // Print results
    eprintln!();
    eprintln!("Conversion complete:");
    eprintln!("  Total messages: {}", total_messages);
    eprintln!("  Tables converted: {}", table_names.len());
    eprintln!();
    eprintln!("Messages per topic:");

    for (topic, count) in &all_topics {
        eprintln!("  {}: {}", topic, count);
    }

    Ok(())
}

// ============================================================================
// Analyze command implementation
// ============================================================================

/// Measure per-file sizes in a directory (for Parquet/Vortex)
/// Returns map of topic_name -> size_bytes
fn measure_per_topic_sizes(dir: &Path, extension: &str) -> io::Result<HashMap<String, u64>> {
    let mut sizes = HashMap::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().is_some_and(|e| e == extension) {
            let name = path.file_stem().unwrap().to_string_lossy().to_string();
            sizes.insert(name, fs::metadata(&path)?.len());
        }
    }
    Ok(sizes)
}

/// Measure per-table sizes for LanceDB (each table is a .lance subdirectory)
fn measure_lance_per_topic_sizes(dir: &Path) -> io::Result<HashMap<String, u64>> {
    let mut sizes = HashMap::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() && path.extension().is_some_and(|e| e == "lance") {
            let name = path.file_stem().unwrap().to_string_lossy().to_string();
            sizes.insert(name, dir_size_recursive(&path)?);
        }
    }
    Ok(sizes)
}

/// Calculate total size of a directory recursively
fn dir_size_recursive(dir: &Path) -> io::Result<u64> {
    let mut total = 0;
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let meta = entry.metadata()?;
        if meta.is_dir() {
            total += dir_size_recursive(&entry.path())?;
        } else {
            total += meta.len();
        }
    }
    Ok(total)
}

/// Format a size in bytes to human-readable form
fn format_size(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;

    if bytes >= GB {
        format!("{:.1} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}

/// Format a duration to human-readable form
fn format_duration(d: std::time::Duration) -> String {
    let secs = d.as_secs_f64();
    if secs >= 60.0 {
        let mins = (secs / 60.0).floor();
        let remaining_secs = secs - mins * 60.0;
        format!("{:.0}m {:.1}s", mins, remaining_secs)
    } else if secs >= 1.0 {
        format!("{:.2}s", secs)
    } else {
        format!("{:.0}ms", secs * 1000.0)
    }
}

/// Sanitize a topic name to match the file name convention
fn sanitize_topic_name(topic: &str) -> String {
    topic
        .trim_start_matches('/')
        .replace(['/', '-'], "_")
}

/// Result of a conversion with per-topic failure tracking
struct ConversionResult {
    messages_per_topic: HashMap<String, usize>,
    failed_topics: std::collections::HashSet<String>,
}

/// Internal conversion to Parquet/Vortex that returns stats without verbose output.
/// Handles per-topic failures gracefully - failed topics are skipped and reported.
async fn convert_mcap_to_output_format_quiet(
    inputs: Vec<PathBuf>,
    output: String,
    format: OutputFmt,
) -> Result<ConversionResult, Box<dyn std::error::Error + Send + Sync>> {
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use std::collections::{BTreeMap, HashSet};
    use std::sync::Arc;
    use yconv::arrow::ArrowRowSink;
    use yconv::mcap::Ros1Reader;
    use yconv::output::TopicWriter;
    use yconv::ros1::CompiledTranscoder;

    const BATCH_SIZE: usize = 1000;

    let output_path = PathBuf::from(&output);
    fs::create_dir_all(&output_path)?;

    let mut output_db = output::create_output(format, &output_path)?;
    let mut messages_per_topic: BTreeMap<String, usize> = BTreeMap::new();
    let mut failed_topics: HashSet<String> = HashSet::new();

    // Track writers and their schemas per topic
    let mut writers: std::collections::HashMap<String, Box<dyn TopicWriter>> =
        std::collections::HashMap::new();
    let mut sinks: std::collections::HashMap<String, ArrowRowSink> =
        std::collections::HashMap::new();
    let mut transcoders: std::collections::HashMap<String, CompiledTranscoder> =
        std::collections::HashMap::new();
    let mut log_times: std::collections::HashMap<String, Vec<i64>> =
        std::collections::HashMap::new();
    let mut pending_counts: std::collections::HashMap<String, usize> =
        std::collections::HashMap::new();
    // Track output file paths for cleanup on failure
    let mut topic_paths: std::collections::HashMap<String, PathBuf> =
        std::collections::HashMap::new();

    /// Helper to clean up a failed topic
    fn cleanup_failed_topic(
        topic: &str,
        writers: &mut std::collections::HashMap<String, Box<dyn TopicWriter>>,
        sinks: &mut std::collections::HashMap<String, ArrowRowSink>,
        transcoders: &mut std::collections::HashMap<String, CompiledTranscoder>,
        log_times: &mut std::collections::HashMap<String, Vec<i64>>,
        pending_counts: &mut std::collections::HashMap<String, usize>,
        topic_paths: &std::collections::HashMap<String, PathBuf>,
    ) {
        writers.remove(topic);
        sinks.remove(topic);
        transcoders.remove(topic);
        log_times.remove(topic);
        pending_counts.remove(topic);
        // Try to delete the output file
        if let Some(path) = topic_paths.get(topic) {
            let _ = fs::remove_file(path);
        }
    }

    for input in &inputs {
        let file = File::open(input)?;
        let mut reader = Ros1Reader::new(file);

        while let Some(raw_msg) = reader.next_raw_message()? {
            let topic = &raw_msg.topic;

            // Skip messages from failed topics
            if failed_topics.contains(topic) {
                continue;
            }

            // Initialize writer for new topic
            if !writers.contains_key(topic) {
                let init_result: Result<(), Box<dyn std::error::Error + Send + Sync>> = (|| {
                    let schema = reader
                        .topic_schema(topic)
                        .expect("schema should exist after reading message")
                        .clone();

                    // Compile the transcoder once per topic
                    let definition = reader.definition(raw_msg.schema_id).unwrap();
                    let registry = reader.registry(raw_msg.schema_id).unwrap();
                    let transcoder = CompiledTranscoder::compile(definition, registry)?;
                    transcoders.insert(topic.clone(), transcoder);

                    // Add _log_time column to schema
                    let mut fields: Vec<Arc<Field>> = schema.fields().iter().cloned().collect();
                    fields.push(Arc::new(Field::new(
                        "_log_time",
                        DataType::Timestamp(TimeUnit::Nanosecond, None),
                        false,
                    )));
                    let full_schema = Arc::new(Schema::new(fields));

                    // Track output file path for potential cleanup
                    let sanitized = sanitize_topic_name(topic);
                    let ext = match format {
                        OutputFmt::Parquet => "parquet",
                        OutputFmt::Vortex => "vortex",
                        OutputFmt::DuckDb => "duckdb",
                    };
                    topic_paths.insert(
                        topic.clone(),
                        output_path.join(format!("{}.{}", sanitized, ext)),
                    );

                    let writer = output_db.create_topic_writer(topic, full_schema.clone())?;
                    writers.insert(topic.clone(), writer);

                    // Create the sink (without _log_time, we add it separately)
                    let sink = ArrowRowSink::new(schema)?;
                    sinks.insert(topic.clone(), sink);
                    log_times.insert(topic.clone(), Vec::new());
                    pending_counts.insert(topic.clone(), 0);
                    Ok(())
                })(
                );

                if let Err(e) = init_result {
                    eprintln!("Warning: Failed to initialize topic '{}': {}", topic, e);
                    failed_topics.insert(topic.clone());
                    cleanup_failed_topic(
                        topic,
                        &mut writers,
                        &mut sinks,
                        &mut transcoders,
                        &mut log_times,
                        &mut pending_counts,
                        &topic_paths,
                    );
                    continue;
                }
            }

            // Transcode message
            let transcode_result: Result<(), Box<dyn std::error::Error + Send + Sync>> = (|| {
                let sink = sinks.get_mut(topic).unwrap();
                let transcoder = transcoders.get(topic).unwrap();
                let topic_log_times = log_times.get_mut(topic).unwrap();
                let count = pending_counts.get_mut(topic).unwrap();

                transcoder.transcode(&raw_msg.data, sink)?;
                topic_log_times.push(raw_msg.log_time as i64);
                *count += 1;

                // Flush when batch full
                if *count >= BATCH_SIZE {
                    let batch = sink.finish()?;
                    // Add _log_time column
                    let time_array: arrow::array::ArrayRef =
                        Arc::new(arrow::array::TimestampNanosecondArray::from(
                            std::mem::take(topic_log_times),
                        ));
                    let mut columns: Vec<arrow::array::ArrayRef> = batch.columns().to_vec();
                    columns.push(time_array);
                    let mut fields: Vec<Arc<Field>> =
                        batch.schema().fields().iter().cloned().collect();
                    fields.push(Arc::new(Field::new(
                        "_log_time",
                        DataType::Timestamp(TimeUnit::Nanosecond, None),
                        false,
                    )));
                    let schema = Arc::new(Schema::new(fields));
                    let batch = arrow::array::RecordBatch::try_new(schema, columns)?;
                    writers.get_mut(topic).unwrap().write_batch(batch)?;
                    *messages_per_topic.entry(topic.clone()).or_insert(0) += *count;
                    *count = 0;
                }
                Ok(())
            })(
            );

            if let Err(e) = transcode_result {
                eprintln!("Warning: Failed to process topic '{}': {}", topic, e);
                failed_topics.insert(topic.clone());
                cleanup_failed_topic(
                    topic,
                    &mut writers,
                    &mut sinks,
                    &mut transcoders,
                    &mut log_times,
                    &mut pending_counts,
                    &topic_paths,
                );
            }
        }
    }

    // Flush remaining batches (collect topics first to avoid borrow issues)
    let topics_to_flush: Vec<(String, usize)> = pending_counts
        .iter()
        .filter(|(t, c)| **c > 0 && !failed_topics.contains(*t))
        .map(|(t, c)| (t.clone(), *c))
        .collect();

    for (topic, count) in topics_to_flush {
        let flush_result: Result<(), Box<dyn std::error::Error + Send + Sync>> = (|| {
            let sink = sinks.get_mut(&topic).unwrap();
            let topic_log_times = log_times.get_mut(&topic).unwrap();
            let batch = sink.finish()?;
            // Add _log_time column
            let time_array: arrow::array::ArrayRef = Arc::new(
                arrow::array::TimestampNanosecondArray::from(std::mem::take(topic_log_times)),
            );
            let mut columns: Vec<arrow::array::ArrayRef> = batch.columns().to_vec();
            columns.push(time_array);
            let mut fields: Vec<Arc<Field>> = batch.schema().fields().iter().cloned().collect();
            fields.push(Arc::new(Field::new(
                "_log_time",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            )));
            let schema = Arc::new(Schema::new(fields));
            let batch = arrow::array::RecordBatch::try_new(schema, columns)?;
            writers.get_mut(&topic).unwrap().write_batch(batch)?;
            *messages_per_topic.entry(topic.clone()).or_insert(0) += count;
            Ok(())
        })();

        if let Err(e) = flush_result {
            eprintln!("Warning: Failed to flush topic '{}': {}", topic, e);
            failed_topics.insert(topic.clone());
            cleanup_failed_topic(
                &topic,
                &mut writers,
                &mut sinks,
                &mut transcoders,
                &mut log_times,
                &mut pending_counts,
                &topic_paths,
            );
        }
    }

    // Finish all writers (only non-failed topics)
    let topics_to_finish: Vec<String> = writers
        .keys()
        .filter(|t| !failed_topics.contains(*t))
        .cloned()
        .collect();

    for topic in topics_to_finish {
        if let Some(writer) = writers.remove(&topic) {
            if let Err(e) = writer.finish() {
                eprintln!("Warning: Failed to finish topic '{}': {}", topic, e);
                failed_topics.insert(topic.clone());
                // Try to delete the output file
                if let Some(path) = topic_paths.get(&topic) {
                    let _ = fs::remove_file(path);
                }
            }
        }
    }

    output_db.finish()?;

    Ok(ConversionResult {
        messages_per_topic: messages_per_topic.into_iter().collect(),
        failed_topics,
    })
}

/// Internal conversion to LanceDB that returns stats without verbose output
async fn convert_mcap_to_lance_quiet(
    inputs: Vec<PathBuf>,
    output: String,
) -> Result<ConversionResult, Box<dyn std::error::Error + Send + Sync>> {
    let options = ConvertOptions::default();
    let db = connect_with_options(&output, options.data_storage_version)
        .await
        .map_err(|e| format!("Failed to connect to LanceDB at {}: {e}", output))?;

    let mut total_messages: HashMap<String, usize> = HashMap::new();

    for input in &inputs {
        let encoding =
            detect_mcap_encoding(input).map_err(|e| format!("Failed to detect encoding: {}", e))?;
        let file = File::open(input)?;

        let stats = match encoding.as_str() {
            "ros1" => convert_mcap_to_lance_fast(file, &db, options.clone()).await?,
            "protobuf" => convert_protobuf_mcap_to_lance(file, &db, options.clone()).await?,
            "cdr" => convert_cdr_mcap_to_lance(file, &db, options.clone()).await?,
            _ => return Err(format!("Unknown encoding: {}", encoding).into()),
        };

        for (topic, count) in stats.messages_per_topic {
            *total_messages.entry(topic).or_insert(0) += count;
        }
    }

    // LanceDB doesn't have per-topic failure tracking yet (it fails atomically)
    Ok(ConversionResult {
        messages_per_topic: total_messages,
        failed_topics: std::collections::HashSet::new(),
    })
}

async fn run_analyze(args: AnalyzeArgs) -> ExitCode {
    // Validate input is MCAP
    if !args.input.exists() {
        eprintln!("Error: input file does not exist: {}", args.input.display());
        return ExitCode::FAILURE;
    }

    let ext = args.input.extension().and_then(|e| e.to_str());
    if ext != Some("mcap") {
        eprintln!("Error: analyze command only supports MCAP input files");
        return ExitCode::FAILURE;
    }

    // Determine output directory
    let output_dir = args.output.unwrap_or_else(|| {
        let stem = args.input.file_stem().unwrap_or_default().to_string_lossy();
        PathBuf::from(format!("{}_analysis", stem))
    });

    // Clear output directory if it exists
    if output_dir.exists() {
        if let Err(e) = fs::remove_dir_all(&output_dir) {
            eprintln!("Error clearing output directory: {}", e);
            return ExitCode::FAILURE;
        }
    }

    // Create output subdirectories
    let parquet_dir = output_dir.join("parquet");
    let vortex_dir = output_dir.join("vortex");
    let lance_dir = output_dir.join("lance");

    if let Err(e) = fs::create_dir_all(&parquet_dir) {
        eprintln!("Error creating parquet directory: {}", e);
        return ExitCode::FAILURE;
    }
    if let Err(e) = fs::create_dir_all(&vortex_dir) {
        eprintln!("Error creating vortex directory: {}", e);
        return ExitCode::FAILURE;
    }
    if let Err(e) = fs::create_dir_all(&lance_dir) {
        eprintln!("Error creating lance directory: {}", e);
        return ExitCode::FAILURE;
    }

    // Get MCAP file size
    let mcap_size = match fs::metadata(&args.input) {
        Ok(m) => m.len(),
        Err(e) => {
            eprintln!("Error reading input file: {}", e);
            return ExitCode::FAILURE;
        }
    };

    eprintln!(
        "Analyzing: {} ({})",
        args.input.display(),
        format_size(mcap_size)
    );
    eprintln!();
    eprintln!("Converting to Parquet, Vortex, and LanceDB in parallel...");

    // Run conversions in parallel with timing
    let inputs = vec![args.input.clone()];
    let parquet_output = parquet_dir.to_string_lossy().to_string();
    let vortex_output = vortex_dir.to_string_lossy().to_string();
    let lance_output = lance_dir.to_string_lossy().to_string();

    // Wrap each conversion to capture timing
    use std::time::{Duration, Instant};

    async fn timed<T>(fut: impl std::future::Future<Output = T>) -> (T, Duration) {
        let start = Instant::now();
        let result = fut.await;
        (result, start.elapsed())
    }

    let ((parquet_result, parquet_time), (vortex_result, vortex_time), (lance_result, lance_time)) = tokio::join!(
        timed(convert_mcap_to_output_format_quiet(
            inputs.clone(),
            parquet_output,
            OutputFmt::Parquet
        )),
        timed(convert_mcap_to_output_format_quiet(
            inputs.clone(),
            vortex_output,
            OutputFmt::Vortex
        )),
        timed(convert_mcap_to_lance_quiet(inputs.clone(), lance_output)),
    );

    // Track which formats completely failed (vs per-topic failures)
    let parquet_ok = parquet_result.is_ok();
    let vortex_ok = vortex_result.is_ok();
    let lance_ok = lance_result.is_ok();

    // Log complete failures
    if let Err(ref e) = parquet_result {
        eprintln!("Warning: Parquet conversion failed completely: {}", e);
    }
    if let Err(ref e) = vortex_result {
        eprintln!("Warning: Vortex conversion failed completely: {}", e);
    }
    if let Err(ref e) = lance_result {
        eprintln!("Warning: LanceDB conversion failed completely: {}", e);
    }

    // Get conversion results with per-topic failure tracking
    let parquet_conv = parquet_result.unwrap_or(ConversionResult {
        messages_per_topic: HashMap::new(),
        failed_topics: std::collections::HashSet::new(),
    });
    let vortex_conv = vortex_result.unwrap_or(ConversionResult {
        messages_per_topic: HashMap::new(),
        failed_topics: std::collections::HashSet::new(),
    });
    let lance_conv = lance_result.unwrap_or(ConversionResult {
        messages_per_topic: HashMap::new(),
        failed_topics: std::collections::HashSet::new(),
    });

    // Measure sizes
    let parquet_sizes = if parquet_ok {
        measure_per_topic_sizes(&parquet_dir, "parquet").unwrap_or_default()
    } else {
        HashMap::new()
    };

    let vortex_sizes = if vortex_ok {
        measure_per_topic_sizes(&vortex_dir, "vortex").unwrap_or_default()
    } else {
        HashMap::new()
    };

    let lance_sizes = if lance_ok {
        measure_lance_per_topic_sizes(&lance_dir).unwrap_or_default()
    } else {
        HashMap::new()
    };

    // Collect all topics from all conversions (including failed ones for display)
    let mut all_topics: std::collections::HashSet<String> = std::collections::HashSet::new();
    all_topics.extend(parquet_conv.messages_per_topic.keys().cloned());
    all_topics.extend(parquet_conv.failed_topics.iter().cloned());
    all_topics.extend(vortex_conv.messages_per_topic.keys().cloned());
    all_topics.extend(vortex_conv.failed_topics.iter().cloned());
    all_topics.extend(lance_conv.messages_per_topic.keys().cloned());
    all_topics.extend(lance_conv.failed_topics.iter().cloned());
    let mut all_topics: Vec<String> = all_topics.into_iter().collect();
    all_topics.sort();

    if all_topics.is_empty() {
        eprintln!("Error: No topics were found");
        return ExitCode::FAILURE;
    }

    // Calculate totals (only from successful topics)
    let total_messages: usize = parquet_conv
        .messages_per_topic
        .values()
        .sum::<usize>()
        .max(lance_conv.messages_per_topic.values().sum());
    let total_parquet: u64 = parquet_sizes.values().sum();
    let total_vortex: u64 = vortex_sizes.values().sum();
    let total_lance: u64 = lance_sizes.values().sum();

    // Count failures
    let parquet_failures = parquet_conv.failed_topics.len();
    let vortex_failures = vortex_conv.failed_topics.len();
    let lance_failures = lance_conv.failed_topics.len();

    // Print table
    eprintln!();
    eprintln!("Per-Topic Compression Analysis:");

    // Helper to format a multiplier with color (green < 1.0, white = 1.0, red > 1.0)
    // Returns (formatted_string, visible_length) where visible_length excludes ANSI codes
    fn format_multiplier(ratio: f64) -> (String, usize) {
        if ratio <= 0.0 {
            return (String::new(), 0);
        }
        // Color gradient: green (0.5x) -> white (1.0x) -> red (2.0x)
        let (r, g, b) = if ratio < 1.0 {
            // Green to white: ratio 0.5 -> 1.0 maps to (0,255,0) -> (255,255,255)
            let t = ((ratio - 0.5) / 0.5).clamp(0.0, 1.0);
            let r = (255.0 * t) as u8;
            let b = (255.0 * t) as u8;
            (r, 255u8, b)
        } else {
            // White to red: ratio 1.0 -> 2.0 maps to (255,255,255) -> (255,0,0)
            let t = ((ratio - 1.0) / 1.0).clamp(0.0, 1.0);
            let g = (255.0 * (1.0 - t)) as u8;
            let b = (255.0 * (1.0 - t)) as u8;
            (255u8, g, b)
        };
        let visible = format!("{:.2}x", ratio);
        let visible_len = visible.len();
        (
            format!("\x1b[38;2;{};{};{}m{}\x1b[0m", r, g, b, visible),
            visible_len,
        )
    }

    // Calculate column widths
    let topic_width = all_topics.iter().map(|t| t.len()).max().unwrap_or(5).max(5);
    let msg_width = 10;
    let size_width = 18; // Wider to accommodate "1.23 MB (1.23x)"

    // Header
    println!(
        "┌─{:─<tw$}─┬─{:─<mw$}─┬─{:─<sw$}─┬─{:─<sw$}─┬─{:─<sw$}─┐",
        "",
        "",
        "",
        "",
        "",
        tw = topic_width,
        mw = msg_width,
        sw = size_width
    );
    println!(
        "│ {:tw$} │ {:>mw$} │ {:>sw$} │ {:>sw$} │ {:>sw$} │",
        "Topic",
        "Messages",
        "Parquet",
        "Vortex",
        "LanceDB",
        tw = topic_width,
        mw = msg_width,
        sw = size_width
    );
    println!(
        "├─{:─<tw$}─┼─{:─<mw$}─┼─{:─<sw$}─┼─{:─<sw$}─┼─{:─<sw$}─┤",
        "",
        "",
        "",
        "",
        "",
        tw = topic_width,
        mw = msg_width,
        sw = size_width
    );

    // Data rows
    for topic in &all_topics {
        let msgs = parquet_conv
            .messages_per_topic
            .get(topic)
            .or_else(|| lance_conv.messages_per_topic.get(topic))
            .copied()
            .unwrap_or(0);
        // File names are sanitized (e.g., /foo/bar -> foo_bar)
        let sanitized = sanitize_topic_name(topic);

        // Get parquet size as baseline for multiplier
        let pq_size = if parquet_ok && !parquet_conv.failed_topics.contains(topic) {
            parquet_sizes.get(&sanitized).copied().unwrap_or(0)
        } else {
            0
        };

        // Format size with multiplier relative to parquet
        // Parquet column: just the size (it's the baseline)
        let pq_str = if !parquet_ok {
            "FAILED".to_string()
        } else if parquet_conv.failed_topics.contains(topic) {
            "FAILED".to_string()
        } else if pq_size > 0 {
            format_size(pq_size)
        } else {
            "0 B".to_string()
        };

        // Vortex column: size + colored multiplier vs parquet
        let (vx_str, vx_extra_len) = if !vortex_ok {
            ("FAILED".to_string(), 0)
        } else if vortex_conv.failed_topics.contains(topic) {
            ("FAILED".to_string(), 0)
        } else {
            let size = vortex_sizes.get(&sanitized).copied().unwrap_or(0);
            if size > 0 && pq_size > 0 {
                let ratio = size as f64 / pq_size as f64;
                let (mult, mult_visible_len) = format_multiplier(ratio);
                let formatted = format!("{} ({})", format_size(size), mult);
                // Extra length = total string length - visible content length
                let visible_len = format_size(size).len() + 3 + mult_visible_len; // " (" + ")"
                (formatted.clone(), formatted.len() - visible_len)
            } else if size > 0 {
                (format_size(size), 0)
            } else {
                ("0 B".to_string(), 0)
            }
        };

        // LanceDB column: size + colored multiplier vs parquet
        let (lc_str, lc_extra_len) = if !lance_ok {
            ("FAILED".to_string(), 0)
        } else if lance_conv.failed_topics.contains(topic) {
            ("FAILED".to_string(), 0)
        } else {
            let size = lance_sizes.get(&sanitized).copied().unwrap_or(0);
            if size > 0 && pq_size > 0 {
                let ratio = size as f64 / pq_size as f64;
                let (mult, mult_visible_len) = format_multiplier(ratio);
                let formatted = format!("{} ({})", format_size(size), mult);
                // Extra length = total string length - visible content length
                let visible_len = format_size(size).len() + 3 + mult_visible_len; // " (" + ")"
                (formatted.clone(), formatted.len() - visible_len)
            } else if size > 0 {
                (format_size(size), 0)
            } else {
                ("0 B".to_string(), 0)
            }
        };

        // Print with padding adjusted for ANSI escape codes
        println!(
            "│ {:tw$} │ {:>mw$} │ {:>sw$} │ {:>vw$} │ {:>lw$} │",
            topic,
            msgs,
            pq_str,
            vx_str,
            lc_str,
            tw = topic_width,
            mw = msg_width,
            sw = size_width,
            vw = size_width + vx_extra_len,
            lw = size_width + lc_extra_len
        );
    }

    // Totals row with multipliers
    let (total_vx_str, total_vx_extra) = if vortex_ok && parquet_ok && total_parquet > 0 {
        let ratio = total_vortex as f64 / total_parquet as f64;
        let (mult, mult_visible_len) = format_multiplier(ratio);
        let formatted = format!("{} ({})", format_size(total_vortex), mult);
        let visible_len = format_size(total_vortex).len() + 3 + mult_visible_len;
        (formatted.clone(), formatted.len() - visible_len)
    } else if vortex_ok {
        (format_size(total_vortex), 0)
    } else {
        ("FAILED".to_string(), 0)
    };

    let (total_lc_str, total_lc_extra) = if lance_ok && parquet_ok && total_parquet > 0 {
        let ratio = total_lance as f64 / total_parquet as f64;
        let (mult, mult_visible_len) = format_multiplier(ratio);
        let formatted = format!("{} ({})", format_size(total_lance), mult);
        let visible_len = format_size(total_lance).len() + 3 + mult_visible_len;
        (formatted.clone(), formatted.len() - visible_len)
    } else if lance_ok {
        (format_size(total_lance), 0)
    } else {
        ("FAILED".to_string(), 0)
    };

    println!(
        "├─{:─<tw$}─┼─{:─<mw$}─┼─{:─<sw$}─┼─{:─<sw$}─┼─{:─<sw$}─┤",
        "",
        "",
        "",
        "",
        "",
        tw = topic_width,
        mw = msg_width,
        sw = size_width
    );
    println!(
        "│ {:tw$} │ {:>mw$} │ {:>sw$} │ {:>vw$} │ {:>lw$} │",
        "TOTAL",
        total_messages,
        if parquet_ok {
            format_size(total_parquet)
        } else {
            "FAILED".to_string()
        },
        total_vx_str,
        total_lc_str,
        tw = topic_width,
        mw = msg_width,
        sw = size_width,
        vw = size_width + total_vx_extra,
        lw = size_width + total_lc_extra
    );
    println!(
        "└─{:─<tw$}─┴─{:─<mw$}─┴─{:─<sw$}─┴─{:─<sw$}─┴─{:─<sw$}─┘",
        "",
        "",
        "",
        "",
        "",
        tw = topic_width,
        mw = msg_width,
        sw = size_width
    );

    // Compression ratios (only for successful conversions)
    println!();
    println!("Compression Ratios (vs MCAP {}):", format_size(mcap_size));
    if parquet_ok {
        let failure_note = if parquet_failures > 0 {
            format!(" [{} failed]", parquet_failures)
        } else {
            String::new()
        };
        println!(
            "  Parquet: {:.2}x ({}) in {}{}",
            total_parquet as f64 / mcap_size as f64,
            format_size(total_parquet),
            format_duration(parquet_time),
            failure_note
        );
    } else {
        println!("  Parquet: FAILED in {}", format_duration(parquet_time));
    }
    if vortex_ok {
        let failure_note = if vortex_failures > 0 {
            format!(" [{} failed]", vortex_failures)
        } else {
            String::new()
        };
        println!(
            "  Vortex:  {:.2}x ({}) in {}{}",
            total_vortex as f64 / mcap_size as f64,
            format_size(total_vortex),
            format_duration(vortex_time),
            failure_note
        );
    } else {
        println!("  Vortex:  FAILED in {}", format_duration(vortex_time));
    }
    if lance_ok {
        let failure_note = if lance_failures > 0 {
            format!(" [{} failed]", lance_failures)
        } else {
            String::new()
        };
        println!(
            "  LanceDB: {:.2}x ({}) in {}{}",
            total_lance as f64 / mcap_size as f64,
            format_size(total_lance),
            format_duration(lance_time),
            failure_note
        );
    } else {
        println!("  LanceDB: FAILED in {}", format_duration(lance_time));
    }

    println!();
    println!("Output written to: {}", output_dir.display());

    // Clean up if requested
    if args.clean {
        if let Err(e) = fs::remove_dir_all(&output_dir) {
            eprintln!("Warning: failed to clean up output directory: {}", e);
        } else {
            println!("Output directory cleaned up.");
        }
    }

    ExitCode::SUCCESS
}
