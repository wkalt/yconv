//! Interactive SQL shell with psql-like interface.
//!
//! Provides a unified SQL interface to various data formats using DuckDB as the query engine.

use std::path::Path;

use duckdb::{Config, Connection};
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use thiserror::Error;

/// Errors that can occur during shell operations.
#[derive(Debug, Error)]
pub enum ShellError {
    #[error("DuckDB error: {0}")]
    DuckDb(#[from] duckdb::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Unsupported format: {0}")]
    UnsupportedFormat(String),

    #[error("No tables found in {0}")]
    NoTables(String),

    #[error("LanceDB error: {0}")]
    Lance(#[from] lancedb::Error),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
}

/// Data source format for the shell.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShellFormat {
    Parquet,
    Vortex,
    Lance,
    DuckDb,
}

/// Detect the format of a data source from its path.
pub fn detect_format(path: &Path) -> Result<ShellFormat, ShellError> {
    if path.is_file() {
        let ext = path
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("");

        match ext {
            "parquet" => return Ok(ShellFormat::Parquet),
            "vortex" => return Ok(ShellFormat::Vortex),
            "duckdb" | "db" => return Ok(ShellFormat::DuckDb),
            _ => {}
        }
    }

    if path.is_dir() {
        // Check for Lance dataset (has _versions directory inside)
        if path.join("_versions").is_dir() {
            return Ok(ShellFormat::Lance);
        }

        // Check for LanceDB database (directory containing .lance table directories)
        let has_lance_tables = std::fs::read_dir(path)?
            .filter_map(|e| e.ok())
            .any(|e| {
                let p = e.path();
                p.is_dir() && p.extension().map(|ext| ext == "lance").unwrap_or(false)
            });
        if has_lance_tables {
            return Ok(ShellFormat::Lance);
        }

        // Check for directory of parquet files
        let has_parquet = std::fs::read_dir(path)?
            .filter_map(|e| e.ok())
            .any(|e| {
                e.path()
                    .extension()
                    .map(|ext| ext == "parquet")
                    .unwrap_or(false)
            });
        if has_parquet {
            return Ok(ShellFormat::Parquet);
        }

        // Check for directory of vortex files
        let has_vortex = std::fs::read_dir(path)?
            .filter_map(|e| e.ok())
            .any(|e| {
                e.path()
                    .extension()
                    .map(|ext| ext == "vortex")
                    .unwrap_or(false)
            });
        if has_vortex {
            return Ok(ShellFormat::Vortex);
        }
    }

    Err(ShellError::UnsupportedFormat(
        path.display().to_string(),
    ))
}

/// Interactive SQL shell.
pub struct Shell {
    conn: Connection,
    tables: Vec<String>,
}

impl Shell {
    /// Open a shell for the given data source.
    pub fn open(path: &Path) -> Result<Self, ShellError> {
        let format = detect_format(path)?;

        // Create in-memory DuckDB connection with unsigned extensions allowed
        let config = Config::default()
            .allow_unsigned_extensions()?;
        let conn = Connection::open_in_memory_with_flags(config)?;

        let tables = match format {
            ShellFormat::DuckDb => Self::load_duckdb(&conn, path)?,
            ShellFormat::Parquet => Self::load_parquet(&conn, path)?,
            ShellFormat::Vortex => Self::load_vortex(&conn, path)?,
            ShellFormat::Lance => Self::load_lance(&conn, path)?,
        };

        if tables.is_empty() {
            return Err(ShellError::NoTables(path.display().to_string()));
        }

        Ok(Self { conn, tables })
    }

    /// Load tables from a DuckDB file.
    fn load_duckdb(conn: &Connection, path: &Path) -> Result<Vec<String>, ShellError> {
        // Attach the database file
        let attach_sql = format!("ATTACH '{}' AS source (READ_ONLY)", path.display());
        conn.execute(&attach_sql, [])?;

        // Get table names from the attached database
        let mut stmt = conn.prepare(
            "SELECT table_name FROM information_schema.tables WHERE table_catalog = 'source' AND table_schema = 'main'"
        )?;
        let table_names: Vec<String> = stmt
            .query_map([], |row| row.get(0))?
            .filter_map(|r| r.ok())
            .collect();

        // Create views in main schema for clean table names
        for name in &table_names {
            let view_sql = format!(
                "CREATE VIEW \"{}\" AS SELECT * FROM source.main.\"{}\"",
                name, name
            );
            conn.execute(&view_sql, [])?;
        }

        Ok(table_names)
    }

    /// Load tables from Parquet file(s).
    fn load_parquet(conn: &Connection, path: &Path) -> Result<Vec<String>, ShellError> {
        let mut tables = Vec::new();

        if path.is_file() {
            // Single parquet file
            let table_name = path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("data");

            let view_sql = format!(
                "CREATE VIEW \"{}\" AS SELECT * FROM read_parquet('{}')",
                table_name,
                path.display()
            );
            conn.execute(&view_sql, [])?;
            tables.push(table_name.to_string());
        } else {
            // Directory of parquet files
            for entry in std::fs::read_dir(path)? {
                let entry = entry?;
                let file_path = entry.path();
                if file_path.extension().map(|e| e == "parquet").unwrap_or(false) {
                    let table_name = file_path
                        .file_stem()
                        .and_then(|s| s.to_str())
                        .unwrap_or("data");

                    let view_sql = format!(
                        "CREATE VIEW \"{}\" AS SELECT * FROM read_parquet('{}')",
                        table_name,
                        file_path.display()
                    );
                    conn.execute(&view_sql, [])?;
                    tables.push(table_name.to_string());
                }
            }
        }

        tables.sort();
        Ok(tables)
    }

    /// Load tables from Vortex file(s).
    fn load_vortex(conn: &Connection, path: &Path) -> Result<Vec<String>, ShellError> {
        // Install and load vortex extension (core extension, not community)
        eprintln!("Installing vortex extension...");
        if let Err(e) = conn.execute("INSTALL vortex", []) {
            return Err(ShellError::UnsupportedFormat(format!(
                "Vortex extension not available for this platform. Error: {}",
                e
            )));
        }
        if let Err(e) = conn.execute("LOAD vortex", []) {
            return Err(ShellError::UnsupportedFormat(format!(
                "Failed to load vortex extension: {}",
                e
            )));
        }
        eprintln!("Vortex extension loaded.");

        let mut tables = Vec::new();

        if path.is_file() {
            // Single vortex file
            let table_name = path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("data");

            let view_sql = format!(
                "CREATE VIEW \"{}\" AS SELECT * FROM read_vortex('{}')",
                table_name,
                path.display()
            );
            conn.execute(&view_sql, [])?;
            tables.push(table_name.to_string());
        } else {
            // Directory of vortex files
            for entry in std::fs::read_dir(path)? {
                let entry = entry?;
                let file_path = entry.path();
                if file_path.extension().map(|e| e == "vortex").unwrap_or(false) {
                    let table_name = file_path
                        .file_stem()
                        .and_then(|s| s.to_str())
                        .unwrap_or("data");

                    let view_sql = format!(
                        "CREATE VIEW \"{}\" AS SELECT * FROM read_vortex('{}')",
                        table_name,
                        file_path.display()
                    );
                    conn.execute(&view_sql, [])?;
                    tables.push(table_name.to_string());
                }
            }
        }

        tables.sort();
        Ok(tables)
    }

    /// Load tables from a Lance dataset.
    fn load_lance(conn: &Connection, path: &Path) -> Result<Vec<String>, ShellError> {
        // Install and load lance extension from community repository
        eprintln!("Installing lance extension...");
        if let Err(e) = conn.execute("INSTALL lance FROM community", []) {
            return Err(ShellError::UnsupportedFormat(format!(
                "Lance extension not available for this platform. Error: {}",
                e
            )));
        }
        if let Err(e) = conn.execute("LOAD lance", []) {
            return Err(ShellError::UnsupportedFormat(format!(
                "Failed to load lance extension: {}",
                e
            )));
        }
        eprintln!("Lance extension loaded.");

        let mut tables = Vec::new();

        // Check if this is a LanceDB database (directory of .lance tables)
        // or a single Lance table
        if path.join("_versions").is_dir() {
            // Single Lance table
            let table_name = path
                .file_name()
                .and_then(|s| s.to_str())
                .map(|s| s.trim_end_matches(".lance"))
                .unwrap_or("data");

            let view_sql = format!(
                "CREATE VIEW \"{}\" AS SELECT * FROM '{}'",
                table_name,
                path.display()
            );
            conn.execute(&view_sql, [])?;
            tables.push(table_name.to_string());
        } else {
            // LanceDB database - scan for .lance subdirectories
            for entry in std::fs::read_dir(path)? {
                let entry = entry?;
                let entry_path = entry.path();
                if entry_path.is_dir() {
                    if let Some(ext) = entry_path.extension() {
                        if ext == "lance" {
                            // Extract table name from directory name (without .lance extension)
                            let Some(table_name) = entry_path
                                .file_stem()
                                .and_then(|s| s.to_str()) else {
                                continue;
                            };

                            if table_name.is_empty() {
                                continue;
                            }

                            let view_sql = format!(
                                "CREATE VIEW \"{}\" AS SELECT * FROM '{}'",
                                table_name,
                                entry_path.display()
                            );
                            if let Err(e) = conn.execute(&view_sql, []) {
                                eprintln!("Warning: Failed to load table {}: {}", table_name, e);
                                continue;
                            }
                            tables.push(table_name.to_string());
                        }
                    }
                }
            }
        }

        tables.sort();
        Ok(tables)
    }

    /// Run the interactive shell.
    pub fn run(&self) -> Result<(), ShellError> {
        println!("Loaded {} table(s)", self.tables.len());
        println!("Type \\h for help, \\q to quit\n");

        let mut rl = DefaultEditor::new().expect("Failed to create editor");

        loop {
            match rl.readline("yconv> ") {
                Ok(line) => {
                    let line = line.trim();
                    if line.is_empty() {
                        continue;
                    }

                    let _ = rl.add_history_entry(line);

                    if line.starts_with('\\') {
                        if self.handle_meta_command(line) {
                            break;
                        }
                    } else {
                        self.execute_query(line);
                    }
                }
                Err(ReadlineError::Interrupted) => {
                    println!("^C");
                    continue;
                }
                Err(ReadlineError::Eof) => {
                    println!("\\q");
                    break;
                }
                Err(err) => {
                    eprintln!("Error: {:?}", err);
                    break;
                }
            }
        }

        Ok(())
    }

    /// Handle a meta-command (starts with \).
    /// Returns true if the shell should exit.
    fn handle_meta_command(&self, cmd: &str) -> bool {
        let parts: Vec<&str> = cmd.split_whitespace().collect();
        let cmd = parts.first().map(|s| *s).unwrap_or("");

        match cmd {
            "\\q" | "\\quit" => return true,

            "\\h" | "\\help" | "\\?" => {
                println!("Meta-commands:");
                println!("  \\dt           List tables");
                println!("  \\dt+ or \\d+   List tables with sizes");
                println!("  \\d <table>    Describe table schema");
                println!("  \\h            Show this help");
                println!("  \\q            Quit");
                println!();
                println!("Or enter SQL queries directly.");
            }

            "\\dt" | "\\tables" => {
                self.list_tables();
            }

            "\\dt+" | "\\d+" => {
                self.list_tables_extended();
            }

            "\\d" | "\\describe" => {
                if parts.len() < 2 {
                    println!("Usage: \\d <table_name>");
                } else {
                    let table_name = parts[1];
                    self.describe_table(table_name);
                }
            }

            _ => {
                println!("Unknown command: {}", cmd);
                println!("Type \\h for help");
            }
        }

        false
    }

    /// Describe a table's schema.
    fn describe_table(&self, table_name: &str) {
        let query = format!("DESCRIBE \"{}\"", table_name);
        self.execute_query(&query);
    }

    /// List tables in psql format.
    fn list_tables(&self) {
        if self.tables.is_empty() {
            println!("No tables found.");
            return;
        }

        // Calculate column widths
        let max_name = self.tables.iter().map(|t| t.len()).max().unwrap_or(4).max(4);

        // Print header
        println!("        List of relations");
        println!(" {:^max_name$} | Type  ", "Name", max_name = max_name);
        println!("-{}-+-------", "-".repeat(max_name));

        // Print rows
        for table in &self.tables {
            println!(" {:max_name$} | table ", table, max_name = max_name);
        }

        println!("({} row{})", self.tables.len(), if self.tables.len() == 1 { "" } else { "s" });
    }

    /// List tables with sizes in psql format.
    fn list_tables_extended(&self) {
        if self.tables.is_empty() {
            println!("No tables found.");
            return;
        }

        // Collect data first to calculate column widths
        let mut rows: Vec<(String, String, String)> = Vec::new();
        for table in &self.tables {
            let row_count = self.get_row_count(table);
            let size = self.estimate_table_size(table);
            rows.push((table.clone(), row_count, size));
        }

        // Calculate column widths
        let max_name = rows.iter().map(|(n, _, _)| n.len()).max().unwrap_or(4).max(4);
        let max_rows = rows.iter().map(|(_, r, _)| r.len()).max().unwrap_or(4).max(4);
        let max_size = rows.iter().map(|(_, _, s)| s.len()).max().unwrap_or(4).max(4);

        // Print header
        println!("                    List of relations");
        println!(
            " {:^max_name$} | Type  | {:^max_rows$} | {:^max_size$} ",
            "Name", "Rows", "Size",
            max_name = max_name, max_rows = max_rows, max_size = max_size
        );
        println!(
            "-{}-+-------+-{}-+-{}-",
            "-".repeat(max_name), "-".repeat(max_rows), "-".repeat(max_size)
        );

        // Print rows
        for (name, row_count, size) in &rows {
            println!(
                " {:max_name$} | table | {:>max_rows$} | {:>max_size$} ",
                name, row_count, size,
                max_name = max_name, max_rows = max_rows, max_size = max_size
            );
        }

        println!("({} row{})", self.tables.len(), if self.tables.len() == 1 { "" } else { "s" });
    }

    /// Get row count for a table.
    fn get_row_count(&self, table: &str) -> String {
        let query = format!("SELECT COUNT(*) FROM \"{}\"", table);
        match self.conn.prepare(&query) {
            Ok(mut stmt) => {
                match stmt.query_arrow([]) {
                    Ok(result) => {
                        let batches: Vec<arrow::array::RecordBatch> = result.collect();
                        if !batches.is_empty() && batches[0].num_rows() > 0 {
                            format_arrow_value(batches[0].column(0), 0)
                        } else {
                            "?".to_string()
                        }
                    }
                    Err(_) => "?".to_string(),
                }
            }
            Err(_) => "?".to_string(),
        }
    }

    /// Estimate table size in human-readable format.
    fn estimate_table_size(&self, table: &str) -> String {
        // Try to get estimated size using DuckDB's pragma_storage_info
        let query = format!(
            "SELECT SUM(compressed_size) as size FROM pragma_storage_info('{}')",
            table
        );

        match self.conn.prepare(&query) {
            Ok(mut stmt) => {
                match stmt.query_arrow([]) {
                    Ok(result) => {
                        let batches: Vec<arrow::array::RecordBatch> = result.collect();
                        if !batches.is_empty() && batches[0].num_rows() > 0 {
                            let col = batches[0].column(0);
                            if !col.is_null(0) {
                                if let Some(size) = extract_size_value(col) {
                                    return format_size(size);
                                }
                            }
                        }
                        // No storage info available (views over external data)
                        String::new()
                    }
                    Err(_) => String::new(),
                }
            }
            Err(_) => String::new(),
        }
    }

    /// Execute a SQL query and print results.
    fn execute_query(&self, query: &str) {
        let query_upper = query.trim().to_uppercase();

        // Check if this is a statement that doesn't return results
        let is_non_query = query_upper.starts_with("CREATE")
            || query_upper.starts_with("DROP")
            || query_upper.starts_with("INSERT")
            || query_upper.starts_with("UPDATE")
            || query_upper.starts_with("DELETE")
            || query_upper.starts_with("COPY")
            || query_upper.starts_with("INSTALL")
            || query_upper.starts_with("LOAD")
            || query_upper.starts_with("SET")
            || query_upper.starts_with("ATTACH")
            || query_upper.starts_with("DETACH");

        if is_non_query {
            match self.conn.execute(query, []) {
                Ok(rows_affected) => {
                    if rows_affected > 0 {
                        println!("OK, {} row(s) affected", rows_affected);
                    } else {
                        println!("OK");
                    }
                }
                Err(e) => {
                    eprintln!("Error: {}", e);
                }
            }
            return;
        }

        // For queries that return results, use query_arrow
        match self.conn.prepare(query) {
            Ok(mut stmt) => {
                match stmt.query_arrow([]) {
                    Ok(arrow_result) => {
                        let batches: Vec<arrow::array::RecordBatch> = arrow_result.collect();

                        if batches.is_empty() {
                            println!("(0 rows)");
                            return;
                        }

                        let schema = batches[0].schema();
                        let column_names: Vec<String> = schema
                            .fields()
                            .iter()
                            .map(|f| f.name().to_string())
                            .collect();

                        // Collect all values to calculate column widths
                        let mut all_rows: Vec<Vec<String>> = Vec::new();
                        for batch in &batches {
                            for row_idx in 0..batch.num_rows() {
                                let values: Vec<String> = (0..batch.num_columns())
                                    .map(|col_idx| {
                                        format_arrow_value(batch.column(col_idx), row_idx)
                                    })
                                    .collect();
                                all_rows.push(values);
                            }
                        }

                        // Calculate column widths
                        let mut widths: Vec<usize> = column_names.iter().map(|n| n.len()).collect();
                        for row in &all_rows {
                            for (i, val) in row.iter().enumerate() {
                                if i < widths.len() {
                                    widths[i] = widths[i].max(val.len());
                                }
                            }
                        }

                        // Print header
                        let header: Vec<String> = column_names
                            .iter()
                            .zip(&widths)
                            .map(|(n, w)| format!(" {:^width$} ", n, width = *w))
                            .collect();
                        println!("{}", header.join("|"));

                        // Print separator
                        let sep: Vec<String> = widths.iter().map(|w| "-".repeat(*w + 2)).collect();
                        println!("{}", sep.join("+"));

                        // Print rows
                        for row in &all_rows {
                            let formatted: Vec<String> = row
                                .iter()
                                .zip(&widths)
                                .map(|(v, w)| format!(" {:>width$} ", v, width = *w))
                                .collect();
                            println!("{}", formatted.join("|"));
                        }

                        println!("({} row{})", all_rows.len(), if all_rows.len() == 1 { "" } else { "s" });
                    }
                    Err(e) => {
                        eprintln!("Query error: {}", e);
                    }
                }
            }
            Err(e) => {
                eprintln!("Prepare error: {}", e);
            }
        }
    }
}

/// Format a value from an Arrow array.
fn format_arrow_value(array: &arrow::array::ArrayRef, row: usize) -> String {
    use arrow::array::*;
    use arrow::datatypes::DataType;

    if array.is_null(row) {
        return "NULL".to_string();
    }

    match array.data_type() {
        DataType::Boolean => {
            array.as_any().downcast_ref::<BooleanArray>()
                .map(|a| if a.value(row) { "t" } else { "f" }.to_string())
                .unwrap_or_else(|| "?".to_string())
        }
        DataType::Int8 => {
            array.as_any().downcast_ref::<Int8Array>()
                .map(|a| a.value(row).to_string())
                .unwrap_or_else(|| "?".to_string())
        }
        DataType::Int16 => {
            array.as_any().downcast_ref::<Int16Array>()
                .map(|a| a.value(row).to_string())
                .unwrap_or_else(|| "?".to_string())
        }
        DataType::Int32 => {
            array.as_any().downcast_ref::<Int32Array>()
                .map(|a| a.value(row).to_string())
                .unwrap_or_else(|| "?".to_string())
        }
        DataType::Int64 => {
            array.as_any().downcast_ref::<Int64Array>()
                .map(|a| a.value(row).to_string())
                .unwrap_or_else(|| "?".to_string())
        }
        DataType::UInt8 => {
            array.as_any().downcast_ref::<UInt8Array>()
                .map(|a| a.value(row).to_string())
                .unwrap_or_else(|| "?".to_string())
        }
        DataType::UInt16 => {
            array.as_any().downcast_ref::<UInt16Array>()
                .map(|a| a.value(row).to_string())
                .unwrap_or_else(|| "?".to_string())
        }
        DataType::UInt32 => {
            array.as_any().downcast_ref::<UInt32Array>()
                .map(|a| a.value(row).to_string())
                .unwrap_or_else(|| "?".to_string())
        }
        DataType::UInt64 => {
            array.as_any().downcast_ref::<UInt64Array>()
                .map(|a| a.value(row).to_string())
                .unwrap_or_else(|| "?".to_string())
        }
        DataType::Float32 => {
            array.as_any().downcast_ref::<Float32Array>()
                .map(|a| a.value(row).to_string())
                .unwrap_or_else(|| "?".to_string())
        }
        DataType::Float64 => {
            array.as_any().downcast_ref::<Float64Array>()
                .map(|a| a.value(row).to_string())
                .unwrap_or_else(|| "?".to_string())
        }
        DataType::Utf8 => {
            array.as_any().downcast_ref::<StringArray>()
                .map(|a| a.value(row).to_string())
                .unwrap_or_else(|| "?".to_string())
        }
        DataType::LargeUtf8 => {
            array.as_any().downcast_ref::<LargeStringArray>()
                .map(|a| a.value(row).to_string())
                .unwrap_or_else(|| "?".to_string())
        }
        DataType::Binary => {
            array.as_any().downcast_ref::<BinaryArray>()
                .map(|a| {
                    let bytes = a.value(row);
                    if bytes.len() > 16 {
                        format!("<{} bytes>", bytes.len())
                    } else {
                        format!("\\x{}", bytes.iter().map(|b| format!("{:02x}", b)).collect::<String>())
                    }
                })
                .unwrap_or_else(|| "?".to_string())
        }
        DataType::LargeBinary => {
            array.as_any().downcast_ref::<LargeBinaryArray>()
                .map(|a| format!("<{} bytes>", a.value(row).len()))
                .unwrap_or_else(|| "?".to_string())
        }
        DataType::Timestamp(_unit, _) => {
            array.as_any().downcast_ref::<TimestampNanosecondArray>()
                .map(|a| {
                    let nanos = a.value(row);
                    let secs = nanos / 1_000_000_000;
                    let nsec = (nanos % 1_000_000_000) as u32;
                    format!("{}.{:09}", secs, nsec)
                })
                .or_else(|| {
                    array.as_any().downcast_ref::<TimestampMicrosecondArray>()
                        .map(|a| {
                            let micros = a.value(row);
                            let secs = micros / 1_000_000;
                            let usec = (micros % 1_000_000) as u32;
                            format!("{}.{:06}", secs, usec)
                        })
                })
                .or_else(|| {
                    array.as_any().downcast_ref::<TimestampMillisecondArray>()
                        .map(|a| {
                            let millis = a.value(row);
                            let secs = millis / 1000;
                            let msec = (millis % 1000) as u32;
                            format!("{}.{:03}", secs, msec)
                        })
                })
                .or_else(|| {
                    array.as_any().downcast_ref::<TimestampSecondArray>()
                        .map(|a| a.value(row).to_string())
                })
                .unwrap_or_else(|| "?".to_string())
        }
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => {
            // For lists, show a compact representation
            "{...}".to_string()
        }
        DataType::Struct(_) => {
            // For structs, show a compact representation
            "{...}".to_string()
        }
        DataType::Map(_, _) => {
            "{...}".to_string()
        }
        _ => format!("({})", array.data_type()),
    }
}

/// Extract a size value from an Arrow array (handles various int types).
fn extract_size_value(array: &arrow::array::ArrayRef) -> Option<u64> {
    use arrow::array::*;
    use arrow::datatypes::DataType;

    if array.is_null(0) {
        return None;
    }

    match array.data_type() {
        DataType::Int8 => array.as_any().downcast_ref::<Int8Array>().map(|a| a.value(0) as u64),
        DataType::Int16 => array.as_any().downcast_ref::<Int16Array>().map(|a| a.value(0) as u64),
        DataType::Int32 => array.as_any().downcast_ref::<Int32Array>().map(|a| a.value(0) as u64),
        DataType::Int64 => array.as_any().downcast_ref::<Int64Array>().map(|a| a.value(0) as u64),
        DataType::UInt8 => array.as_any().downcast_ref::<UInt8Array>().map(|a| a.value(0) as u64),
        DataType::UInt16 => array.as_any().downcast_ref::<UInt16Array>().map(|a| a.value(0) as u64),
        DataType::UInt32 => array.as_any().downcast_ref::<UInt32Array>().map(|a| a.value(0) as u64),
        DataType::UInt64 => array.as_any().downcast_ref::<UInt64Array>().map(|a| a.value(0)),
        DataType::Decimal128(_, _) => array.as_any().downcast_ref::<Decimal128Array>().map(|a| a.value(0) as u64),
        _ => None,
    }
}

/// Format a byte size in human-readable format.
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
