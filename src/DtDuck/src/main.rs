use arrow::array::RecordBatch;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use clap::Parser;
use duckdb::{Connection};
use std::fs::File;
use std::io::stdout;
use std::process::{Command, Stdio};

#[derive(Parser, Debug)]
#[command(name = "DtDuck")]
#[command(about = "DtPipe SQL Engine powered by DuckDB — High Performance SQL", long_about = None)]
struct Args {
    /// Input datasets (format: alias="provider:locator")
    #[arg(short = 'i', long = "in", value_parser = parse_alias_arg)]
    inputs: Vec<(String, String, String)>,

    /// SQL query to execute
    #[arg(short = 'q', long)]
    query: String,

    /// Output destination (format: "provider:locator" or omit for console)
    #[arg(short = 'o', long)]
    out: Option<String>,
}

/// Parse an argument of the form `alias="provider:locator"`
fn parse_alias_arg(s: &str) -> Result<(String, String, String), String> {
    let parts: Vec<&str> = s.splitn(2, '=').collect();
    if parts.len() != 2 {
        return Err(format!("Expected 'alias=provider:locator', got '{}'", s));
    }

    let alias = parts[0].to_string();
    let provider_locator = parts[1];

    let pl_parts: Vec<&str> = provider_locator.splitn(2, ':').collect();
    if pl_parts.len() != 2 {
        return Err(format!(
            "Expected 'provider:locator', got '{}'",
            provider_locator
        ));
    }

    Ok((alias, pl_parts[0].to_string(), pl_parts[1].to_string()))
}

/// Parse `provider:locator` for output
fn parse_provider_locator(s: &str) -> Result<(String, String), String> {
    let parts: Vec<&str> = s.splitn(2, ':').collect();
    if parts.len() != 2 {
        return Err(format!("Expected 'provider:locator', got '{}'", s));
    }
    Ok((parts[0].to_string(), parts[1].to_string()))
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    
    // Create an in-memory DuckDB connection
    let conn = Connection::open_in_memory()?;
    
    // Register inputs
    for (alias, provider, locator) in &args.inputs {
        match provider.as_str() {
            "csv" => {
                conn.execute(&format!("CREATE VIEW \"{}\" AS SELECT * FROM read_csv_auto('{}')", alias, locator), [])?;
            }
            "parquet" => {
                conn.execute(&format!("CREATE VIEW \"{}\" AS SELECT * FROM read_parquet('{}')", alias, locator), [])?;
            }
            "ipc" => {
                conn.execute(&format!("CREATE VIEW \"{}\" AS SELECT * FROM read_ipc('{}')", alias, locator), [])?;
            }
            "proc" => {
                let proc_args: Vec<&str> = locator.split_whitespace().collect();
                let mut child = Command::new(proc_args[0])
                    .args(&proc_args[1..])
                    .stdout(Stdio::piped())
                    .spawn()?;
                
                let stdout_pipe = child.stdout.take().expect("Failed to capture stdout");
                let mut reader = StreamReader::try_new(stdout_pipe, None)?;
                
                // Create table schema from Arrow
                let mut sql = format!("CREATE TABLE \"{}\" (", alias);
                for (i, field) in reader.schema().fields().iter().enumerate() {
                    if i > 0 { sql.push_str(", "); }
                    sql.push_str(&format!("\"{}\" ", field.name()));
                    let dt = field.data_type();
                    if dt.is_integer() { sql.push_str("BIGINT"); }
                    else if dt.is_floating() { sql.push_str("DOUBLE"); }
                    else { sql.push_str("VARCHAR"); }
                }
                sql.push_str(")");
                conn.execute(&sql, [])?;

                // STREAMING ingestion into DuckDB via Appender
                // Note: We slice batches to DuckDB's standard vector size (2048) to avoid panics
                let mut appender = conn.appender(&alias)?;
                while let Some(batch_result) = reader.next() {
                    let batch = batch_result?;
                    let mut offset = 0;
                    let batch_len = batch.num_rows();
                    while offset < batch_len {
                        let len = std::cmp::min(1024, batch_len - offset); // Using 1024 for safety
                        let slice = batch.slice(offset, len);
                        appender.append_record_batch(slice)?;
                        offset += len;
                    }
                }
                drop(appender);
                
                let _ = child.wait()?;
            }
            _ => return Err(format!("Unknown provider: {}", provider).into()),
        }
    }

    // Execute query
    if let Some(out_arg) = args.out {
        let (out_provider, out_locator) = parse_provider_locator(&out_arg)?;
        let mut stmt = conn.prepare(&args.query)?;

        match out_provider.as_str() {
            "csv" => {
                let arrow_result = stmt.query_arrow([])?;
                let file = File::create(out_locator)?;
                let mut writer = arrow::csv::Writer::new(file);
                for batch in arrow_result {
                    writer.write(&batch)?;
                }
            }
            "parquet" => {
                conn.execute(&format!("COPY ({}) TO '{}' (FORMAT PARQUET)", args.query, out_locator), [])?;
            }
            "proc" | "arrow" => {
                if out_locator == "-" {
                    let arrow_result = stmt.query_arrow([])?;
                    let mut writer = StreamWriter::try_new(stdout(), &arrow_result.get_schema())?;
                    for batch in arrow_result {
                        writer.write(&batch)?;
                    }
                    writer.finish()?;
                } else {
                    return Err("Use proc:- or arrow:- for stdout".into());
                }
            }
            _ => return Err(format!("Unknown output provider: {}", out_provider).into()),
        }
    } else {
        // Console print
        let mut stmt = conn.prepare(&args.query)?;
        let n_cols = stmt.column_count();
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            for i in 0..n_cols {
                let val: Result<String, _> = row.get(i);
                match val {
                    Ok(s) => print!("{}\t", s),
                    Err(_) => print!("NULL\t"),
                }
            }
            println!();
        }
    }

    Ok(())
}
