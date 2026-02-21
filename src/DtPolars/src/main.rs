use clap::Parser;
use polars::prelude::*;
use polars::sql::SQLContext;
use std::fs::File;
use std::io::stdout;
use std::process::{Command, Stdio};

#[derive(Parser, Debug)]
#[command(name = "DtPolars")]
#[command(about = "DtPipe SQL Engine powered by Polars", long_about = None)]
struct Args {
    /// Input datasets (format: alias="provider:locator")
    #[arg(short = 'i', long = "in", value_parser = parse_alias_arg)]
    inputs: Vec<(String, String, String)>, // (alias, provider, locator)

    /// SQL query to execute
    #[arg(short = 'q', long)]
    query: String,

    /// Output destination (format: "provider:locator" or "STDOUT")
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

/// Parse independent `provider:locator` (e.g., for output)
fn parse_provider_locator(s: &str) -> Result<(String, String), String> {
    let parts: Vec<&str> = s.splitn(2, ':').collect();
    if parts.len() != 2 {
        return Err(format!("Expected 'provider:locator', got '{}'", s));
    }
    Ok((parts[0].to_string(), parts[1].to_string()))
}

/// Start a subprocess (e.g., dtpipe) and return an Arrow IPC Stream reader
fn read_from_proc(command_str: &str) -> Result<DataFrame, Box<dyn std::error::Error>> {
    println!("Spawn [proc]: {}", command_str);
    let args: Vec<&str> = command_str.split_whitespace().collect();
    if args.is_empty() {
        return Err("Empty proc command".into());
    }

    let mut child = Command::new(args[0])
        .args(&args[1..])
        .stdout(Stdio::piped())
        .spawn()?;

    let stdout_pipe = child.stdout.take().expect("Failed to capture stdout");
    let df = IpcStreamReader::new(stdout_pipe).finish()?;

    // Wait for the proc to finish 
    let _ = child.wait()?;
    Ok(df)
}

/// Map a provider string to a Polars LazyFrame
fn load_input(provider: &str, locator: &str) -> Result<LazyFrame, Box<dyn std::error::Error>> {
    match provider {
        "proc" => Ok(read_from_proc(locator)?.lazy()),
        "csv" => Ok(LazyCsvReader::new(locator).has_header(true).finish()?),
        "parquet" => Ok(LazyFrame::scan_parquet(locator, ScanArgsParquet::default())?),
        "ipc" => Ok(LazyFrame::scan_ipc(locator, ScanArgsIpc::default())?),
        _ => Err(format!("Unsupported input provider: {}", provider).into()),
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let mut sql_context = SQLContext::new();

    // 1. Process All Inputs
    for (alias, provider, locator) in args.inputs {
        println!("Loading [{}] from {}:{}", alias, provider, locator);
        let lf = load_input(&provider, &locator)?;
        sql_context.register(&alias, lf);
    }

    // 2. Execute SQL Query
    println!("Executing Query: {}", args.query);
    let result_lazy = sql_context.execute(&args.query)?;
    let mut result_df = result_lazy.collect()?;

    // 3. Handle Output
    if let Some(out_arg) = args.out {
        let (out_provider, out_locator) = parse_provider_locator(&out_arg)?;

        match out_provider.as_str() {
            "parquet" => {
                println!("Writing Parquet to {}", out_locator);
                let file = File::create(&out_locator)?;
                ParquetWriter::new(file).finish(&mut result_df)?;
            }
            "csv" => {
                println!("Writing CSV to {}", out_locator);
                let mut file = File::create(&out_locator)?;
                CsvWriter::new(&mut file).include_header(true).finish(&mut result_df)?;
            }
            "ipc" => {
                println!("Writing Arrow IPC File to {}", out_locator);
                let file = File::create(&out_locator)?;
                IpcWriter::new(file).finish(&mut result_df)?;
            }
            "proc" | "arrow" => {
                // For now, if someone wants to pipe out, they emit IPC Stream to stdout
                // If it's `proc:-` or `arrow:-` it goes to standard out.
                if out_locator == "-" {
                    IpcStreamWriter::new(stdout()).finish(&mut result_df)?;
                } else {
                    return Err(format!("Writing to a named proc/pipe is not yet natively supported. Use proc:- to pipe to STDOUT").into());
                }
            }
            _ => return Err(format!("Unsupported output provider: {}", out_provider).into()),
        }
    } else {
        // Fallback to console print
        println!("Result:\n{}", result_df);
    }

    Ok(())
}
