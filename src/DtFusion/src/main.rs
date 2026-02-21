use clap::Parser;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::ipc::writer::{FileWriter, StreamWriter};
use datafusion::arrow::csv as arrow_csv;
use datafusion::datasource::streaming::StreamingTable;
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion::datasource::MemTable;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::context::TaskContext;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::file::properties::WriterProperties;
use datafusion::prelude::SessionConfig;
use datafusion::prelude::*;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::StreamExt;
use std::fs::File;
use std::io::{stdout, BufWriter};
use std::process::{Command, Stdio};
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};

#[derive(Parser, Debug)]
#[command(name = "DtFusion")]
#[command(about = "DtPipe SQL Engine powered by Apache DataFusion — Streaming SQL", long_about = None)]
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

/// A simple implementation of PartitionStream to wrap our async batch stream
struct AsyncPartitionStream {
    schema: SchemaRef,
    stream: Mutex<Option<SendableRecordBatchStream>>,
}

impl AsyncPartitionStream {
    fn new(schema: SchemaRef, stream: SendableRecordBatchStream) -> Self {
        Self {
            schema,
            stream: Mutex::new(Some(stream)),
        }
    }
}

impl std::fmt::Debug for AsyncPartitionStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsyncPartitionStream")
            .field("schema", &self.schema)
            .finish()
    }
}

impl PartitionStream for AsyncPartitionStream {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let mut guard = self.stream.try_lock().expect("Stream already being executed or double lock");
        guard.take().expect("Stream can only be executed once")
    }
}

/// Register a single input source into the DataFusion SessionContext
async fn register_input(
    ctx: &SessionContext,
    alias: &str,
    provider: &str,
    locator: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    match provider {
        "csv" => {
            ctx.register_csv(alias, locator, CsvReadOptions::default())
                .await?;
        }
        "parquet" => {
            ctx.register_parquet(alias, locator, ParquetReadOptions::default())
                .await?;
        }
        "ipc" => {
            let file = File::open(locator)?;
            let reader = datafusion::arrow::ipc::reader::FileReader::try_new(file, None)?;
            let schema = reader.schema();
            let batches: Vec<RecordBatch> = reader
                .into_iter()
                .collect::<Result<Vec<_>, _>>()?;
            let table = MemTable::try_new(schema, vec![batches])?;
            ctx.register_table(alias, Arc::new(table))?;
        }
        "proc" => {
            let command_str = locator.to_string();
            let args: Vec<&str> = command_str.split_whitespace().collect();
            let mut child = Command::new(args[0])
                .args(&args[1..])
                .stdout(Stdio::piped())
                .spawn()?;
            let stdout_pipe = child.stdout.take().expect("Failed to capture stdout");

            // Channel for batches
            let (tx, rx) = mpsc::channel::<DFResult<RecordBatch>>(2);
            let (schema_tx, schema_rx) = std::sync::mpsc::channel::<SchemaRef>();

            // Thread for blocking read
            std::thread::spawn(move || {
                let reader = match StreamReader::try_new(stdout_pipe, None) {
                    Ok(r) => r,
                    Err(e) => {
                        let _ = tx.blocking_send(Err(DataFusionError::External(Box::new(e))));
                        return;
                    }
                };

                let schema = reader.schema();
                let _ = schema_tx.send(schema);

                for batch_result in reader {
                    match batch_result {
                        Ok(batch) => {
                            if tx.blocking_send(Ok(batch)).is_err() {
                                break;
                            }
                        }
                        Err(e) => {
                            let _ = tx.blocking_send(Err(DataFusionError::External(Box::new(e))));
                            break;
                        }
                    }
                }
                let _ = child.wait();
            });

            let schema = schema_rx.recv().map_err(|e| format!("Failed to receive schema: {}", e))?;
            
            let stream = RecordBatchStreamAdapter::new(
                schema.clone(),
                tokio_stream::wrappers::ReceiverStream::new(rx),
            );

            let partition_stream = Arc::new(AsyncPartitionStream::new(schema.clone(), Box::pin(stream)));
            let table = StreamingTable::try_new(schema, vec![partition_stream])?;
            ctx.register_table(alias, Arc::new(table))?;
        }
        _ => {
            return Err(format!("Unsupported input provider: {}", provider).into());
        }
    }
    Ok(())
}

/// Write a stream of RecordBatches to an Arrow IPC Stream on stdout
async fn write_ipc_stream_stdout(
    mut stream: SendableRecordBatchStream,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = stream.schema();
    let mut writer = StreamWriter::try_new(stdout(), &schema)?;
    while let Some(batch_result) = stream.next().await {
        let batch = batch_result?;
        writer.write(&batch)?;
    }
    writer.finish()?;
    Ok(())
}

/// Write a stream of RecordBatches to a CSV file
async fn write_csv_file(
    mut stream: SendableRecordBatchStream,
    path: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let file = File::create(path)?;
    let buf = BufWriter::new(file);
    let mut writer = arrow_csv::WriterBuilder::new()
        .with_header(true)
        .build(buf);
    while let Some(batch_result) = stream.next().await {
        let batch = batch_result?;
        writer.write(&batch)?;
    }
    Ok(())
}

/// Write a stream of RecordBatches to a Parquet file
async fn write_parquet_file(
    mut stream: SendableRecordBatchStream,
    path: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let file = File::create(path)?;
    let schema = stream.schema();
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
    while let Some(batch_result) = stream.next().await {
        let batch = batch_result?;
        writer.write(&batch)?;
    }
    writer.close()?;
    Ok(())
}

/// Write a stream of RecordBatches to an Arrow IPC File
async fn write_ipc_file(
    mut stream: SendableRecordBatchStream,
    path: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let file = File::create(path)?;
    let schema = stream.schema();
    let mut writer = FileWriter::try_new(file, &schema)?;
    while let Some(batch_result) = stream.next().await {
        let batch = batch_result?;
        writer.write(&batch)?;
    }
    writer.finish()?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    // Disable identifier normalization to preserve column name case (e.g. GenerateIndex)
    let config = SessionConfig::new()
        .set_str("datafusion.sql_parser.enable_ident_normalization", "false");
    let ctx = SessionContext::new_with_config(config);

    // 1. Register all inputs
    for (alias, provider, locator) in &args.inputs {
        eprintln!(
            "[dtfusion] Loading [{}] from {}:{}",
            alias, provider, locator
        );
        register_input(&ctx, alias, provider, locator).await?;
    }

    // 2. Execute SQL Query
    eprintln!("[dtfusion] Executing: {}", args.query);
    let df = ctx.sql(&args.query).await?;

    // 3. Handle Output — streaming batch-by-batch
    if let Some(out_arg) = args.out {
        let (out_provider, out_locator) =
            parse_provider_locator(&out_arg).map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;

        let stream = df.execute_stream().await?;

        match out_provider.as_str() {
            "csv" => {
                eprintln!("[dtfusion] Streaming CSV to {}", out_locator);
                write_csv_file(stream, &out_locator).await?;
            }
            "parquet" => {
                eprintln!("[dtfusion] Streaming Parquet to {}", out_locator);
                write_parquet_file(stream, &out_locator).await?;
            }
            "ipc" => {
                eprintln!("[dtfusion] Streaming IPC File to {}", out_locator);
                write_ipc_file(stream, &out_locator).await?;
            }
            "proc" | "arrow" => {
                if out_locator == "-" {
                    write_ipc_stream_stdout(stream).await?;
                } else {
                    return Err(
                        "Use proc:- or arrow:- to pipe to STDOUT".into(),
                    );
                }
            }
            _ => {
                return Err(
                    format!("Unsupported output provider: {}", out_provider)
                        .into(),
                );
            }
        }
    } else {
        // Fallback: print to console
        df.show().await?;
    }

    Ok(())
}
