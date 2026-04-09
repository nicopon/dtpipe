use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use futures::StreamExt;
use datafusion::prelude::*;

use arrow::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use arrow::array::RecordBatchReader;
use arrow::datatypes::SchemaRef;
use datafusion::catalog::streaming::StreamingTable;
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;

use crate::{ErrorCode, DtfbRuntime};

const PLAN_TIMEOUT_SECS: u64 = 30;

pub struct DtfbContext {
    pub runtime: Arc<tokio::runtime::Runtime>,
    pub ctx: Arc<SessionContext>,
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtfb_context_new(runtime_ptr: *mut DtfbRuntime) -> *mut DtfbContext {
    let rt = crate::ffi_ref_null!(runtime_ptr);
    let mut config = SessionConfig::new();
    // Enable identifier normalization (default behavior) to support case-insensitive SQL matching
    config.options_mut().sql_parser.enable_ident_normalization = true;

    Box::into_raw(Box::new(DtfbContext {
        runtime: Arc::clone(&rt.inner),
        ctx: Arc::new(SessionContext::new_with_config(config)),
    }))
}

fn lowercase_schema(schema: SchemaRef) -> SchemaRef {
    let fields: Vec<_> = schema.fields().iter().map(|f| {
        let name = f.name().to_lowercase();
        f.as_ref().clone().with_name(name)
    }).collect();
    Arc::new(arrow::datatypes::Schema::new(fields))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtfb_context_destroy(ptr: *mut DtfbContext) {
    if !ptr.is_null() {
        unsafe { drop(Box::from_raw(ptr)) };
    }
}

struct FfiPartitionStream {
    schema: SchemaRef,
    reader: Arc<Mutex<Option<ArrowArrayStreamReader>>>,
}

impl std::fmt::Debug for FfiPartitionStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FfiPartitionStream").field("schema", &self.schema).finish()
    }
}

impl PartitionStream for FfiPartitionStream {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<datafusion::execution::context::TaskContext>) -> datafusion::execution::SendableRecordBatchStream {
        let mut reader_guard = match self.reader.try_lock() {
            Ok(g) => g,
            Err(_) => {
                let err_stream = futures::stream::once(async move {
                    Err(datafusion::error::DataFusionError::Execution("FFI stream reader lock failed".to_string()))
                });
                return Box::pin(RecordBatchStreamAdapter::new(self.schema.clone(), err_stream));
            }
        };

        let reader = match reader_guard.take() {
            Some(r) => r,
            None => {
                let err_stream = futures::stream::once(async move {
                    Err(datafusion::error::DataFusionError::Execution("FFI stream can only be executed once".to_string()))
                });
                return Box::pin(RecordBatchStreamAdapter::new(self.schema.clone(), err_stream));
            }
        };

        let schema = self.schema.clone();
        let schema_for_stream = schema.clone();
        let stream = futures::stream::iter(reader).map(move |r| {
            match r {
                Ok(rb) => {
                    // Force the lowercased schema onto the batch.
                    // try_new validates types/count but not names, which is what we want.
                    arrow::array::RecordBatch::try_new(schema_for_stream.clone(), rb.columns().to_vec())
                        .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))
                }
                Err(e) => Err(datafusion::error::DataFusionError::ArrowError(Box::new(e), None))
            }
        });

        Box::pin(RecordBatchStreamAdapter::new(schema, stream))
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtfb_register_stream(
    ctx_ptr: *mut DtfbContext,
    name_ptr: *const std::ffi::c_char,
    stream_ptr: *mut FFI_ArrowArrayStream,
) -> ErrorCode {
    let dtfb = crate::ffi_ref!(ctx_ptr);
    let name = crate::ffi_cstr!(name_ptr);

    let ffi_stream = unsafe { std::ptr::read(stream_ptr) };
    let reader = match ArrowArrayStreamReader::try_new(ffi_stream) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("[dtfusion-bridge] register_stream: failed to create FFI stream reader: {:?}", e);
            return ErrorCode::Error;
        }
    };

    let original_schema = reader.schema();
    let schema = lowercase_schema(original_schema);

    let partition_stream = Arc::new(FfiPartitionStream {
        schema: schema.clone(),
        reader: Arc::new(Mutex::new(Some(reader))),
    });

    match StreamingTable::try_new(schema, vec![partition_stream]) {
        Ok(table) => {
            match dtfb.ctx.register_table(name, Arc::new(table)) {
                Ok(_) => ErrorCode::Ok,
                Err(e) => {
                    eprintln!("[dtfusion-bridge] register_stream: failed to register table '{}': {:?}", name, e);
                    ErrorCode::Error
                }
            }
        },
        Err(e) => {
            eprintln!("[dtfusion-bridge] register_stream: StreamingTable::try_new failed: {:?}", e);
            ErrorCode::Error
        }
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtfb_get_schema(
    ctx_ptr: *mut DtfbContext,
    sql_ptr: *const std::ffi::c_char,
    ffi_schema_ptr: *mut arrow::ffi::FFI_ArrowSchema,
) -> ErrorCode {
    let dtfb = crate::ffi_ref!(ctx_ptr);
    let sql = crate::ffi_cstr!(sql_ptr);

    let ctx = Arc::clone(&dtfb.ctx);
    let sql = sql.to_string();

    // Run planning on a dedicated OS thread so that even a CPU-bound infinite loop
    // (e.g. DataFusion's TypeCoercion hang on arrow.uuid + Utf8 JOIN) does not block
    // the calling thread beyond PLAN_TIMEOUT_SECS.
    // tokio::time::timeout cannot help here: DataFusion's loop never yields, so the
    // Tokio timer never gets polled. An OS-level mpsc::recv_timeout is the only reliable
    // mechanism to interrupt a non-yielding future.
    let runtime = Arc::clone(&dtfb.runtime);
    type PlanResult = Result<arrow::datatypes::Schema, String>;
    let (tx, rx) = std::sync::mpsc::sync_channel::<PlanResult>(1);

    std::thread::spawn(move || {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            runtime.block_on(async move {
                match ctx.sql(&sql).await {
                    Ok(df) => Ok(df.schema().as_arrow().as_ref().clone()),
                    Err(e) => Err(format!("{:?}", e)),
                }
            })
        }));
        let _ = tx.send(match result {
            Ok(r) => r,
            Err(_) => Err("panic during SQL planning".to_string()),
        });
    });

    let schema = match rx.recv_timeout(Duration::from_secs(PLAN_TIMEOUT_SECS)) {
        Ok(Ok(s)) => s,
        Ok(Err(msg)) => {
            eprintln!("[dtfusion-bridge] SQL error in get_schema: {}", msg);
            return ErrorCode::Error;
        }
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
            eprintln!(
                "[dtfusion-bridge] SQL planning timed out (>{}s) — incompatible column types in a JOIN condition (e.g. FixedSizeBinary+arrow.uuid vs Utf8). Use --column-types to declare UUID columns on text sources, or use --sql-engine duckdb.",
                PLAN_TIMEOUT_SECS
            );
            return ErrorCode::Error;
        }
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            eprintln!("[dtfusion-bridge] SQL planning thread panicked unexpectedly");
            return ErrorCode::Error;
        }
    };

    match arrow::ffi::FFI_ArrowSchema::try_from(&schema) {
        Ok(ffi_schema) => {
            unsafe { std::ptr::write(ffi_schema_ptr, ffi_schema) };
            ErrorCode::Ok
        }
        Err(e) => {
            eprintln!("[dtfusion-bridge] FFI schema export error in get_schema: {:?}", e);
            ErrorCode::Error
        }
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtfb_register_batches(
    ctx_ptr: *mut DtfbContext,
    name_ptr: *const std::ffi::c_char,
    ffi_schema_ptr: *mut arrow::ffi::FFI_ArrowSchema,
    ffi_batches_ptr: *mut *mut arrow::ffi::FFI_ArrowArray,
    num_batches: usize,
) -> ErrorCode {
    let dtfb = crate::ffi_ref!(ctx_ptr);
    let name = crate::ffi_cstr!(name_ptr).to_string();

    let ffi_schema = unsafe { &*ffi_schema_ptr };
    let original_schema = match arrow::datatypes::Schema::try_from(ffi_schema) {
        Ok(s) => Arc::new(s),
        Err(e) => {
            eprintln!("[dtfusion-bridge] register_batches: failed to import FFI schema: {:?}", e);
            return ErrorCode::Error;
        }
    };
    let schema = lowercase_schema(original_schema);

    let mut batches = Vec::with_capacity(num_batches);
    for i in 0..num_batches {
        let array_ptr = unsafe { *ffi_batches_ptr.add(i) };
        if array_ptr.is_null() { return ErrorCode::Error; }
        let ffi_array = unsafe { std::ptr::read(array_ptr) };

        let array_data = match unsafe { arrow::ffi::from_ffi(ffi_array, ffi_schema) } {
            Ok(a) => a,
            Err(e) => {
                eprintln!("[dtfusion-bridge] register_batches: failed to import FFI array at index {}: {:?}", i, e);
                return ErrorCode::Error;
            }
        };

        let struct_array = arrow::array::StructArray::from(array_data);
        let columns = struct_array.columns().to_vec();

        // Force the lowercased schema onto the batch
        let rb = match arrow::array::RecordBatch::try_new(Arc::clone(&schema), columns) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("[dtfusion-bridge] register_batches: RecordBatch::try_new failed at index {}: {:?}", i, e);
                return ErrorCode::Error;
            }
        };
        batches.push(rb);
    }

    let ctx = Arc::clone(&dtfb.ctx);
    let table = match datafusion::datasource::MemTable::try_new(Arc::clone(&schema), vec![batches]) {
        Ok(t) => Arc::new(t),
        Err(e) => {
            eprintln!("[dtfusion-bridge] register_batches: MemTable::try_new failed for '{}': {:?}", name, e);
            return ErrorCode::Error;
        }
    };

    dtfb.runtime.block_on(async move {
        match ctx.register_table(&name, table) {
            Ok(_) => ErrorCode::Ok,
            Err(e) => {
                eprintln!("[dtfusion-bridge] register_batches: failed to register table '{}': {:?}", name, e);
                ErrorCode::Error
            }
        }
    })
}

struct FfiStreamExporter {
    schema: SchemaRef,
    // Box pin helps keep the async stream safely referenced
    stream: std::pin::Pin<Box<datafusion::execution::SendableRecordBatchStream>>,
    runtime: Arc<tokio::runtime::Runtime>,
}

impl Iterator for FfiStreamExporter {
    type Item = Result<arrow::array::RecordBatch, arrow::error::ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.runtime.block_on(async {
            match futures::StreamExt::next(&mut self.stream).await {
                Some(Ok(batch)) => Some(Ok(batch)),
                Some(Err(e)) => Some(Err(arrow::error::ArrowError::ExternalError(Box::new(e)))),
                None => None,
            }
        })
    }
}

impl arrow::array::RecordBatchReader for FfiStreamExporter {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtfb_execute_stream(
    ctx_ptr: *mut DtfbContext,
    sql_ptr: *const std::ffi::c_char,
    out_stream_ptr: *mut FFI_ArrowArrayStream,
) -> ErrorCode {
    let dtfb = crate::ffi_ref!(ctx_ptr);
    let sql = crate::ffi_cstr!(sql_ptr);

    let ctx: Arc<SessionContext> = Arc::clone(&dtfb.ctx);
    let sql = sql.to_string();
    let runtime: Arc<tokio::runtime::Runtime> = Arc::clone(&dtfb.runtime);

    // Phase 1: SQL planning with OS-level timeout
    let runtime_for_plan = Arc::clone(&runtime);
    let ctx_for_plan = Arc::clone(&ctx);
    let sql_for_plan = sql.clone();
    type PlanResult = Result<datafusion::dataframe::DataFrame, String>;
    let (plan_tx, plan_rx) = std::sync::mpsc::sync_channel::<PlanResult>(1);

    std::thread::spawn(move || {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            runtime_for_plan.block_on(async move {
                match ctx_for_plan.sql(&sql_for_plan).await {
                    Ok(df) => Ok(df),
                    Err(e) => Err(format!("{:?}", e)),
                }
            })
        }));
        let _ = plan_tx.send(match result {
            Ok(r) => r,
            Err(_) => Err("panic during SQL planning".to_string()),
        });
    });

    let df = match plan_rx.recv_timeout(Duration::from_secs(PLAN_TIMEOUT_SECS)) {
        Ok(Ok(df)) => df,
        Ok(Err(msg)) => {
            eprintln!("[dtfusion-bridge] SQL error in execute_stream: {}", msg);
            return ErrorCode::Error;
        }
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
            eprintln!(
                "[dtfusion-bridge] SQL planning timed out (>{}s)",
                PLAN_TIMEOUT_SECS
            );
            return ErrorCode::Error;
        }
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            eprintln!("[dtfusion-bridge] SQL planning thread panicked unexpectedly");
            return ErrorCode::Error;
        }
    };

    // Phase 2: Execution stream setup
    let exec_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        runtime.block_on(async move {
            match df.execute_stream().await {
                Ok(s) => Ok(s),
                Err(e) => {
                    eprintln!("[dtfusion-bridge] execute_stream error: {:?}", e);
                    Err(())
                }
            }
        })
    }));

    let stream = match exec_result {
        Ok(Ok(s)) => s,
        Ok(Err(_)) => return ErrorCode::Error,
        Err(_) => {
            eprintln!("[dtfusion-bridge] Panic during stream setup");
            return ErrorCode::Error;
        }
    };

    let schema = stream.schema();

    let exporter = FfiStreamExporter {
        schema,
        stream: Box::pin(stream),
        runtime,
    };

    let ffi_stream = FFI_ArrowArrayStream::new(Box::new(exporter));

    unsafe { std::ptr::write(out_stream_ptr, ffi_stream) };
    ErrorCode::Ok
}
