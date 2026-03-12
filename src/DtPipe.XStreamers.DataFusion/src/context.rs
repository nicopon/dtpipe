use std::sync::Arc;
#[cfg(unix)]
use std::os::unix::io::FromRawFd;
#[cfg(windows)]
use std::os::windows::io::FromRawHandle;
use tokio::sync::Mutex;
use futures::StreamExt;
use datafusion::prelude::*;

use arrow_array::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use arrow_array::RecordBatchReader;
use arrow_schema::SchemaRef;
use datafusion::catalog::streaming::StreamingTable;
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use arrow_ipc::writer::StreamWriter;

use crate::{ErrorCode, DtfbRuntime};

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
    Arc::new(arrow_schema::Schema::new(fields))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtfb_context_destroy(ptr: *mut DtfbContext) {
    if !ptr.is_null() {
        unsafe { drop(Box::from_raw(ptr)) };
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtfb_register_parquet(
    ctx_ptr: *mut DtfbContext,
    name_ptr: *const std::ffi::c_char,
    path_ptr: *const std::ffi::c_char,
) -> ErrorCode {
    let dtfb = crate::ffi_ref!(ctx_ptr);
    let name = crate::ffi_cstr!(name_ptr);
    let path = crate::ffi_cstr!(path_ptr);

    let ctx: Arc<SessionContext> = Arc::clone(&dtfb.ctx);
    let name = name.to_string();
    let path = path.to_string();

    dtfb.runtime.block_on(async move {
        match ctx.register_parquet(&name, &path, ParquetReadOptions::default()).await {
            Ok(_) => ErrorCode::Ok,
            Err(_) => ErrorCode::Error,
        }
    })
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
                    arrow_array::RecordBatch::try_new(schema_for_stream.clone(), rb.columns().to_vec())
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
        Err(_) => return ErrorCode::Error,
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
                Err(_) => ErrorCode::Error,
            }
        },
        Err(_) => ErrorCode::Error,
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtfb_get_schema(
    ctx_ptr: *mut DtfbContext,
    sql_ptr: *const std::ffi::c_char,
    ffi_schema_ptr: *mut arrow_array::ffi::FFI_ArrowSchema,
) -> ErrorCode {
    let dtfb = crate::ffi_ref!(ctx_ptr);
    let sql = crate::ffi_cstr!(sql_ptr);

    let ctx = Arc::clone(&dtfb.ctx);
    let sql = sql.to_string();

    dtfb.runtime.block_on(async move {
        let df = match ctx.sql(&sql).await {
            Ok(df) => df,
            Err(e) => {
                eprintln!("[dtfusion-bridge] SQL error in get_schema: {:?}", e);
                return ErrorCode::Error;
            }
        };

        let arrow_schema = df.schema().as_arrow().as_ref().clone();
        
        match arrow_array::ffi::FFI_ArrowSchema::try_from(&arrow_schema) {
            Ok(ffi_schema) => {
                unsafe { std::ptr::write(ffi_schema_ptr, ffi_schema) };
                ErrorCode::Ok
            }
            Err(e) => {
                eprintln!("[dtfusion-bridge] FFI schema error: {:?}", e);
                ErrorCode::Error
            }
        }
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtfb_register_csv(
    ctx_ptr: *mut DtfbContext,
    name_ptr: *const std::ffi::c_char,
    path_ptr: *const std::ffi::c_char,
) -> ErrorCode {
    let dtfb = crate::ffi_ref!(ctx_ptr);
    let name = crate::ffi_cstr!(name_ptr);
    let path = crate::ffi_cstr!(path_ptr);

    let ctx = Arc::clone(&dtfb.ctx);
    let name = name.to_string();
    let path = path.to_string();

    dtfb.runtime.block_on(async move {
        match ctx.register_csv(&name, &path, datafusion::prelude::CsvReadOptions::default()).await {
            Ok(_) => ErrorCode::Ok,
            Err(_) => ErrorCode::Error,
        }
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtfb_register_batches(
    ctx_ptr: *mut DtfbContext,
    name_ptr: *const std::ffi::c_char,
    ffi_schema_ptr: *mut arrow_array::ffi::FFI_ArrowSchema,
    ffi_batches_ptr: *mut *mut arrow_array::ffi::FFI_ArrowArray,
    num_batches: usize,
) -> ErrorCode {
    let dtfb = crate::ffi_ref!(ctx_ptr);
    let name = crate::ffi_cstr!(name_ptr).to_string();

    let ffi_schema = unsafe { &*ffi_schema_ptr };
    let original_schema = match arrow_schema::Schema::try_from(ffi_schema) {
        Ok(s) => Arc::new(s),
        Err(_) => return ErrorCode::Error,
    };
    let schema = lowercase_schema(original_schema);

    let mut batches = Vec::with_capacity(num_batches);
    for i in 0..num_batches {
        let array_ptr = unsafe { *ffi_batches_ptr.add(i) };
        if array_ptr.is_null() { return ErrorCode::Error; }
        let ffi_array = unsafe { std::ptr::read(array_ptr) };
        
        let array_data = match unsafe { arrow_array::ffi::from_ffi(ffi_array, ffi_schema) } {
            Ok(a) => a,
            Err(_) => return ErrorCode::Error,
        };
        
        let struct_array = arrow_array::StructArray::from(array_data);
        let columns = struct_array.columns().to_vec();
        
        // Force the lowercased schema onto the batch
        let rb = match arrow_array::RecordBatch::try_new(Arc::clone(&schema), columns) {
            Ok(r) => r,
            Err(_) => return ErrorCode::Error,
        };
        batches.push(rb);
    }

    let ctx = Arc::clone(&dtfb.ctx);
    let table = match datafusion::datasource::MemTable::try_new(Arc::clone(&schema), vec![batches]) {
        Ok(t) => Arc::new(t),
        Err(_) => return ErrorCode::Error,
    };

    dtfb.runtime.block_on(async move {
        match ctx.register_table(&name, table) {
            Ok(_) => ErrorCode::Ok,
            Err(_) => ErrorCode::Error,
        }
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtfb_execute_to_fd(
    ctx_ptr: *mut DtfbContext,
    sql_ptr: *const std::ffi::c_char,
    handle: *mut std::ffi::c_void,
) -> ErrorCode {
    let dtfb = crate::ffi_ref!(ctx_ptr);
    let sql = crate::ffi_cstr!(sql_ptr);

    let ctx: Arc<SessionContext> = Arc::clone(&dtfb.ctx);
    let sql = sql.to_string();
    let runtime: Arc<tokio::runtime::Runtime> = Arc::clone(&dtfb.runtime);

    dtfb.runtime.block_on(async move {
        let df = match ctx.sql(&sql).await {
            Ok(df) => df,
            Err(e) => {
                eprintln!("[dtfusion-bridge] SQL error in execute_to_fd: {:?}", e);
                return ErrorCode::Error;
            }
        };

        let mut stream = match df.execute_stream().await {
            Ok(s) => s,
            Err(_) => {
                return ErrorCode::Error;
            }
        };

        let schema = stream.schema();
        
        let handle_val = handle as usize;
        
        // Blocking I/O section
        let result = runtime.spawn_blocking(move || {
            let handle = handle_val as *mut std::ffi::c_void;
            #[cfg(unix)]
            let mut file = std::mem::ManuallyDrop::new(unsafe { std::fs::File::from_raw_fd(handle as i32) });
            #[cfg(windows)]
            let mut file = std::mem::ManuallyDrop::new(unsafe { std::fs::File::from_raw_handle(handle) });

            // writer takes &mut *file which implements Write without owning
            let mut writer = match StreamWriter::try_new(&mut *file, &schema) {
                Ok(w) => w,
                Err(e) => {
                    eprintln!("[dtfusion-bridge] IPC StreamWriter error: {:?}", e);
                    return Err(());
                }
            };

            while let Some(batch_result) = futures::executor::block_on(stream.next()) {
                match batch_result {
                    Ok(batch) => {
                        if let Err(e) = writer.write(&batch) {
                            eprintln!("[dtfusion-bridge] IPC write error: {:?}", e);
                            return Err(());
                        }
                    }
                    Err(e) => {
                        eprintln!("[dtfusion-bridge] IPC stream next error: {:?}", e);
                        return Err(());
                    }
                }
            }

            if let Err(e) = writer.finish() {
                eprintln!("[dtfusion-bridge] IPC writer finish error: {:?}", e);
                return Err(());
            }

            Ok(())
        }).await;

        match result {
            Ok(Ok(())) => ErrorCode::Ok,
            Ok(Err(_)) => ErrorCode::Error,
            Err(e) => {
                eprintln!("[dtfusion-bridge] Blocking spawn error: {:?}", e);
                ErrorCode::Error
            }
        }
    })
}
