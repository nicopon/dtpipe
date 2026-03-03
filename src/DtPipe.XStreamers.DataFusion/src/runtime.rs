use std::sync::Arc;
use tokio::runtime::Runtime;

pub struct DtfbRuntime {
    pub inner: Arc<Runtime>,
}

#[unsafe(no_mangle)]
pub extern "C" fn dtfb_runtime_new() -> *mut DtfbRuntime {
    match Runtime::new() {
        Ok(rt) => Box::into_raw(Box::new(DtfbRuntime { inner: Arc::new(rt) })),
        Err(_) => std::ptr::null_mut(),
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn dtfb_runtime_destroy(ptr: *mut DtfbRuntime) {
    if !ptr.is_null() {
        unsafe { drop(Box::from_raw(ptr)) };
    }
}
