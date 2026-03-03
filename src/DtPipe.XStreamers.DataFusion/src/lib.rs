mod runtime;
mod context;

pub use runtime::*;
pub use context::*;

#[repr(C)]
pub enum ErrorCode {
    Ok = 0,
    Error = -1,
}

#[macro_export]
macro_rules! ffi_ref {
    ($ptr:expr) => {
        match unsafe { $ptr.as_ref() } {
            Some(r) => r,
            None => return ErrorCode::Error,
        }
    };
}

#[macro_export]
macro_rules! ffi_ref_null {
    ($ptr:expr) => {
        match unsafe { $ptr.as_ref() } {
            Some(r) => r,
            None => return std::ptr::null_mut(),
        }
    };
}

#[macro_export]
macro_rules! ffi_cstr {
    ($ptr:expr) => {
        match unsafe { std::ffi::CStr::from_ptr($ptr).to_str() } {
            Ok(s) => s,
            Err(_) => return ErrorCode::Error,
        }
    };
}

#[macro_export]
macro_rules! ffi_cstr_null {
    ($ptr:expr) => {
        match unsafe { std::ffi::CStr::from_ptr($ptr).to_str() } {
            Ok(s) => s,
            Err(_) => return std::ptr::null_mut(),
        }
    };
}
