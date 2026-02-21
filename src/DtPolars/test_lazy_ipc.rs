use polars::prelude::*;
fn main() {
    // Check if there is something like LazyIpcStreamReader
    let _ = LazyIpcReader::new("test"); // We know this exists for IPC files
}
