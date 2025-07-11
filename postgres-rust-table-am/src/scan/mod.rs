use pgrx::pg_sys::{BlockNumber, HeapScanDesc, ReadStream, ScanDirection};
use pgrx::prelude::*;

#[pg_guard]
unsafe extern "C-unwind" fn heapgettup_initial_block(
    scan: HeapScanDesc,
    dir: ScanDirection::Type,
) -> BlockNumber {
    todo!("");
}

#[pg_guard]
unsafe extern "C-unwind" fn heapgettup_advance_block(
    scan: HeapScanDesc,
    block: BlockNumber,
    dir: ScanDirection::Type,
) -> BlockNumber {
    todo!("");
}

#[pg_guard]
pub unsafe extern "C-unwind" fn heap_scan_stream_read_next_serial(
    stream: *mut ReadStream,
    callback_private_data: *mut ::core::ffi::c_void,
    per_buffer_data: *mut ::core::ffi::c_void,
) -> BlockNumber {
    todo!("")
}
