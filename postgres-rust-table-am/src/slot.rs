use pgrx::pg_sys::{Datum, HeapTuple, MinimalTuple, TupleTableSlot, TupleTableSlotOps};
use pgrx::prelude::*;

// TODO: This struct is uncompleted
pub struct RsAmTupleTableSlot {
    pub _base: TupleTableSlot,
}

#[pg_guard]
unsafe extern "C-unwind" fn tts_rsam_init(_slot: *mut TupleTableSlot) {
    todo!("rs access method slot init")
}

#[pg_guard]
unsafe extern "C-unwind" fn tts_rsam_release(_slot: *mut TupleTableSlot) {
    todo!("rs access method slot release")
}

#[pg_guard]
unsafe extern "C-unwind" fn tts_rsam_clear(_slot: *mut TupleTableSlot) {
    todo!("rs access method slot release")
}

#[pg_guard]
unsafe extern "C-unwind" fn tts_rsam_getsomeattrs(
    _slot: *mut TupleTableSlot,
    _natts: ::core::ffi::c_int,
) {
    todo!("get attributes")
}

#[pg_guard]
unsafe extern "C-unwind" fn tts_rsam_getsysattr(
    _slot: *mut TupleTableSlot,
    _attnum: ::core::ffi::c_int,
    _isnull: *mut bool,
) -> Datum {
    todo!("get system attribute")
}

#[pg_guard]
unsafe extern "C-unwind" fn tts_rsam_is_current_xact_tuple(_slot: *mut TupleTableSlot) -> bool {
    todo!("rs access method slot release")
}

#[pg_guard]
unsafe extern "C-unwind" fn tts_rsam_materialize(_slot: *mut TupleTableSlot) {
    todo!("rs access method slot release")
}

#[pg_guard]
unsafe extern "C-unwind" fn tts_rsam_copyslot(
    _dst_slot: *mut TupleTableSlot,
    _src_slot: *mut TupleTableSlot,
) {
    todo!("copy slot")
}

#[pg_guard]
unsafe extern "C-unwind" fn tts_rsam_copy_heap_tuple(_slot: *mut TupleTableSlot) -> HeapTuple {
    todo!("rs access method slot release")
}
#[pg_guard]
unsafe extern "C-unwind" fn tts_rsam_copy_minimal_tuple(
    _slot: *mut TupleTableSlot,
) -> MinimalTuple {
    todo!("rs access method slot release")
}

// TODO: Implement rsam tuple table slot operations
pub static TTSOpsRsAmTuple: TupleTableSlotOps = TupleTableSlotOps {
    base_slot_size: std::mem::size_of::<RsAmTupleTableSlot>(),
    init: Some(tts_rsam_init),
    release: Some(tts_rsam_release),
    clear: Some(tts_rsam_clear),
    getsomeattrs: Some(tts_rsam_getsomeattrs),
    getsysattr: Some(tts_rsam_getsysattr),
    is_current_xact_tuple: Some(tts_rsam_is_current_xact_tuple),
    materialize: Some(tts_rsam_materialize),
    copyslot: Some(tts_rsam_copyslot),

    get_heap_tuple: None,
    get_minimal_tuple: None,

    copy_heap_tuple: Some(tts_rsam_copy_heap_tuple),
    copy_minimal_tuple: Some(tts_rsam_copy_minimal_tuple),
};
