use pgrx::pg_sys::{
    Datum, HeapTuple, HeapTupleHeaderData__bindgen_ty_1, ItemPointerData, MemoryContextSwitchTo,
    MinimalTuple, Oid, TupleDescData, TupleTableSlot, TupleTableSlotOps, __IncompleteArrayField,
    TTS_FLAG_SHOULDFREE,
};
use pgrx::prelude::*;

#[repr(C)]
pub struct MyHeapTupleHeaderData {
    pub t_choice: HeapTupleHeaderData__bindgen_ty_1,
    pub t_ctid: ItemPointerData,
    pub t_infomask2: u16,
    pub t_infomask: u16,
    pub t_hoff: u8,
    pub t_bits: __IncompleteArrayField<u8>,
}

#[repr(C)]
#[allow(non_snake_case)] // to disable warning about t_tableOid field name
pub struct MyHeapTupleData {
    pub t_len: u32,
    pub t_self: ItemPointerData,
    pub t_tableOid: Oid,
    pub t_data: *mut MyHeapTupleHeaderData,
}

// TODO: At the moment this struct is emulation of HeapTupleTableSlot
#[repr(C)]
pub struct MyHeapTupleTableSlot {
    pub base: TupleTableSlot,
    pub tuple: *mut MyHeapTupleData,
    pub off: u32,
    pub tupdata: MyHeapTupleData,
}

// TODO: At the moment this struct is emulation of HeapBufferTupleTableSlot
#[repr(C)]
pub struct RsAmTupleTableSlot {
    pub base: MyHeapTupleTableSlot,
    pub buffer: i32, // buffer identifier in shared buffers
}

unsafe extern "C-unwind" fn myheap_form_tuple(
    tupleDescriptor: *mut TupleDescData,
    values: *mut Datum,
    isnull: *mut bool,
) -> *mut MyHeapTupleData {
    todo!("create heap tuple from datum and null falgs arrays")
}

// create a palloc'ed copy of the tuple
unsafe extern "C-unwind" fn myheap_copy_tuple(tuple: *mut MyHeapTupleData) -> *mut MyHeapTupleData {
    todo!("return a copy of the entire tuple")
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
// TODO: this functions is not implemented
unsafe extern "C-unwind" fn tts_rsam_materialize(slot: *mut TupleTableSlot) {
    // aka cast parent class to its child
    let rsslot = slot as *mut RsAmTupleTableSlot;
    // switch to slot's context to allocate memory in it
    let oldContext = MemoryContextSwitchTo((*slot).tts_mcxt);

    // here type cast is safe as long as TTS_FLAG_SHOULDFREE is less than max u16
    if ((*slot).tts_flags & TTS_FLAG_SHOULDFREE as u16) != 0 {
        // slot is already materialized, so there is nothing to do
        return;
    }

    
    (*rsslot).base.tuple = if (*rsslot).base.tuple.is_null() {
        todo!("")
    } else {
        todo!("")
    };

    MemoryContextSwitchTo(oldContext); // return to old context
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
#[allow(non_upper_case_globals)] // to disable compiler warning about global var name
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
