use pgrx::pg_sys::{
    Buffer, HeapTupleHeaderData, ItemPointer, LockTupleMode, LockWaitPolicy, Relation,
    TransactionId,
};
use pgrx::prelude::*;
#[pg_guard]
pub unsafe extern "C-unwind" fn aquire_tuplock(
    rel: Relation,
    tid: ItemPointer,
    mode: LockTupleMode::Type,
    wait_policy: LockWaitPolicy::Type,
    have_tuple_lock: *mut bool,
) -> bool {
    todo!("")
}

#[pg_guard]
pub unsafe extern "C-unwind" fn xmax_infomask_changed(new_mask: u16, old_mask: u16) -> bool {
    todo!("")
}

#[pg_guard]
pub unsafe extern "C-unwind" fn UpdateXmaxHintBits(
    tuple: *mut HeapTupleHeaderData,
    buffer: Buffer,
    xid: TransactionId,
) {
}

#[pg_guard]
pub unsafe extern "C-unwind" fn tuple_header_get_update_xid(
    tup: *const HeapTupleHeaderData,
) -> TransactionId {
    todo!("")
}
