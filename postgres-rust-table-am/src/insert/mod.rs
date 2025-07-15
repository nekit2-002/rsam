use pg_sys::WalLevel::WAL_LEVEL_REPLICA;
use pgrx::pg_sys::{
    pfree, pgstat_count_heap_insert, visibilitymap_clear, visibilitymap_pin, wal_level, Buffer,
    BufferGetPage, BulkInsertStateData, CommandId, CritSectionCount, GetCurrentTransactionId,
    HeapTuple, HeapTupleData, InvalidBuffer, ItemPointerGetBlockNumber, MarkBufferDirty,
    PageClearAllVisible, PageIsAllVisible, RelationData, ReleaseBuffer, TransactionId,
    UnlockReleaseBuffer, RELPERSISTENCE_PERMANENT, VISIBILITYMAP_VALID_BITS,
};
use pgrx::prelude::*;

#[macro_export]
macro_rules! START_CRIT_SECTION {
    () => {
        CritSectionCount += 1;
    };
}

#[macro_export]
macro_rules! END_CRIT_SECTION {
    () => {
        if CritSectionCount > 0 {
            CritSectionCount -= 1;
        }
    };
}

#[macro_export]
macro_rules! RelationIsPermanent {
    ($rel:expr) => {
        (*(*$rel).rd_rel).relpersistence == RELPERSISTENCE_PERMANENT as i8
    };
}

#[macro_export]
macro_rules! XLogIsNeeded {
    () => {
        wal_level >= WAL_LEVEL_REPLICA as i32
    };
}

#[macro_export]
macro_rules! RelationNeedsWal {
    ($rel:expr) => {
        RelationIsPermanent!($rel)
            && (XLogIsNeeded!()
                || (*$rel).rd_createSubid == 0 && (*$rel).rd_firstRelfilelocatorSubid == 0)
    };
}

#[pg_guard]
pub unsafe extern "C-unwind" fn heap_prepare_insert(
    rel: *mut RelationData,
    tup: *mut HeapTupleData,
    tid: TransactionId,
    cid: CommandId,
    options: i32,
) -> HeapTuple {
    todo!("")
}

#[pg_guard]
pub unsafe extern "C-unwind" fn RelationGetBufferForTuple(
    rel: *mut RelationData,
    len: usize,
    otherBuffer: Buffer,
    options: i32,
    state: *mut BulkInsertStateData,
    vmbuffer: *mut Buffer,
    vmbuffer_other: *mut Buffer,
    num_pages: i32,
) -> Buffer {
    todo!("")
}

#[pg_guard]
pub unsafe extern "C-unwind" fn RelationPutTuple(
    rel: *mut RelationData,
    buffer: Buffer,
    tuple: *mut HeapTupleData,
) {
}

#[pg_guard]
pub unsafe extern "C-unwind" fn heap_insert(
    rel: *mut RelationData,
    tup: *mut HeapTupleData,
    cid: CommandId,
    options: i32,
    state: *mut BulkInsertStateData,
) {
    let xid = GetCurrentTransactionId();
    let mut vmbuffer = InvalidBuffer as i32;
    let mut all_visible_cleared = false;
    let tuple = heap_prepare_insert(rel, tup, xid, cid, options);
    let buffer = RelationGetBufferForTuple(
        rel,
        (*tuple).t_len as usize,
        InvalidBuffer as i32,
        options,
        state,
        &raw mut vmbuffer,
        std::ptr::null_mut(),
        0,
    );

    START_CRIT_SECTION!();
    RelationPutTuple(rel, buffer, tuple);
    if PageIsAllVisible(BufferGetPage(buffer)) {
        all_visible_cleared = true;
        PageClearAllVisible(BufferGetPage(buffer));
        visibilitymap_clear(
            rel,
            ItemPointerGetBlockNumber(&raw const (*tuple).t_self),
            vmbuffer,
            VISIBILITYMAP_VALID_BITS as u8,
        );
    }

    MarkBufferDirty(buffer);
    // TODO: implement write ahead log stuff
    // if RelationNeedsWal!(rel) {}
    END_CRIT_SECTION!();
    UnlockReleaseBuffer(buffer);
    if vmbuffer != InvalidBuffer as i32 {
        ReleaseBuffer(vmbuffer);
    }

    pgstat_count_heap_insert(rel, 1);
    if tuple != tup {
        (*tup).t_self = (*tuple).t_self;
        pfree(tuple.cast());
    }
}
