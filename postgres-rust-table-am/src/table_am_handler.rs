use crate::am_handler_port::{new_table_am_routine, TableAmArgs, TableAmHandler, TableAmRoutine};
use crate::delete::*;
use crate::include::general::*;
use crate::include::relaion_macro::*;
use crate::insert::*;
use crate::scan::fetcher::{heap_gettup, heap_gettup_pagemode};
use crate::scan::visibility::*;
use crate::scan::*;
use crate::update::{fetch_tuple, tuple_update};

// import types
use pg_sys::{
    BlockNumber, BufferAccessStrategy, BufferHeapTupleTableSlot, BulkInsertStateData, CommandId,
    ForkNumber, ForkNumber::*, HeapScanDesc, HeapScanDescData, HeapTupleData, IndexBuildCallback,
    IndexFetchTableData, IndexInfo, ItemPointer, LockTupleMode, LockWaitPolicy, MultiXactId, Oid,
    ParallelBlockTableScanWorkerData, ParallelTableScanDesc, ParallelTableScanDescData, ReadStream,
    ReadStreamBlockNumberCB, RelFileLocator, Relation, RelationData, SampleScanState,
    ScanDirection, ScanKey, ScanKeyData, Snapshot, SnapshotData, TM_FailureData, TM_IndexDeleteOp,
    TM_Result, TU_UpdateIndexes, TableScanDesc, TransactionId, TupleTableSlot, TupleTableSlotOps,
    VacuumParams, ValidateIndexState,
};

// import constants
use pg_sys::{
    InvalidBuffer, InvalidCommandId, InvalidTransactionId, TTSOpsBufferHeapTuple, BLCKSZ,
    BUFFER_LOCK_EXCLUSIVE, BUFFER_LOCK_UNLOCK, HEAP_HOT_UPDATED, HEAP_KEYS_UPDATED, HEAP_MOVED,
    HEAP_XMAX_BITS, HEAP_XMAX_INVALID, HEAP_XMAX_IS_MULTI, READ_STREAM_SEQUENTIAL,
    VISIBILITYMAP_VALID_BITS,
};

use pgrx::pg_sys::LockTupleMode::LockTupleExclusive;
use pgrx::pg_sys::LockWaitPolicy::LockWaitBlock;
use pgrx::pg_sys::ScanOptions::*;
use pgrx::pg_sys::TM_Result::*;
use pgrx::pg_sys::TU_UpdateIndexes::{TU_All, TU_None, TU_Summarizing};
use pgrx::pg_sys::XLTW_Oper::XLTW_Delete;

// import functions
use pgrx::pg_sys::{
    palloc, pfree, pgstat_count_heap_delete, read_stream_begin_relation, read_stream_end,
    read_stream_reset, smgrnblocks, visibilitymap_clear, visibilitymap_pin, BufferGetBlockNumber,
    BufferGetPage, BufferIsValid, ExecFetchSlotHeapTuple, ExecStoreBufferHeapTuple,
    ExecStorePinnedBufferHeapTuple, FreeAccessStrategy, GetCurrentTransactionId,
    HeapTupleHeaderAdjustCmax, HeapTupleHeaderGetCmax, HeapTupleHeaderIsHeapOnly,
    HeapTupleHeaderIsOnlyLocked, HeapTupleSatisfiesUpdate, IsInParallelMode, ItemPointerCopy,
    ItemPointerEquals, ItemPointerGetBlockNumber, ItemPointerGetOffsetNumber,
    ItemPointerIndicatesMovedPartitions, ItemPointerIsValid, LockBuffer, MarkBufferDirty,
    MultiXactIdSetOldestMember, PageClearAllVisible, PageGetItem, PageGetItemId, PageIsAllVisible,
    ReadBuffer, RelationDecrementReferenceCount, RelationGetSmgr, RelationIncrementReferenceCount,
    ReleaseBuffer, TransactionIdIsCurrentTransactionId, UnlockReleaseBuffer, UnlockTuple,
    XactLockTableWait,
};
use pgrx::prelude::*;

pgrx::extension_sql!(
    "CREATE ACCESS METHOD rsam TYPE TABLE HANDLER rsam_am_handler;",
    requires = [rsam_am_handler],
    name = "rsam",
);

#[pg_guard]
unsafe extern "C-unwind" fn slot_callbacks(_rel: Relation) -> *const TupleTableSlotOps {
    // &raw const slot::TTSOpsRsAmTuple
    &raw const TTSOpsBufferHeapTuple // TODO: Implement own TupleTableSlotOps
}

#[pg_guard]
unsafe extern "C-unwind" fn scan_begin(
    rel: *mut RelationData,
    snapshot: *mut SnapshotData,
    nkeys: i32,
    key: ScanKey,
    pscan: *mut ParallelTableScanDescData,
    flags: u32,
) -> TableScanDesc {
    RelationIncrementReferenceCount(rel);

    // Allocate and initialize scan descriptor
    let scan = palloc(std::mem::size_of::<HeapScanDescData>()) as HeapScanDesc;
    (*scan).rs_base.rs_rd = rel;
    (*scan).rs_base.rs_snapshot = snapshot;
    (*scan).rs_base.rs_nkeys = nkeys;
    (*scan).rs_base.rs_flags = flags;
    (*scan).rs_base.rs_parallel = pscan;
    (*scan).rs_strategy = std::ptr::null_mut();
    (*scan).rs_cbuf = InvalidBuffer as i32;
    (*scan).rs_ctup.t_tableOid = (*rel).rd_id;

    if snapshot.is_null() || !IsMVCCSnapshot!(snapshot) {
        (*scan).rs_base.rs_flags &= !SO_ALLOW_PAGEMODE;
    }

    // serializable transaction stuff
    // if (*scan).rs_base.rs_flags & (SO_TYPE_SEQSCAN | SO_TYPE_SAMPLESCAN) != 0 {

    // }

    (*scan).rs_parallelworkerdata = if !pscan.is_null() {
        palloc(std::mem::size_of::<ParallelBlockTableScanWorkerData>()).cast()
    } else {
        std::ptr::null_mut()
    };

    // we do this here instead of in initscan() because heap_rescan also calls
    // initscan() and we don't want to allocate memory again
    (*scan).rs_base.rs_key = if nkeys > 0 {
        palloc(std::mem::size_of::<ScanKeyData>()) as ScanKey
    } else {
        std::ptr::null_mut()
    };

    initscan(scan, key, false);

    (*scan).rs_read_stream = std::ptr::null_mut();

    if (*scan).rs_base.rs_flags & SO_TYPE_SEQSCAN != 0 {
        let cb: ReadStreamBlockNumberCB = Some(heap_scan_stream_read_next_serial);
        (*scan).rs_read_stream = read_stream_begin_relation(
            READ_STREAM_SEQUENTIAL as i32,
            (*scan).rs_strategy,
            (*scan).rs_base.rs_rd,
            MAIN_FORKNUM,
            cb,
            scan.cast(),
            0,
        )
    }

    scan as TableScanDesc
}

#[pg_guard]
unsafe extern "C-unwind" fn scan_end(desc: TableScanDesc) {
    let scan = desc as HeapScanDesc;
    if BufferIsValid((*scan).rs_cbuf) {
        ReleaseBuffer((*scan).rs_cbuf);
    }

    if !(*scan).rs_read_stream.is_null() {
        read_stream_end((*scan).rs_read_stream);
    }

    RelationDecrementReferenceCount((*scan).rs_base.rs_rd);
    if !(*scan).rs_base.rs_key.is_null() {
        pfree((*scan).rs_base.rs_key.cast());
    }

    if !(*scan).rs_strategy.is_null() {
        FreeAccessStrategy((*scan).rs_strategy);
    }

    if !(*scan).rs_parallelworkerdata.is_null() {
        pfree((*scan).rs_parallelworkerdata.cast());
    }
    pfree(scan.cast())
}

#[pg_guard]
unsafe extern "C-unwind" fn scan_rescan(
    desc: TableScanDesc,
    key: *mut ScanKeyData,
    set_params: bool,
    allow_strat: bool,
    allow_sync: bool,
    allow_pagemode: bool,
) {
    let scan = desc as HeapScanDesc;
    if set_params {
        if allow_strat {
            (*scan).rs_base.rs_flags |= SO_ALLOW_STRAT;
        } else {
            (*scan).rs_base.rs_flags &= !SO_ALLOW_STRAT;
        }

        if allow_sync {
            (*scan).rs_base.rs_flags |= SO_ALLOW_SYNC;
        } else {
            (*scan).rs_base.rs_flags &= !SO_ALLOW_SYNC;
        }

        if allow_pagemode
            && !(*scan).rs_base.rs_snapshot.is_null()
            && IsMVCCSnapshot!((*scan).rs_base.rs_snapshot)
        {
            (*scan).rs_base.rs_flags |= SO_ALLOW_PAGEMODE;
        } else {
            (*scan).rs_base.rs_flags &= !SO_ALLOW_PAGEMODE;
        }
    }

    if BufferIsValid((*scan).rs_cbuf) {
        ReleaseBuffer((*scan).rs_cbuf);
        (*scan).rs_cbuf = InvalidBuffer as i32;
    }

    if !(*scan).rs_read_stream.is_null() {
        read_stream_reset((*scan).rs_read_stream);
    }

    initscan(scan, key, true);
}

#[pg_guard]
unsafe extern "C-unwind" fn scan_getnextslot(
    sscan: TableScanDesc,
    direction: ScanDirection::Type,
    slot: *mut TupleTableSlot,
) -> bool {
    let scan = sscan as HeapScanDesc;
    // if ((*sscan).rs_flags & SO_ALLOW_PAGEMODE) != 0 {
    //     heap_gettup_pagemode(scan, direction, (*sscan).rs_nkeys, (*sscan).rs_key);
    // } else {
    heap_gettup(scan, direction, (*sscan).rs_nkeys, (*sscan).rs_key);
    // }

    if (*scan).rs_ctup.t_data.is_null() {
        let clear = (*(*slot).tts_ops)
            .clear
            .expect("no clear method in TTSOps!");
        clear(slot);
        return false;
    }

    pgstat_count_heap_getnext((*scan).rs_base.rs_rd);
    ExecStoreBufferHeapTuple(&raw mut (*scan).rs_ctup, slot, (*scan).rs_cbuf);
    true
}

#[pg_guard]
unsafe extern "C-unwind" fn parallelscan_estimate(_rel: Relation) -> usize {
    todo!("parallelscan_estimate")
}

#[pg_guard]
unsafe extern "C-unwind" fn parallelscan_initialize(
    _rel: Relation,
    _pscan: ParallelTableScanDesc,
) -> usize {
    todo!("parallelscan_initialize")
}

#[pg_guard]
unsafe extern "C-unwind" fn parallelscan_reinitialize(
    _rel: Relation,
    _pscan: ParallelTableScanDesc,
) {
    todo!("parallelscan_reinitialize")
}

#[pg_guard]
unsafe extern "C-unwind" fn index_fetch_begin(_rel: Relation) -> *mut IndexFetchTableData {
    todo!("index_fetch_begin")
}

#[pg_guard]
unsafe extern "C-unwind" fn index_fetch_reset(_data: *mut IndexFetchTableData) {
    todo!("index_fetch_reset")
}

#[pg_guard]
unsafe extern "C-unwind" fn index_fetch_end(_data: *mut IndexFetchTableData) {
    todo!("index_fetch_end")
}

#[pg_guard]
unsafe extern "C-unwind" fn index_fetch_tuple(
    _scan: *mut IndexFetchTableData,
    _tid: ItemPointer,
    _snapshot: Snapshot,
    _slot: *mut TupleTableSlot,
    _call_again: *mut bool,
    _all_dead: *mut bool,
) -> bool {
    todo!("index_fetch_tuple")
}

#[pg_guard]
unsafe extern "C-unwind" fn tuple_fetch_row_version(
    rel: Relation,
    tid: ItemPointer,
    snapshot: Snapshot,
    slot: *mut TupleTableSlot,
) -> bool {
    let bslot = slot as *mut BufferHeapTupleTableSlot;
    let mut buffer = InvalidBuffer as i32;
    Assert((*slot).tts_ops == &TTSOpsBufferHeapTuple);

    (*bslot).base.tupdata.t_self = *tid;

    if fetch_tuple(
        rel,
        snapshot,
        &raw mut (*bslot).base.tupdata,
        &raw mut buffer,
        false,
    ) {
        ExecStorePinnedBufferHeapTuple(&raw mut (*bslot).base.tupdata, slot, buffer);
        (*slot).tts_tableOid = RelationGetRelId!(rel);
        return true;
    }
    false
}

#[pg_guard]
unsafe extern "C-unwind" fn tuple_tid_valid(_scan: TableScanDesc, _tid: ItemPointer) -> bool {
    todo!("tuple_tid_valid")
}

#[pg_guard]
unsafe extern "C-unwind" fn tuple_get_latest_tid(_scan: TableScanDesc, _tid: ItemPointer) {
    todo!("tuple_get_latest_tid")
}

#[pg_guard]
unsafe extern "C-unwind" fn tuple_satisfies_snapshot(
    _rel: Relation,
    _slot: *mut TupleTableSlot,
    _snapshot: Snapshot,
) -> bool {
    todo!("tuple_satisfies_snapshot")
}

#[pg_guard]
unsafe extern "C-unwind" fn index_delete_tuples(
    _rel: Relation,
    _delstate: *mut TM_IndexDeleteOp,
) -> TransactionId {
    todo!("index_delete_tuples")
}

#[pg_guard]
unsafe extern "C-unwind" fn tuple_insert(
    rel: Relation,
    slot: *mut TupleTableSlot,
    cid: CommandId,
    options: ::core::ffi::c_int,
    _bistate: *mut BulkInsertStateData,
) {
    let mut shouldFree = true;
    let tuple = ExecFetchSlotHeapTuple(slot, true, &raw mut shouldFree);
    (*slot).tts_tableOid = (*rel).rd_id;
    (*tuple).t_tableOid = (*rel).rd_id;

    // heap_insert(rel, tuple, cid, options, bistate);
    heap_insert(rel, tuple, cid, options, std::ptr::null_mut());
    ItemPointerCopy(&raw const (*tuple).t_self, &raw mut (*slot).tts_tid);

    if shouldFree {
        pfree(tuple.cast());
    }
}

#[pg_guard]
unsafe extern "C-unwind" fn tuple_insert_speculative(
    _rel: Relation,
    _slot: *mut TupleTableSlot,
    _cid: CommandId,
    _options: ::core::ffi::c_int,
    _bistate: *mut BulkInsertStateData,
    _spec_token: u32,
) {
    todo!("tuple_insert_speculative")
}

#[pg_guard]
unsafe extern "C-unwind" fn tuple_complete_speculative(
    _rel: Relation,
    _slot: *mut TupleTableSlot,
    _spec_token: u32,
    _succeeded: bool,
) {
    todo!("tuple_complete_speculative")
}

#[pg_guard]
unsafe extern "C-unwind" fn multi_insert(
    _rel: Relation,
    _slots: *mut *mut TupleTableSlot,
    _nslots: ::core::ffi::c_int,
    _cid: CommandId,
    _options: ::core::ffi::c_int,
    _bistate: *mut BulkInsertStateData,
) {
    todo!("multi_insert")
}

#[pg_guard]
unsafe extern "C-unwind" fn tuple_delete(
    rel: Relation,
    tid: ItemPointer,
    cid: CommandId,
    _snapshot: Snapshot,
    crosscheck: Snapshot,
    wait: bool,
    tmfd: *mut TM_FailureData,
    changing_part: bool,
) -> TM_Result::Type {
    let mut cid = cid;
    let xid = GetCurrentTransactionId();
    let mut vmbuffer = InvalidBuffer as i32;

    Assert(ItemPointerIsValid(tid));
    if IsInParallelMode() {
        ereport!(
            PgLogLevel::ERROR,
            PgSqlErrorCode::ERRCODE_INVALID_TRANSACTION_STATE,
            "cannot delete tuples during a parallel operation"
        )
    }

    let block = ItemPointerGetBlockNumber(tid);
    let buffer = ReadBuffer(rel, block);
    let page = BufferGetPage(buffer);
    if PageIsAllVisible(page) {
        visibilitymap_pin(rel, block, &raw mut vmbuffer);
    }

    LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE as i32);
    let lp = PageGetItemId(page, ItemPointerGetOffsetNumber(tid));
    Assert(ItemIdIsNormal!(lp));

    let mut tp: HeapTupleData = HeapTupleData {
        t_len: (*lp).lp_len(),
        t_self: *tid,
        t_tableOid: RelationGetRelId!(rel),
        t_data: PageGetItem(page, lp).cast(),
    };

    let (mut result, have_tuple_lock) = loop {
        let mut have_tuple_lock = false;

        if vmbuffer == InvalidBuffer as i32 && PageIsAllVisible(page) {
            LockBuffer(buffer, BUFFER_LOCK_UNLOCK as i32);
            visibilitymap_pin(rel, block, &raw mut vmbuffer);
            LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE as i32);
        }
        let mut result = HeapTupleSatisfiesUpdate(&raw mut tp, cid, buffer);
        if result == TM_Invisible {
            UnlockReleaseBuffer(buffer);
            ereport!(
                PgLogLevel::ERROR,
                PgSqlErrorCode::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
                "attempted to delete invisible tuple!"
            )
        } else if result == TM_BeingModified && wait {
            let xwait = (*tp.t_data).t_choice.t_heap.t_xmax;
            let infomask = (*tp.t_data).t_infomask;
            if infomask & HEAP_XMAX_IS_MULTI as u16 != 0 {
            } else if !TransactionIdIsCurrentTransactionId(xwait) {
                LockBuffer(buffer, BUFFER_LOCK_UNLOCK as i32);
                aquire_tuplock(
                    rel,
                    &raw mut (tp.t_self),
                    LockTupleExclusive,
                    LockWaitBlock,
                    &raw mut have_tuple_lock,
                );

                XactLockTableWait(xwait, rel, &raw mut tp.t_self, XLTW_Delete);
                LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE as i32);

                if (vmbuffer == InvalidBuffer as i32 && PageIsAllVisible(page))
                    || xmax_infomask_changed((*tp.t_data).t_infomask, infomask)
                    || !((*tp.t_data).t_choice.t_heap.t_xmax == xwait)
                {
                    continue;
                }

                UpdateXmaxHintBits(tp.t_data, buffer, xwait);
            }

            result = if (*tp.t_data).t_infomask & HEAP_XMAX_INVALID as u16 != 0
                || xmax_is_locked_only((*tp.t_data).t_infomask)
                || HeapTupleHeaderIsOnlyLocked(tp.t_data)
            {
                TM_Ok
            } else if !ItemPointerEquals(&raw mut tp.t_self, &raw mut (*tp.t_data).t_ctid) {
                TM_Updated
            } else {
                TM_Deleted
            }
        }

        break (result, have_tuple_lock);
    };

    if result != TM_Ok {
        Assert(
            result == TM_SelfModified
                || result == TM_Updated
                || result == TM_Deleted
                || result == TM_BeingModified,
        );
        Assert((*tp.t_data).t_infomask & HEAP_XMAX_INVALID as u16 == 0);
        Assert(
            result != TM_Updated
                || !ItemPointerEquals(&raw mut (tp.t_self), &raw mut (*tp.t_data).t_ctid),
        );
    }

    if !crosscheck.is_null() && result == TM_Ok {
        if !tuple_satisfies_visibility(&raw mut tp, crosscheck, buffer) {
            result = TM_Updated;
        }
    }

    if result != TM_Ok {
        (*tmfd).ctid = (*tp.t_data).t_ctid;
        (*tmfd).xmax = tuple_header_get_update_xid(tp.t_data);
        (*tmfd).cmax = if result == TM_SelfModified {
            HeapTupleHeaderGetCmax(tp.t_data)
        } else {
            InvalidCommandId
        };

        UnlockReleaseBuffer(buffer);
        if have_tuple_lock {
            UnlockTuple(rel, &raw mut (tp.t_self), LockTupleExclusive as i32);
        }

        if vmbuffer != InvalidBuffer as i32 {
            ReleaseBuffer(vmbuffer);
        }
        return result;
    }

    let mut is_combo = false;
    HeapTupleHeaderAdjustCmax(tp.t_data, &raw mut cid, &raw mut is_combo);
    let mut old_key_copied = false;
    let old_key_tuple = extract_replica_identity(rel, &raw mut tp, true, &raw mut old_key_copied);

    MultiXactIdSetOldestMember();
    let mut new_xmax = InvalidTransactionId;
    let mut new_infomask = 0;
    let mut new_infomask2 = 0;
    compute_new_xmax_infomask(
        (*tp.t_data).t_choice.t_heap.t_xmax,
        (*tp.t_data).t_infomask,
        (*tp.t_data).t_infomask2,
        xid,
        LockTupleExclusive,
        true,
        &raw mut new_xmax,
        &raw mut new_infomask,
        &raw mut new_infomask2,
    );

    START_CRIT_SECTION!();

    PageSetPrunable(page, xid);
    let mut all_visible_cleared = false;
    if PageIsAllVisible(page) {
        all_visible_cleared = true;
        PageClearAllVisible(page);
        visibilitymap_clear(
            rel,
            BufferGetBlockNumber(buffer),
            vmbuffer,
            VISIBILITYMAP_VALID_BITS as u8,
        );
    }

    (*tp.t_data).t_infomask &= !(HEAP_XMAX_BITS | HEAP_MOVED) as u16;
    (*tp.t_data).t_infomask2 &= !HEAP_KEYS_UPDATED as u16;
    (*tp.t_data).t_infomask |= new_infomask;
    (*tp.t_data).t_infomask2 |= new_infomask2;
    (*tp.t_data).t_infomask2 |= HEAP_HOT_UPDATED as u16;
    (*tp.t_data).t_choice.t_heap.t_xmax = new_xmax;
    header_set_cmax(tp.t_data, cid, is_combo);
    (*tp.t_data).t_ctid = tp.t_self;

    if changing_part {
        ItemPointerIndicatesMovedPartitions(&raw const (*tp.t_data).t_ctid);
    }

    MarkBufferDirty(buffer);
    // TODO: implement write ahead log stuff
    // if RelationNeedsWal!(rel) {}

    END_CRIT_SECTION!();

    LockBuffer(buffer, BUFFER_LOCK_UNLOCK as i32);
    if vmbuffer != InvalidBuffer as i32 {
        ReleaseBuffer(vmbuffer);
    }

    // TODO: implement deleting toast data if needed

    ReleaseBuffer(buffer);
    if have_tuple_lock {
        UnlockTuple(rel, &raw mut (tp.t_self), LockTupleExclusive as i32);
    }

    pgstat_count_heap_delete(rel);
    if old_key_copied && !old_key_tuple.is_null() {
        pfree(old_key_tuple.cast());
    }

    TM_Ok
}

#[pg_guard]
unsafe extern "C-unwind" fn rsam_tuple_update(
    rel: Relation,
    otid: ItemPointer,
    slot: *mut TupleTableSlot,
    cid: CommandId,
    snapshot: Snapshot,
    crosscheck: Snapshot,
    wait: bool,
    tmfd: *mut TM_FailureData,
    lockmode: *mut LockTupleMode::Type,
    update_indexes: *mut TU_UpdateIndexes::Type,
) -> TM_Result::Type {
    let mut shouldFree = true;
    let tuple = ExecFetchSlotHeapTuple(slot, true, &raw mut shouldFree);
    (*slot).tts_tableOid = RelationGetRelId!(rel);
    (*tuple).t_tableOid = (*slot).tts_tableOid;
    let result = tuple_update(
        rel,
        otid,
        tuple,
        cid,
        crosscheck,
        wait,
        tmfd,
        lockmode,
        update_indexes,
    );

    ItemPointerCopy(&raw const (*tuple).t_self, &raw mut (*slot).tts_tid);
    if result != TM_Ok {
        Assert(*update_indexes == TU_None);
    } else if !HeapTupleHeaderIsHeapOnly((*tuple).t_data) {
        Assert(*update_indexes == TU_All);
    } else {
        Assert(*update_indexes == TU_Summarizing || *update_indexes == TU_None);
    }

    if shouldFree {
        pfree(tuple.cast());
    }
    result
}

#[pg_guard]
unsafe extern "C-unwind" fn tuple_lock(
    _rel: Relation,
    _tid: ItemPointer,
    _snapshot: Snapshot,
    _slot: *mut TupleTableSlot,
    _cid: CommandId,
    _mode: LockTupleMode::Type,
    _wait_policy: LockWaitPolicy::Type,
    _flags: u8,
    _tmfd: *mut TM_FailureData,
) -> TM_Result::Type {
    todo!("tuple_lock")
}

/// Create new relation storage for `rel`.
/// Note that this is also used for `TRUNCATE <table>`!
#[pg_guard]
unsafe extern "C-unwind" fn relation_set_new_filelocator(
    rel: Relation,
    newrlocator: *const RelFileLocator,
    persistence: ::core::ffi::c_char,
    freeze_xid: *mut TransactionId,
    minmulti: *mut MultiXactId,
) {
    // TODO: properly support truncate.
    assert_eq!(
        (*(*rel).rd_rel).oid,
        Oid::INVALID,
        "relation_set_new_filelocator",
    );

    *freeze_xid = pg_sys::InvalidTransactionId;
    *minmulti = pg_sys::InvalidTransactionId;

    let srel = pg_sys::RelationCreateStorage(*newrlocator, persistence, false);
    pg_sys::smgrclose(srel);
}

#[pg_guard]
unsafe extern "C-unwind" fn relation_nontransactional_truncate(_rel: Relation) {
    todo!("relation_nontransactional_truncate")
}

#[pg_guard]
unsafe extern "C-unwind" fn relation_copy_data(
    _rel: Relation,
    _newrlocator: *const RelFileLocator,
) {
    todo!("relation_copy_data")
}

#[pg_guard]
unsafe extern "C-unwind" fn relation_copy_for_cluster(
    _old_table: Relation,
    _new_table: Relation,
    _old_index: Relation,
    _use_sort: bool,
    _oldest_xmin: TransactionId,
    _xid_cutoff: *mut TransactionId,
    _multi_cutoff: *mut MultiXactId,
    _num_tuples: *mut f64,
    _tups_vacuumed: *mut f64,
    _tups_recently_dead: *mut f64,
) {
    todo!("relation_copy_for_cluster")
}

#[pg_guard]
unsafe extern "C-unwind" fn relation_vacuum(
    _rel: Relation,
    _params: *mut VacuumParams,
    _bstrategy: BufferAccessStrategy,
) {
    todo!("relation_vacuum")
}

#[pg_guard]
unsafe extern "C-unwind" fn scan_analyze_next_block(
    _scan: TableScanDesc,
    _stream: *mut ReadStream,
) -> bool {
    todo!("scan_analyze_next_block")
}

#[pg_guard]
unsafe extern "C-unwind" fn scan_analyze_next_tuple(
    _scan: TableScanDesc,
    _oldest_xmin: TransactionId,
    _liverows: *mut f64,
    _deadrows: *mut f64,
    _slot: *mut TupleTableSlot,
) -> bool {
    todo!("scan_analyze_next_tuple")
}

#[pg_guard]
unsafe extern "C-unwind" fn index_build_range_scan(
    _table_rel: Relation,
    _index_rel: Relation,
    _index_info: *mut IndexInfo,
    _allow_sync: bool,
    _anyvisible: bool,
    _progress: bool,
    _start_blockno: BlockNumber,
    _numblocks: BlockNumber,
    _callback: IndexBuildCallback,
    _callback_state: *mut ::core::ffi::c_void,
    _scan: TableScanDesc,
) -> f64 {
    todo!("index_build_range_scan")
}

#[pg_guard]
unsafe extern "C-unwind" fn index_validate_scan(
    _table_rel: Relation,
    _index_rel: Relation,
    _index_info: *mut IndexInfo,
    _snapshot: Snapshot,
    _state: *mut ValidateIndexState,
) {
    todo!("index_validate_scan")
}

#[pg_guard]
unsafe extern "C-unwind" fn relation_size(rel: Relation, fork_number: ForkNumber::Type) -> u64 {
    let mut nblocks: u64 = 0;
    if fork_number == InvalidForkNumber {
        for i in 0..INIT_FORKNUM {
            nblocks += smgrnblocks(RelationGetSmgr(rel), i) as u64;
        }
    } else {
        nblocks = smgrnblocks(RelationGetSmgr(rel), fork_number) as u64;
    }

    nblocks * BLCKSZ as u64
}

#[pg_guard]
unsafe extern "C-unwind" fn relation_needs_toast_table(_rel: Relation) -> bool {
    false
}

#[pg_guard]
unsafe extern "C-unwind" fn relation_estimate_size(
    rel: Relation,
    attr_widths: *mut i32,
    pages: *mut BlockNumber,
    tuples: *mut f64,
    allvisfrac: *mut f64,
) {
    let _tuple_width = pg_sys::get_rel_data_width(rel, attr_widths);
    *pages = 0;
    *tuples = 0.0;
    *allvisfrac = 0.0;
}

#[pg_guard]
unsafe extern "C-unwind" fn scan_sample_next_block(
    _scan: TableScanDesc,
    _scanstate: *mut SampleScanState,
) -> bool {
    todo!("scan_sample_next_block")
}

#[pg_guard]
unsafe extern "C-unwind" fn scan_sample_next_tuple(
    _scan: TableScanDesc,
    _scanstate: *mut SampleScanState,
    _slot: *mut TupleTableSlot,
) -> bool {
    todo!("scan_sample_next_tuple")
}

#[pg_extern(strict)]
fn rsam_am_handler(_internal: TableAmArgs) -> TableAmHandler {
    // See postgres : GetTableAmRoutine.
    static VTABLE: TableAmRoutine = const {
        let mut vtable = new_table_am_routine();

        vtable.slot_callbacks = Some(slot_callbacks);

        vtable.scan_begin = Some(scan_begin);
        vtable.scan_end = Some(scan_end);
        vtable.scan_rescan = Some(scan_rescan);
        vtable.scan_getnextslot = Some(scan_getnextslot);

        vtable.parallelscan_estimate = Some(parallelscan_estimate);
        vtable.parallelscan_initialize = Some(parallelscan_initialize);
        vtable.parallelscan_reinitialize = Some(parallelscan_reinitialize);

        vtable.index_fetch_begin = Some(index_fetch_begin);
        vtable.index_fetch_reset = Some(index_fetch_reset);
        vtable.index_fetch_end = Some(index_fetch_end);
        vtable.index_fetch_tuple = Some(index_fetch_tuple);

        vtable.tuple_fetch_row_version = Some(tuple_fetch_row_version);
        vtable.tuple_tid_valid = Some(tuple_tid_valid);
        vtable.tuple_get_latest_tid = Some(tuple_get_latest_tid);
        vtable.tuple_satisfies_snapshot = Some(tuple_satisfies_snapshot);
        vtable.index_delete_tuples = Some(index_delete_tuples);

        vtable.tuple_insert = Some(tuple_insert);
        vtable.tuple_insert_speculative = Some(tuple_insert_speculative);
        vtable.tuple_complete_speculative = Some(tuple_complete_speculative);

        vtable.multi_insert = Some(multi_insert);
        vtable.tuple_delete = Some(tuple_delete);
        vtable.tuple_update = Some(rsam_tuple_update);
        vtable.tuple_lock = Some(tuple_lock);

        vtable.relation_set_new_filelocator = Some(relation_set_new_filelocator);
        vtable.relation_nontransactional_truncate = Some(relation_nontransactional_truncate);
        vtable.relation_copy_data = Some(relation_copy_data);
        vtable.relation_copy_for_cluster = Some(relation_copy_for_cluster);
        vtable.relation_vacuum = Some(relation_vacuum);
        vtable.scan_analyze_next_block = Some(scan_analyze_next_block);
        vtable.scan_analyze_next_tuple = Some(scan_analyze_next_tuple);
        vtable.index_build_range_scan = Some(index_build_range_scan);
        vtable.index_validate_scan = Some(index_validate_scan);

        vtable.relation_size = Some(relation_size);
        vtable.relation_needs_toast_table = Some(relation_needs_toast_table);

        vtable.relation_estimate_size = Some(relation_estimate_size);
        vtable.scan_sample_next_block = Some(scan_sample_next_block);
        vtable.scan_sample_next_tuple = Some(scan_sample_next_tuple);

        vtable
    };

    TableAmHandler(&VTABLE)
}
