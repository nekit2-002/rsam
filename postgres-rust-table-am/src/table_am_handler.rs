use crate::am_handler_port::{new_table_am_routine, TableAmArgs, TableAmHandler, TableAmRoutine};
use pg_sys::{
    BlockNumber, BufferAccessStrategy, BulkInsertStateData, CommandId, ForkNumber,
    IndexBuildCallback, IndexFetchTableData, IndexInfo, ItemPointer, LockTupleMode, LockWaitPolicy,
    MultiXactId, Oid, ParallelTableScanDesc, ParallelTableScanDescData, ReadStream, RelFileLocator,
    Relation, RelationData, SampleScanState, ScanDirection, ScanKeyData, Snapshot, SnapshotData,
    TM_FailureData, TM_IndexDeleteOp, TM_Result, TU_UpdateIndexes, TableScanDesc, TransactionId,
    TupleTableSlot, TupleTableSlotOps, VacuumParams, ValidateIndexState,
};
use pgrx::prelude::*;

pgrx::extension_sql!(
    "CREATE ACCESS METHOD rsam TYPE TABLE HANDLER rsam_am_handler;",
    requires = [rsam_am_handler],
    name = "rsam",
);

#[pg_guard]
unsafe extern "C" fn slot_callbacks(_rel: Relation) -> *const TupleTableSlotOps {
    &raw const pg_sys::TTSOpsHeapTuple // TODO: Implement own TupleTableSlotOps
}

#[pg_guard]
unsafe extern "C" fn scan_begin(
    _rel: *mut RelationData,
    _snapshot: *mut SnapshotData,
    _nkeys: i32,
    _keys: *mut ScanKeyData,
    _pscan: *mut ParallelTableScanDescData,
    _flags: u32,
) -> pg_sys::TableScanDesc {
    todo!("scan_begin")
}

#[pg_guard]
unsafe extern "C" fn scan_end(_desc: TableScanDesc) {
    todo!("scan_end")
}

#[pg_guard]
unsafe extern "C" fn scan_rescan(
    _desc: TableScanDesc,
    _keys: *mut ScanKeyData,
    _set_params: bool,
    _allow_strat: bool,
    _allow_sync: bool,
    _allow_pagemode: bool,
) {
    todo!("scan_rescan")
}

#[pg_guard]
unsafe extern "C" fn scan_getnextslot(
    _scan: TableScanDesc,
    _direction: ScanDirection::Type,
    _slot: *mut TupleTableSlot,
) -> bool {
    todo!("scan_getnextslot")
}

#[pg_guard]
unsafe extern "C" fn parallelscan_estimate(_rel: Relation) -> usize {
    todo!("parallelscan_estimate")
}

#[pg_guard]
unsafe extern "C" fn parallelscan_initialize(
    _rel: Relation,
    _pscan: ParallelTableScanDesc,
) -> usize {
    todo!("parallelscan_initialize")
}

#[pg_guard]
unsafe extern "C" fn parallelscan_reinitialize(_rel: Relation, _pscan: ParallelTableScanDesc) {
    todo!("parallelscan_reinitialize")
}

#[pg_guard]
unsafe extern "C" fn index_fetch_begin(_rel: Relation) -> *mut IndexFetchTableData {
    todo!("index_fetch_begin")
}

#[pg_guard]
unsafe extern "C" fn index_fetch_reset(_data: *mut IndexFetchTableData) {
    todo!("index_fetch_reset")
}

#[pg_guard]
unsafe extern "C" fn index_fetch_end(_data: *mut IndexFetchTableData) {
    todo!("index_fetch_end")
}

#[pg_guard]
unsafe extern "C" fn index_fetch_tuple(
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
unsafe extern "C" fn tuple_fetch_row_version(
    _rel: Relation,
    _tid: ItemPointer,
    _snapshot: Snapshot,
    _slot: *mut TupleTableSlot,
) -> bool {
    todo!("tuple_fetch_row_version")
}

#[pg_guard]
unsafe extern "C" fn tuple_tid_valid(_scan: TableScanDesc, _tid: ItemPointer) -> bool {
    todo!("tuple_tid_valid")
}

#[pg_guard]
unsafe extern "C" fn tuple_get_latest_tid(_scan: TableScanDesc, _tid: ItemPointer) {
    todo!("tuple_get_latest_tid")
}

#[pg_guard]
unsafe extern "C" fn tuple_satisfies_snapshot(
    _rel: Relation,
    _slot: *mut TupleTableSlot,
    _snapshot: Snapshot,
) -> bool {
    todo!("tuple_satisfies_snapshot")
}

#[pg_guard]
unsafe extern "C" fn index_delete_tuples(
    _rel: Relation,
    _delstate: *mut TM_IndexDeleteOp,
) -> TransactionId {
    todo!("index_delete_tuples")
}

#[pg_guard]
unsafe extern "C" fn tuple_insert(
    _rel: Relation,
    _slot: *mut TupleTableSlot,
    _cid: CommandId,
    _options: ::core::ffi::c_int,
    _bistate: *mut BulkInsertStateData,
) {
    todo!("tuple_insert")
}

#[pg_guard]
unsafe extern "C" fn tuple_insert_speculative(
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
unsafe extern "C" fn tuple_complete_speculative(
    _rel: Relation,
    _slot: *mut TupleTableSlot,
    _spec_token: u32,
    _succeeded: bool,
) {
    todo!("tuple_complete_speculative")
}

#[pg_guard]
unsafe extern "C" fn multi_insert(
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
unsafe extern "C" fn tuple_delete(
    _rel: Relation,
    _tid: ItemPointer,
    _cid: CommandId,
    _snapshot: Snapshot,
    _crosscheck: Snapshot,
    _wait: bool,
    _tmfd: *mut TM_FailureData,
    _changing_part: bool,
) -> TM_Result::Type {
    todo!("tuple_delete")
}

#[pg_guard]
unsafe extern "C" fn tuple_update(
    _rel: Relation,
    _otid: ItemPointer,
    _slot: *mut TupleTableSlot,
    _cid: CommandId,
    _snapshot: Snapshot,
    _crosscheck: Snapshot,
    _wait: bool,
    _tmfd: *mut TM_FailureData,
    _lockmode: *mut LockTupleMode::Type,
    _update_indexes: *mut TU_UpdateIndexes::Type,
) -> TM_Result::Type {
    todo!("tuple_update")
}

#[pg_guard]
unsafe extern "C" fn tuple_lock(
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
unsafe extern "C" fn relation_set_new_filelocator(
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
unsafe extern "C" fn relation_nontransactional_truncate(_rel: Relation) {
    todo!("relation_nontransactional_truncate")
}

#[pg_guard]
unsafe extern "C" fn relation_copy_data(_rel: Relation, _newrlocator: *const RelFileLocator) {
    todo!("relation_copy_data")
}

#[pg_guard]
unsafe extern "C" fn relation_copy_for_cluster(
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
unsafe extern "C" fn relation_vacuum(
    _rel: Relation,
    _params: *mut VacuumParams,
    _bstrategy: BufferAccessStrategy,
) {
    todo!("relation_vacuum")
}

#[pg_guard]
unsafe extern "C" fn scan_analyze_next_block(
    _scan: TableScanDesc,
    _stream: *mut ReadStream,
) -> bool {
    todo!("scan_analyze_next_block")
}

#[pg_guard]
unsafe extern "C" fn scan_analyze_next_tuple(
    _scan: TableScanDesc,
    _oldest_xmin: TransactionId,
    _liverows: *mut f64,
    _deadrows: *mut f64,
    _slot: *mut TupleTableSlot,
) -> bool {
    todo!("scan_analyze_next_tuple")
}

#[pg_guard]
unsafe extern "C" fn index_build_range_scan(
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
unsafe extern "C" fn index_validate_scan(
    _table_rel: Relation,
    _index_rel: Relation,
    _index_info: *mut IndexInfo,
    _snapshot: Snapshot,
    _state: *mut ValidateIndexState,
) {
    todo!("index_validate_scan")
}

#[pg_guard]
unsafe extern "C" fn relation_size(_rel: Relation, _fork_number: ForkNumber::Type) -> u64 {
    todo!("relation_size")
}

#[pg_guard]
unsafe extern "C" fn relation_needs_toast_table(_rel: Relation) -> bool {
    false
}

#[pg_guard]
unsafe extern "C" fn relation_estimate_size(
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
unsafe extern "C" fn scan_sample_next_block(
    _scan: TableScanDesc,
    _scanstate: *mut SampleScanState,
) -> bool {
    todo!("scan_sample_next_block")
}

#[pg_guard]
unsafe extern "C" fn scan_sample_next_tuple(
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
        vtable.tuple_update = Some(tuple_update);
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
