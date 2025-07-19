use pg_sys::WalLevel::WAL_LEVEL_REPLICA;
use pgrx::pg_sys::ForkNumber::MAIN_FORKNUM;
use pgrx::pg_sys::{
    pfree, pgstat_count_heap_insert, visibilitymap_clear, visibilitymap_pin, wal_level,
    BlockNumber, Buffer, BufferGetBlockNumber, BufferGetPage, BulkInsertStateData, CommandId,
    CritSectionCount, GetCurrentTransactionId, HeapTuple, HeapTupleData, HeapTupleHeader,
    HeapTupleHeaderData, InvalidBlockNumber, InvalidBuffer, InvalidOffsetNumber, Item, ItemIdData,
    ItemPointerGetBlockNumber, ItemPointerSet, MarkBufferDirty, PageAddItemExtended,
    PageClearAllVisible, PageGetItem, PageGetItemId, PageIsAllVisible, ParallelWorkerNumber,
    RelationData, RelationGetNumberOfBlocksInFork, ReleaseBuffer, SizeOfPageHeaderData,
    StdRdOptions, TransactionId, UnlockReleaseBuffer, BLCKSZ, HEAP2_XACT_MASK, HEAP_COMBOCID,
    HEAP_DEFAULT_FILLFACTOR, HEAP_INSERT_SKIP_FSM, HEAP_XACT_MASK, HEAP_XMAX_INVALID, MAXALIGN,
    PAI_IS_HEAP, PAI_OVERWRITE, RELPERSISTENCE_PERMANENT, VISIBILITYMAP_VALID_BITS, Page
};

// overwrite
use pgrx::prelude::*;
use std::cmp::{max, min};

#[macro_export]
macro_rules! MaxHeapTupleSize {
    () => {
        BLCKSZ as usize - MAXALIGN(SizeOfPageHeaderData() + std::mem::size_of::<ItemIdData>())
    };
}

#[macro_export]
macro_rules! MaxHeapTuplesPerPage {
    () => {
        (BLCKSZ as usize - SizeOfPageHeaderData())
            / (MAXALIGN(SizeOfHeapTupleHeader!()) + std::mem::size_of::<ItemIdData>())
    };
}

#[macro_export]
macro_rules! SizeOfHeapTupleHeader {
    () => {
        std::mem::offset_of!(HeapTupleHeaderData, t_bits)
    };
}

#[macro_export]
macro_rules! MinHeapTupleSize {
    () => {
        MAXALIGN(SizeOfHeapTupleHeader!())
    };
}

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
macro_rules! RelationGetFillFactor {
    ($rel:expr, $defaultff:expr) => {
        if !(*$rel).rd_options.is_null() {
            (*((*$rel).rd_options as *mut StdRdOptions)).fillfactor
        } else {
            $defaultff
        }
    };
}

#[macro_export]
macro_rules! RelationGetTargetPageFreeSpace {
    ($rel:expr, $defaultff: expr) => {
        BLCKSZ as usize * (100 - RelationGetFillFactor!($rel, $defaultff) / 100) as usize
    };
}

#[macro_export]
macro_rules! RelationGetTargetBlock {
    ($rel:expr) => {
        if !(*$rel).rd_smgr.is_null() {
            (*(*$rel).rd_smgr).smgr_targblock
        } else {
            InvalidBlockNumber
        }
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

#[macro_export]
macro_rules! PageAddItem {
    ($page: expr, $item: expr, $size:expr, $offn:expr, $overwrite:expr, $is_heap:expr) => {
        if $is_heap {
            if $overwrite {
                PageAddItemExtended(
                    $page,
                    $item,
                    $size,
                    $offn,
                    (PAI_OVERWRITE | PAI_IS_HEAP) as i32,
                )
            } else {
                PageAddItemExtended($page, $item, $size, $offn, PAI_IS_HEAP as i32)
            }
        } else {
            if $overwrite {
                PageAddItemExtended($page, $item, $size, $offn, PAI_OVERWRITE as i32)
            } else {
                PageAddItemExtended($page, $item, $size, $offn, 0)
            }
        }
    };
}

#[pg_guard]
#[allow(non_snake_case)]
unsafe extern "C-unwind" fn heap_tuple_header_set_Xmin(
    tup: *mut HeapTupleHeaderData,
    tid: TransactionId,
) {
    (*tup).t_choice.t_heap.t_xmin = tid;
}

#[pg_guard]
#[allow(non_snake_case)]
unsafe extern "C-unwind" fn heap_tuple_header_set_Xmax(
    tup: *mut HeapTupleHeaderData,
    tid: TransactionId,
) {
    (*tup).t_choice.t_heap.t_xmax = tid;
}

#[pg_guard]
#[allow(non_snake_case)]
unsafe extern "C-unwind" fn heap_tuple_header_set_Cmin(
    tup: *mut HeapTupleHeaderData,
    cid: CommandId,
) {
    (*tup).t_choice.t_heap.t_field3.t_cid = cid;
    (*tup).t_infomask &= !(HEAP_COMBOCID as u16);
}

#[pg_guard]
pub unsafe extern "C-unwind" fn prepare_insert(
    rel: *mut RelationData,
    tup: *mut HeapTupleData,
    tid: TransactionId,
    cid: CommandId,
    options: i32,
) -> HeapTuple {
    (*(*tup).t_data).t_infomask &= !HEAP_XACT_MASK as u16;
    (*(*tup).t_data).t_infomask2 &= !HEAP2_XACT_MASK as u16;
    (*(*tup).t_data).t_infomask |= HEAP_XMAX_INVALID as u16;
    heap_tuple_header_set_Xmin((*tup).t_data, tid);
    heap_tuple_header_set_Cmin((*tup).t_data, cid);
    heap_tuple_header_set_Xmax((*tup).t_data, 0.into());
    (*tup).t_tableOid = (*rel).rd_id;

    // TODO: here should be toast stuff
    tup
}

#[pg_guard]
#[allow(non_snake_case)] // TODO: implement this function
pub unsafe extern "C-unwind" fn RelationGetBufferForTuple(
    rel: *mut RelationData,
    len: usize,
    otherBuffer: Buffer,
    options: i32,
    state: *mut BulkInsertStateData,
    vmbuffer: *mut Buffer,
    vmbuffer_other: *mut Buffer,
    mut num_pages: i32,
) -> Buffer {
    let len = MAXALIGN(len);
    let use_fsm: bool = (options & HEAP_INSERT_SKIP_FSM as i32) != 0;
    let mut buffer: Buffer = InvalidBuffer as i32;
    let (mut pageFreeSpace, mut saveFreeSpace, mut targetFreeSpace): (usize, usize, usize) =
        (0, 0, 0);
    if num_pages <= 0 {
        num_pages = 1;
    }

    if len > MaxHeapTupleSize!() {}
    saveFreeSpace = RelationGetTargetPageFreeSpace!(rel, HEAP_DEFAULT_FILLFACTOR as i32);
    let nearlyEmptySpace: usize =
        MaxHeapTupleSize!() - (MaxHeapTuplesPerPage!() / 8 * std::mem::size_of::<ItemIdData>());

    targetFreeSpace = if len + saveFreeSpace > nearlyEmptySpace {
        max(len, nearlyEmptySpace)
    } else {
        len + saveFreeSpace
    };

    let otherBlock = if otherBuffer != InvalidBuffer as i32 {
        BufferGetBlockNumber(otherBuffer)
    } else {
        InvalidBlockNumber
    };

    // (*(*rel).rd_smgr).smgr_targblock;
    let mut targetBlock = RelationGetTargetBlock!(rel);
    // TODO: implement this
    if targetBlock == InvalidBlockNumber && use_fsm {}

    if targetBlock == InvalidBlockNumber {
        let nblocks: BlockNumber = RelationGetNumberOfBlocksInFork(rel, MAIN_FORKNUM);
        if nblocks > 0 {
            targetBlock = nblocks - 1;
        }
    }

    'cycle: while targetBlock != InvalidBlockNumber {}

    // buffer = RelationAddBlocks();
    targetBlock = BufferGetBlockNumber(buffer);
    let page = BufferGetPage(buffer);

    buffer
}

#[pg_guard]
pub unsafe extern "C-unwind" fn relation_put_tuple(
    _rel: *mut RelationData,
    buffer: Buffer,
    tuple: *mut HeapTupleData,
) {
    let pageHeader = BufferGetPage(buffer);
    let offnum: u16 = PageAddItem!(
        pageHeader,
        (*tuple).t_data as Item,
        (*tuple).t_len as usize,
        InvalidOffsetNumber,
        false,
        true
    );

    // TODO: implement failure in case of failing to insert a tuple to the page
    // if offnum == InvalidOffsetNumber{

    // }

    ItemPointerSet(
        &raw mut (*tuple).t_self,
        BufferGetBlockNumber(buffer),
        offnum,
    );
    let itemId = PageGetItemId(pageHeader, offnum);
    let item: HeapTupleHeader = PageGetItem(pageHeader, itemId).cast();
    (*item).t_ctid = (*tuple).t_self;
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

    /* Fill in tuple header fields and toast the tuple if necessary.

    Note: below this point, heaptup is the data we actually intend to store
    into the relation; tup is the caller's original untoasted data.
    */
    let tuple = prepare_insert(rel, tup, xid, cid, options);
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
    relation_put_tuple(rel, buffer, tuple);
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
