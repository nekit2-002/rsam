use pgrx::pg_sys::ExtendBufferedFlags::EB_LOCK_FIRST;
use pgrx::pg_sys::{
    pfree, pgstat_count_heap_insert, visibilitymap_clear, visibilitymap_pin, visibilitymap_pin_ok,
    BlockNumber, Buffer, BufferGetBlockNumber, BufferGetPage, BufferGetPageSize, BufferIsValid,
    BufferManagerRelation, BulkInsertState, BulkInsertStateData, CommandId, ConditionalLockBuffer,
    CritSectionCount, ExtendBufferedRelBy, GetCurrentTransactionId, GetPageWithFreeSpace,
    HeapTuple, HeapTupleData, HeapTupleHeader, HeapTupleHeaderData, InvalidBlockNumber,
    InvalidBuffer, InvalidOffsetNumber, Item, ItemIdData, ItemPointerGetBlockNumber,
    ItemPointerSet, LockBuffer, MarkBufferDirty, Page, PageAddItemExtended, PageClearAllVisible,
    PageGetHeapFreeSpace, PageGetItem, PageGetItemId, PageInit, PageIsAllVisible, PageIsNew,
    ReadBuffer, RecordAndGetPageWithFreeSpace, RelationData, RelationGetNumberOfBlocksInFork,
    RelationGetSmgr, ReleaseBuffer, SizeOfPageHeaderData, StdRdOptions, TransactionId,
    UnlockReleaseBuffer, BLCKSZ, BUFFER_LOCK_EXCLUSIVE, BUFFER_LOCK_UNLOCK, HEAP2_XACT_MASK,
    HEAP_COMBOCID, HEAP_DEFAULT_FILLFACTOR, HEAP_INSERT_FROZEN, HEAP_INSERT_SKIP_FSM,
    HEAP_XACT_MASK, HEAP_XMAX_INVALID, MAXALIGN, PAI_IS_HEAP, PAI_OVERWRITE, RELPERSISTENCE_TEMP,
    VISIBILITYMAP_VALID_BITS,
};
use pgrx::pg_sys::{
    ForkNumber::*, FreeSpaceMapVacuumRange, RecordPageWithFreeSpace,
    RelationExtensionLockWaiterCount,
};

use crate::include::relaion_macro::*;
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
macro_rules! XLogIsNeeded {
    () => {
        wal_level >= pg_sys::WalLevel::WAL_LEVEL_REPLICA as i32
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
    _options: i32,
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
#[allow(non_snake_case)]
unsafe extern "C-unwind" fn RelationAddBlocks(
    rel: *mut RelationData,
    _state: BulkInsertState,
    num_pages: i32,
    use_fsm: bool,
    did_unlock: *mut bool,
) -> Buffer {
    let mut victim_buffers: [Buffer; 64] = [0; 64];
    let mut extend_by_pages = if !use_fsm {
        1
    } else {
        let mut extend_by_pages = num_pages;
        let waitcount = if !RelationIsLocal!(rel) {
            RelationExtensionLockWaiterCount(rel)
        } else {
            0
        };

        extend_by_pages += extend_by_pages * waitcount;
        min(extend_by_pages, 64)
    } as u32;

    let not_in_fsm_pages = if num_pages > 1 { 1 } else { num_pages as u32 };
    let fst_block = ExtendBufferedRelBy(
        BufferManagerRelation {
            rel: rel,
            smgr: std::ptr::null_mut(),
            relpersistence: (*(*rel).rd_rel).relpersistence,
        },
        MAIN_FORKNUM,
        std::ptr::null_mut(),
        EB_LOCK_FIRST,
        extend_by_pages,
        victim_buffers.as_mut_ptr(),
        &raw mut extend_by_pages,
    );

    let buffer = victim_buffers[0];
    let last_block = fst_block + (extend_by_pages - 1);
    let page = BufferGetPage(buffer);
    if !PageIsNew(page) {
        ereport!(
            PgLogLevel::ERROR,
            PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
            "new page should be empty but is not!"
        );
    }

    PageInit(page, BufferGetPageSize(buffer), 0);
    MarkBufferDirty(buffer);
    *did_unlock = if use_fsm && not_in_fsm_pages < extend_by_pages {
        LockBuffer(buffer, BUFFER_LOCK_UNLOCK as i32);
        true
    } else {
        false
    };

    for i in 1..extend_by_pages {
        let cur = fst_block + i;
        ReleaseBuffer(victim_buffers[i as usize]);
        if use_fsm && i >= not_in_fsm_pages {
            let free_space = BufferGetPageSize(victim_buffers[i as usize]) - SizeOfPageHeaderData();
            RecordPageWithFreeSpace(rel, cur, free_space);
        }
    }

    if use_fsm && not_in_fsm_pages < extend_by_pages {
        let fst_fsm_block = fst_block + not_in_fsm_pages;
        FreeSpaceMapVacuumRange(rel, fst_fsm_block, last_block);
    }

    buffer
}

#[pg_guard]
#[allow(non_snake_case)]
unsafe extern "C-unwind" fn GetVisibilityMapPins(
    rel: *mut RelationData,
    mut buffer1: Buffer,
    mut buffer2: Buffer,
    mut block1: BlockNumber,
    mut block2: BlockNumber,
    vmbuffer1: *mut Buffer,
    vmbuffer2: *mut Buffer,
) -> bool {
    let mut released_blocks = false;
    if !BufferIsValid(buffer1) || (BufferIsValid(buffer2) && block1 > block2) {
        std::mem::swap(&mut buffer1, &mut buffer2);
        std::mem::swap(&mut block1, &mut block2);
        std::ptr::swap(vmbuffer1, vmbuffer2);
    }

    loop {
        let need_to_pin_buffer1 =
            PageIsAllVisible(BufferGetPage(buffer1)) && !visibilitymap_pin_ok(block1, *vmbuffer1);
        let need_to_pin_buffer2 = buffer2 != InvalidBuffer as i32
            && PageIsAllVisible(BufferGetPage(buffer2))
            && !visibilitymap_pin_ok(block2, *vmbuffer2);

        if !need_to_pin_buffer1 && !need_to_pin_buffer2 {
            break;
        }

        released_blocks = true;
        LockBuffer(buffer1, BUFFER_LOCK_UNLOCK as i32);
        if buffer2 != InvalidBuffer as i32 && buffer2 != buffer1 {
            LockBuffer(buffer2, BUFFER_LOCK_UNLOCK as i32);
        }

        if need_to_pin_buffer1 {
            visibilitymap_pin(rel, block1, vmbuffer1);
        }
        if need_to_pin_buffer2 {
            visibilitymap_pin(rel, block2, vmbuffer2);
        }

        LockBuffer(buffer1, BUFFER_LOCK_EXCLUSIVE as i32);
        if buffer2 != InvalidBuffer as i32 && buffer2 != buffer1 {
            LockBuffer(buffer2, BUFFER_LOCK_EXCLUSIVE as i32);
        }
        if buffer2 == InvalidBuffer as i32
            || buffer1 == buffer2
            || (need_to_pin_buffer1 && need_to_pin_buffer2)
        {
            break;
        }
    }
    released_blocks
}

#[pg_guard]
#[allow(non_snake_case)]
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
    let len = MAXALIGN(len);
    // it is safe to init page with null pointer as it is always reinited before being read
    let mut page: Page = std::ptr::null_mut();
    let use_fsm: bool = (options & HEAP_INSERT_SKIP_FSM as i32) == 0;
    let mut buffer: Buffer = InvalidBuffer as i32;
    let mut pageFreeSpace: usize = 0;
    let num_pages = if num_pages <= 0 { 1 } else { num_pages };

    if len > MaxHeapTupleSize!() {
        ereport!(
            PgLogLevel::ERROR,
            PgSqlErrorCode::ERRCODE_PROGRAM_LIMIT_EXCEEDED,
            "row is too big!"
        );
    }
    /* Compute desired extra freespace due to fillfactor option */
    let saveFreeSpace = RelationGetTargetPageFreeSpace!(rel, HEAP_DEFAULT_FILLFACTOR as i32);
    let nearlyEmptySpace: usize =
        MaxHeapTupleSize!() - (MaxHeapTuplesPerPage!() / 8 * std::mem::size_of::<ItemIdData>());

    let targetFreeSpace = if len + saveFreeSpace > nearlyEmptySpace {
        max(len, nearlyEmptySpace)
    } else {
        len + saveFreeSpace
    };

    let otherBlock = if otherBuffer != InvalidBuffer as i32 {
        BufferGetBlockNumber(otherBuffer)
    } else {
        InvalidBlockNumber
    };

    let mut targetBlock = RelationGetTargetBlock!(rel);

    if targetBlock == InvalidBlockNumber && use_fsm {
        targetBlock = GetPageWithFreeSpace(rel, targetFreeSpace)
    }

    if targetBlock == InvalidBlockNumber {
        let nblocks: BlockNumber = RelationGetNumberOfBlocksInFork(rel, MAIN_FORKNUM);
        if nblocks > 0 {
            targetBlock = nblocks - 1;
        }
    }

    loop {
        while targetBlock != InvalidBlockNumber {
            if otherBuffer == InvalidBuffer as i32 {
            } else if otherBlock == targetBlock {
                buffer = otherBuffer;
                if PageIsAllVisible(BufferGetPage(buffer)) {
                    visibilitymap_pin(rel, targetBlock, vmbuffer);
                }
                LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE as i32);
            } else if otherBlock < targetBlock {
                buffer = ReadBuffer(rel, targetBlock);
                if PageIsAllVisible(BufferGetPage(buffer)) {
                    visibilitymap_pin(rel, targetBlock, vmbuffer);
                }
                LockBuffer(otherBuffer, BUFFER_LOCK_EXCLUSIVE as i32);
                LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE as i32);
            } else {
                buffer = ReadBuffer(rel, targetBlock);
                if PageIsAllVisible(BufferGetPage(buffer)) {
                    visibilitymap_pin(rel, targetBlock, vmbuffer);
                }
                LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE as i32);
                LockBuffer(otherBuffer, BUFFER_LOCK_EXCLUSIVE as i32);
            }

            GetVisibilityMapPins(
                rel,
                buffer,
                otherBuffer,
                targetBlock,
                otherBlock,
                vmbuffer,
                vmbuffer_other,
            );
            page = BufferGetPage(buffer);
            if PageIsNew(page) {
                PageInit(page, BufferGetPageSize(buffer), 0);
                MarkBufferDirty(buffer);
            }

            pageFreeSpace = PageGetHeapFreeSpace(page);
            if targetFreeSpace <= pageFreeSpace {
                (*RelationGetSmgr(rel)).smgr_targblock = targetBlock;
                return buffer;
            }

            LockBuffer(buffer, BUFFER_LOCK_UNLOCK as i32);
            if otherBuffer == InvalidBuffer as i32 {
                ReleaseBuffer(buffer);
            } else if otherBlock != targetBlock {
                LockBuffer(otherBuffer, BUFFER_LOCK_UNLOCK as i32);
                ReleaseBuffer(buffer);
            }

            if !use_fsm {
                break;
            } else {
                targetBlock =
                    RecordAndGetPageWithFreeSpace(rel, targetBlock, pageFreeSpace, targetFreeSpace);
            }
        }

        // Have to extend relation
        let mut unlockedTargetBuffer = false;
        buffer = RelationAddBlocks(
            rel,
            state,
            num_pages,
            use_fsm,
            &raw mut unlockedTargetBuffer,
        );
        targetBlock = BufferGetBlockNumber(buffer);
        page = BufferGetPage(buffer);
        pageFreeSpace = PageGetHeapFreeSpace(page);

        if options & HEAP_INSERT_FROZEN as i32 != 0 {
            if !visibilitymap_pin_ok(targetBlock, *vmbuffer) {
                if !unlockedTargetBuffer {
                    LockBuffer(buffer, BUFFER_LOCK_UNLOCK as i32);
                }
                unlockedTargetBuffer = true;
                visibilitymap_pin(rel, targetBlock, vmbuffer);
            }
        }

        let mut recheckVmPins = false;
        if unlockedTargetBuffer {
            if otherBuffer != InvalidBuffer as i32 {
                LockBuffer(otherBuffer, BUFFER_LOCK_EXCLUSIVE as i32);
            }
            LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE as i32);
            recheckVmPins = true;
        } else if otherBuffer != InvalidBuffer as i32 {
            if !ConditionalLockBuffer(otherBuffer) {
                unlockedTargetBuffer = true;
                LockBuffer(buffer, BUFFER_LOCK_UNLOCK as i32);
                LockBuffer(otherBuffer, BUFFER_LOCK_EXCLUSIVE as i32);
                LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE as i32);
            }
            recheckVmPins = true;
        }

        if recheckVmPins {
            if GetVisibilityMapPins(
                rel,
                otherBuffer,
                buffer,
                otherBlock,
                targetBlock,
                vmbuffer_other,
                vmbuffer,
            ) {
                unlockedTargetBuffer = true;
            }
        }

        pageFreeSpace = PageGetHeapFreeSpace(page);

        // TODO: implement this, here must be either error or goto loop statement
        if len > pageFreeSpace {
            if unlockedTargetBuffer {
                if otherBuffer != InvalidBuffer as i32 {
                    LockBuffer(otherBuffer, BUFFER_LOCK_UNLOCK as i32);
                }
                UnlockReleaseBuffer(buffer);
                continue;
            }
            ereport!(
                PgLogLevel::PANIC,
                PgSqlErrorCode::ERRCODE_PROGRAM_LIMIT_EXCEEDED,
                "tuple is too big!"
            );
        }
        break;
    }

    (*RelationGetSmgr(rel)).smgr_targblock = targetBlock;
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
