use crate::include::relaion_macro::RelationUsesLocalBuffers;
use pg_sys::ForkNumber::*;
use pgrx::pg_sys::BufferAccessStrategyType::*;
use pgrx::pg_sys::ScanDirection::ForwardScanDirection;
use pgrx::pg_sys::ScanOptions::*;
use pgrx::pg_sys::{
    pgstat_assoc_relation, synchronize_seqscans, BlockNumber, FreeAccessStrategy,
    GetAccessStrategy, HeapScanDesc, HeapScanDescData, InvalidBlockNumber, InvalidBuffer,
    ItemPointerSetInvalid, NBuffers, ParallelBlockTableScanDesc, ReadStream, RelationData,
    RelationGetNumberOfBlocksInFork, ScanDirection, ScanKeyData,
};
use pgrx::prelude::*;
pub mod fetcher;
pub mod visibility;

#[pg_guard]
unsafe extern "C-unwind" fn heapgettup_initial_block(
    scan_ref: HeapScanDesc,
    dir: ScanDirection::Type,
) -> BlockNumber {
    let scan = *scan_ref;
    if scan.rs_nblocks == 0 || scan.rs_numblocks == 0 {
        return InvalidBlockNumber;
    }

    if dir == ForwardScanDirection {
        return scan.rs_startblock;
    }

    (*scan_ref).rs_base.rs_flags &= !SO_ALLOW_SYNC;
    if scan.rs_numblocks != InvalidBlockNumber {
        return (scan.rs_startblock + scan.rs_numblocks - 1) % scan.rs_nblocks;
    }

    if scan.rs_startblock > 0 {
        return scan.rs_startblock - 1;
    }

    scan.rs_nblocks - 1
}

#[pg_guard]
unsafe extern "C-unwind" fn heapgettup_advance_block(
    scan_ref: HeapScanDesc,
    block: BlockNumber,
    dir: ScanDirection::Type,
) -> BlockNumber {
    let mut block = block;
    let scan = *scan_ref;
    if dir == ForwardScanDirection {
        block += 1;
        if block >= scan.rs_nblocks {
            block = 0;
        }

        if scan.rs_base.rs_flags & SO_ALLOW_SYNC != 0 {
            todo!("")
        }

        if block == scan.rs_startblock {
            return InvalidBlockNumber;
        }

        if scan.rs_numblocks != InvalidBlockNumber {
            if (*scan_ref.sub(1)).rs_numblocks == 0 {
                return InvalidBlockNumber;
            }
        }

        return block;
    }

    if block == scan.rs_startblock {
        return InvalidBlockNumber;
    }

    if scan.rs_numblocks != InvalidBlockNumber {
        if (*scan_ref.sub(1)).rs_numblocks == 0 {
            return InvalidBlockNumber;
        }
    }

    if block == 0 {
        block = scan.rs_nblocks;
    }
    block -= 1;
    block
}

#[pg_guard]
pub unsafe extern "C-unwind" fn heap_scan_stream_read_next_serial(
    _stream: *mut ReadStream, // ? unused param even in postgres ?
    callback_private_data: *mut ::core::ffi::c_void,
    _per_buffer_data: *mut ::core::ffi::c_void, // ? unused param even in postgres ?
) -> BlockNumber {
    let scan = callback_private_data as HeapScanDesc;
    if !(*scan).rs_inited {
        (*scan).rs_prefetch_block = heapgettup_initial_block(scan, (*scan).rs_dir);
        (*scan).rs_inited = true;
    } else {
        (*scan).rs_prefetch_block =
            heapgettup_advance_block(scan, (*scan).rs_prefetch_block, (*scan).rs_dir)
    }
    (*scan).rs_prefetch_block
}

#[pg_guard]
unsafe extern "C-unwind" fn pgstat_should_count_relation(rel: *mut RelationData) -> bool {
    if !(*rel).pgstat_info.is_null() {
        return true;
    }
    if (*rel).pgstat_enabled {
        pgstat_assoc_relation(rel);
        true
    } else {
        false
    }
}

#[pg_guard]
#[inline]
pub unsafe extern "C-unwind" fn pgstat_count_heap_scan(rel: *mut RelationData) {
    if pgstat_should_count_relation(rel) {
        (*(*rel).pgstat_info).counts.numscans += 1;
    }
}

#[pg_guard]
#[inline]
pub unsafe extern "C-unwind" fn pgstat_count_heap_getnext(rel: *mut RelationData) {
    if pgstat_should_count_relation(rel) {
        (*(*rel).pgstat_info).counts.tuples_returned += 1;
    }
}

#[pg_guard]
pub unsafe extern "C-unwind" fn initscan(
    scan: *mut HeapScanDescData,
    key: *mut ScanKeyData,
    keep_start_block: bool,
) {
    // Determine the number of blocks we need to scan
    if (*scan).rs_base.rs_parallel.is_null() {
        (*scan).rs_nblocks = RelationGetNumberOfBlocksInFork((*scan).rs_base.rs_rd, MAIN_FORKNUM);
    } else {
        let bpscan = (*scan).rs_base.rs_parallel as ParallelBlockTableScanDesc;
        (*scan).rs_nblocks = (*bpscan).phs_nblocks;
    }

    let (allow_strat, allow_sync) = if !RelationUsesLocalBuffers!((*scan).rs_base.rs_rd)
        && (*scan).rs_nblocks > (NBuffers as u32 / 4)
    {
        (
            ((*scan).rs_base.rs_flags & SO_ALLOW_STRAT) != 0,
            ((*scan).rs_base.rs_flags & SO_ALLOW_SYNC) != 0,
        )
    } else {
        (false, false)
    };

    if allow_strat {
        if (*scan).rs_strategy.is_null() {
            (*scan).rs_strategy = GetAccessStrategy(BAS_BULKREAD);
        }
    } else {
        if !(*scan).rs_strategy.is_null() {
            FreeAccessStrategy((*scan).rs_strategy);
        }
        (*scan).rs_strategy = std::ptr::null_mut();
    }

    if !(*scan).rs_base.rs_parallel.is_null() {
        if (*(*scan).rs_base.rs_parallel).phs_syncscan {
            (*scan).rs_base.rs_flags |= SO_ALLOW_SYNC;
        } else {
            (*scan).rs_base.rs_flags &= !SO_ALLOW_SYNC;
        }
    } else if keep_start_block {
        if allow_sync && synchronize_seqscans {
            (*scan).rs_base.rs_flags |= SO_ALLOW_SYNC;
        } else {
            (*scan).rs_base.rs_flags &= !SO_ALLOW_SYNC;
        }
    }
    // else if allow_sync && synchronize_seqscans {

    // }
    else {
        (*scan).rs_base.rs_flags &= !SO_ALLOW_SYNC;
        (*scan).rs_startblock = 0;
    }

    (*scan).rs_numblocks = InvalidBlockNumber;
    (*scan).rs_inited = false;
    (*scan).rs_ctup.t_data = std::ptr::null_mut();
    ItemPointerSetInvalid(&raw mut (*scan).rs_ctup.t_self);
    (*scan).rs_cbuf = InvalidBuffer as i32;
    (*scan).rs_cblock = InvalidBlockNumber;
    (*scan).rs_ntuples = 0;
    (*scan).rs_cindex = 0;
    (*scan).rs_dir = ForwardScanDirection;
    (*scan).rs_prefetch_block = InvalidBlockNumber;

    if !key.is_null() && (*scan).rs_base.rs_nkeys > 0 {
        (*scan)
            .rs_base
            .rs_key
            .copy_from(key, (*scan).rs_base.rs_nkeys as usize);
    }

    if ((*scan).rs_base.rs_flags & SO_TYPE_SEQSCAN) != 0 {
        pgstat_count_heap_scan((*scan).rs_base.rs_rd);
    }
}
