use pgrx::pg_sys::ScanDirection::ForwardScanDirection;
use pgrx::pg_sys::ScanOptions::SO_ALLOW_SYNC;
use pgrx::pg_sys::{
    pgstat_assoc_relation, BlockNumber, HeapScanDesc, InvalidBlockNumber, ReadStream, RelationData,
    ScanDirection,
};
use pgrx::prelude::*;
pub mod fetcher;

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
pub unsafe extern "C-unwind" fn pgstat_count_heap_scan(rel: *mut RelationData) {
    if pgstat_should_count_relation(rel) {
        (*(*rel).pgstat_info).counts.numscans += 1;
    }
}
