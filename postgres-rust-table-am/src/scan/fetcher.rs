use pg_sys::{
    check_for_interrupts, heap_getattr, heap_prepare_pagescan, read_stream_next_buffer,
    read_stream_reset, BufferGetBlockNumber, BufferGetPage, BufferIsValid, DatumGetBool,
    FirstOffsetNumber, FunctionCall2Coll, HeapScanDesc, HeapScanDescData, HeapTuple,
    InvalidBlockNumber, InvalidBuffer, ItemId, ItemPointerSet, ItemPointerSetBlockNumber,
    ItemPointerSetOffsetNumber, LockBuffer, OffsetNumber, Page, PageGetItem, PageGetItemId,
    PageGetMaxOffsetNumber, ReleaseBuffer, ScanKeyData, TupleDesc, BUFFER_LOCK_SHARE,
    BUFFER_LOCK_UNLOCK, SK_ISNULL, HeapTupleSatisfiesVisibility
};

use crate::include::general::*;
use crate::include::relaion_macro::RelationGetDescr;
use pg_sys::ScanDirection::*;
use pgrx::{pg_sys::ScanDirection, prelude::*};
use std::cmp::min;
use crate::scan::visibility::*;

#[macro_export]
macro_rules! partial_loop {
    ($before_label:block, $from_label:block, $exec_from_label:expr) => {
      loop {
        if $exec_from_label {
            $from_label
            $exec_from_label = false;
        } else {
            $before_label
            $from_label
        }
      }
    };
}

#[macro_export]
macro_rules! OffsetNumberNext {
    ($offn:expr) => {
        1 + $offn
    };
}

#[macro_export]
macro_rules! OffsetNumberPrev {
    ($offn:expr) => {
        $offn - 1
    };
}

#[pg_guard]
unsafe extern "C-unwind" fn heap_fetch_next_buffer(scan: *mut HeapScanDescData, scandir: i32) {
    Assert(!(*scan).rs_read_stream.is_null());
    if BufferIsValid((*scan).rs_cbuf) {
        ReleaseBuffer((*scan).rs_cbuf);
        (*scan).rs_cbuf = InvalidBuffer as i32;
    }

    check_for_interrupts!();
    if (*scan).rs_dir != scandir {
        (*scan).rs_prefetch_block = (*scan).rs_cblock;
        read_stream_reset((*scan).rs_read_stream);
    }

    (*scan).rs_dir = scandir;
    (*scan).rs_cbuf = read_stream_next_buffer((*scan).rs_read_stream, std::ptr::null_mut());
    if BufferIsValid((*scan).rs_cbuf) {
        (*scan).rs_cblock = BufferGetBlockNumber((*scan).rs_cbuf)
    }
}
#[pg_guard]
unsafe extern "C-unwind" fn heap_key_test(
    tuple: HeapTuple,
    desc: TupleDesc,
    nkeys: i32,
    keys: *mut ScanKeyData,
) -> bool {
    let mut cur_nkeys = nkeys - 1;
    let mut cur_key = keys;
    while cur_nkeys >= 0 {
        if ((*cur_key).sk_flags & SK_ISNULL as i32) != 0 {
            return false;
        }
        let mut isnull = false;
        let atp = heap_getattr(tuple, (*cur_key).sk_attno.into(), desc, &mut isnull);
        if isnull {
            return false;
        }

        let test = FunctionCall2Coll(
            &raw mut (*cur_key).sk_func,
            (*cur_key).sk_collation,
            atp,
            (*cur_key).sk_argument,
        );

        if DatumGetBool(test) {
            return false;
        }

        cur_nkeys -= 1;
        cur_key = cur_key.add(1);
    }

    true
}

#[pg_guard]
pub unsafe extern "C-unwind" fn heap_gettup_pagemode(
    scan: HeapScanDesc,
    dir: i32,
    nkeys: i32,
    key: *mut ScanKeyData,
) {
    let tuple = &raw mut (*scan).rs_ctup;
    let mut page: Page = std::ptr::null_mut();
    let mut line_index = 0;
    let mut lines_left = 0;
    let mut continue_page = false;
    if (*scan).rs_inited {
        page = BufferGetPage((*scan).rs_cbuf);
        line_index = (*scan).rs_cindex + dir;
        lines_left = if dir == 1 {
            (*scan).rs_ntuples - line_index
        } else {
            (*scan).rs_cindex
        };
        continue_page = true;
    }

    partial_loop!(
        {
            heap_fetch_next_buffer(scan, dir);
            if !BufferIsValid((*scan).rs_cbuf) {
                break;
            }

            Assert(BufferGetBlockNumber((*scan).rs_cbuf) == (*scan).rs_cblock);

            heap_prepare_pagescan(scan.cast());
            page = BufferGetPage((*scan).rs_cbuf);
            lines_left = (*scan).rs_ntuples;
            line_index = if dir == 1 { 0 } else { lines_left - 1 };
            ItemPointerSetBlockNumber(&raw mut (*tuple).t_self, (*scan).rs_cblock);
        },
        {
            while lines_left > 0 {
                Assert(line_index <= (*scan).rs_ntuples);
                let line_offset = (*scan).rs_vistuples[line_index as usize];
                let lpp: ItemId = PageGetItemId(page, line_offset);
                Assert(ItemIdIsNormal!(lpp));

                (*tuple).t_data = PageGetItem(page, lpp).cast();
                (*tuple).t_len = (*lpp).lp_len();
                ItemPointerSetOffsetNumber(&raw mut (*tuple).t_self, line_offset);

                if !key.is_null()
                    && !heap_key_test(tuple, RelationGetDescr!((*scan).rs_base.rs_rd), nkeys, key)
                {
                    lines_left -= 1;
                    line_index += dir;
                    continue;
                }
                (*scan).rs_cindex = line_index;
                return;
            }
        },
        continue_page
    );

    if BufferIsValid((*scan).rs_cbuf) {
        ReleaseBuffer((*scan).rs_cbuf)
    }

    // end of scan
    (*scan).rs_cbuf = InvalidBuffer as i32;
    (*scan).rs_cblock = InvalidBlockNumber;
    (*scan).rs_prefetch_block = InvalidBlockNumber;
    (*tuple).t_data = std::ptr::null_mut();
    (*scan).rs_inited = false;
}

#[pg_guard]
// ! this function has bug
unsafe extern "C-unwind" fn heap_gettup_continue_page(
    scan: HeapScanDesc,
    scandir: ScanDirection::Type,
    lines_left: *mut i32,
    line_offset: *mut OffsetNumber,
) -> Page {
    Assert((*scan).rs_inited);
    Assert(BufferIsValid((*scan).rs_cbuf));

    let page = BufferGetPage((*scan).rs_cbuf);
    if scandir == ForwardScanDirection {
        *line_offset = OffsetNumberNext!((*scan).rs_coffset);
        *lines_left = PageGetMaxOffsetNumber(page) as i32 - (*line_offset as i32) + 1;
    } else {
        *line_offset = min(
            PageGetMaxOffsetNumber(page),
            OffsetNumberPrev!((*scan).rs_coffset),
        );
        *lines_left = *line_offset as i32;
    }
    page
}

#[pg_guard]
unsafe extern "C-unwind" fn heap_gettup_start_page(
    scan: HeapScanDesc,
    scandir: i32,
    lines_left: *mut i32,
    line_offset: *mut OffsetNumber,
) -> Page {
    Assert((*scan).rs_inited);
    Assert(BufferIsValid((*scan).rs_cbuf));

    let page = BufferGetPage((*scan).rs_cbuf);
    *lines_left = (PageGetMaxOffsetNumber(page) - FirstOffsetNumber + 1) as i32;
    *line_offset = if scandir == ForwardScanDirection {
        FirstOffsetNumber
    } else {
        (*lines_left) as OffsetNumber
    };
    page
}

#[pg_guard]
pub unsafe extern "C-unwind" fn heap_gettup(
    scan: HeapScanDesc,
    dir: i32,
    nkeys: i32,
    key: *mut ScanKeyData,
) {
    let tuple = &raw mut (*scan).rs_ctup;
    let mut page: Page = std::ptr::null_mut();
    let mut lines_left = 0;
    let mut line_offset: OffsetNumber = 0;
    let mut continue_page = false;
    if (*scan).rs_inited {
        LockBuffer((*scan).rs_cbuf, BUFFER_LOCK_SHARE as i32);
        page = heap_gettup_continue_page(scan, dir, &raw mut lines_left, &raw mut line_offset);
        continue_page = true;
    }

    partial_loop!(
        {
            heap_fetch_next_buffer(scan, dir);
            if !BufferIsValid((*scan).rs_cbuf) {
                break;
            }

            Assert(BufferGetBlockNumber((*scan).rs_cbuf) == (*scan).rs_cblock);

            LockBuffer((*scan).rs_cbuf, BUFFER_LOCK_SHARE as i32);
            page = heap_gettup_start_page(scan, dir, &raw mut lines_left, &raw mut line_offset)
        },
        {
            while lines_left > 0 {
                let lpp = PageGetItemId(page, line_offset);
                if !ItemIdIsNormal!(lpp) {
                    lines_left -= 1;
                    line_offset += dir as u16;
                    continue;
                }

                (*tuple).t_data = PageGetItem(page, lpp).cast();
                (*tuple).t_len = (*lpp).lp_len();
                ItemPointerSet(&raw mut (*tuple).t_self, (*scan).rs_cblock, line_offset);

                let visible = tuple_satisfies_visibility(
                // let visible = HeapTupleSatisfiesVisibility(
                    tuple,
                    (*scan).rs_base.rs_snapshot,
                    (*scan).rs_cbuf,
                );

                if !visible {
                    lines_left -= 1;
                    line_offset += dir as u16;
                    continue;
                }

                if !key.is_null()
                    && !heap_key_test(tuple, RelationGetDescr!((*scan).rs_base.rs_rd), nkeys, key)
                {
                    lines_left -= 1;
                    line_offset += dir as u16;
                    continue;
                }

                LockBuffer((*scan).rs_cbuf, BUFFER_LOCK_UNLOCK as i32);
                (*scan).rs_coffset = line_offset;
                return;
            }

            LockBuffer((*scan).rs_cbuf, BUFFER_LOCK_UNLOCK as i32);
        },
        continue_page
    );

    if BufferIsValid((*scan).rs_cbuf) {
        ReleaseBuffer((*scan).rs_cbuf)
    }

    // end of scan
    (*scan).rs_cbuf = InvalidBuffer as i32;
    (*scan).rs_cblock = InvalidBlockNumber;
    (*scan).rs_prefetch_block = InvalidBlockNumber;
    (*tuple).t_data = std::ptr::null_mut();
    (*scan).rs_inited = false;
}
