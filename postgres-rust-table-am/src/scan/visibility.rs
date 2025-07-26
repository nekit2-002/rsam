use pg_sys::SnapshotType::*;
use pg_sys::{
    Buffer, BufferIsPermanent, HeapTuple, HeapTupleGetUpdateXid,
    HeapTupleHeaderGetCmax, HeapTupleHeaderGetCmin, HeapTupleHeaderGetRawXmin,
    HeapTupleHeaderXminInvalid, InvalidOid, ItemPointerIsValid, MarkBufferDirtyHint, Snapshot,
    TransactionIdDidCommit, TransactionIdFollowsOrEquals, TransactionIdGetCommitLSN,
    TransactionIdIsCurrentTransactionId, XidInMVCCSnapshot,
};
use pgrx::pg_sys::{
    BufferGetLSNAtomic, HistoricSnapshotGetTupleCids, InvalidCommandId,
    ResolveCminCmaxDuringDecoding, TransactionId, TransactionIdPrecedes, XLogNeedsFlush,
    HEAP_LOCK_MASK, HEAP_MOVED_IN, HEAP_MOVED_OFF, HEAP_XMAX_COMMITTED, HEAP_XMAX_EXCL_LOCK,
    HEAP_XMAX_INVALID, HEAP_XMAX_IS_MULTI, HEAP_XMAX_LOCK_ONLY, HEAP_XMIN_COMMITTED,
    HEAP_XMIN_FROZEN, HEAP_XMIN_INVALID,
};
use pgrx::{pg_sys::HeapTupleHeaderData, prelude::*};
use std::slice::from_raw_parts_mut;
use crate::include::general::Assert;

#[pg_guard]
#[allow(non_snake_case)]
unsafe extern "C-unwind" fn HeapTupleHeaderXminCommited(tup: *const HeapTupleHeaderData) -> bool {
    ((*tup).t_infomask & HEAP_XMIN_COMMITTED as u16) != 0
}

#[pg_guard]
#[allow(non_snake_case)]
unsafe extern "C-unwind" fn HeapTupleHeaderXminFrozen(tup: *const HeapTupleHeaderData) -> bool {
    ((*tup).t_infomask & HEAP_XMIN_FROZEN as u16) == HEAP_XMIN_FROZEN as u16
}

#[pg_guard]
#[allow(non_snake_case)]
pub unsafe extern "C-unwind" fn xmax_is_locked_only(infomask: u16) -> bool {
    (infomask & HEAP_XMAX_LOCK_ONLY as u16 != 0)
        || (infomask & (HEAP_LOCK_MASK | HEAP_XMAX_IS_MULTI) as u16 == HEAP_XMAX_EXCL_LOCK as u16)
}

#[allow(non_snake_case)]
unsafe extern "C-unwind" fn TransactionIdInArray(
    xid: TransactionId,
    xip: *mut TransactionId,
    num: u32,
) -> bool {
    if num <= 0 {
        return false;
    }
    let xip = from_raw_parts_mut(xip, num as usize);
    xip.binary_search(&xid).is_ok()
}

#[pg_guard]
#[allow(non_snake_case)]
unsafe extern "C-unwind" fn SetHintBits(
    tuple: *mut HeapTupleHeaderData,
    buffer: Buffer,
    infomask: u16,
    xid: TransactionId,
) {
    if xid != 0.into() {
        let commit_lsn = TransactionIdGetCommitLSN(xid);
        if BufferIsPermanent(buffer)
            && XLogNeedsFlush(commit_lsn)
            && BufferGetLSNAtomic(buffer) < commit_lsn
        {
            return;
        }
    }

    (*tuple).t_infomask |= infomask;
    MarkBufferDirtyHint(buffer, true);
}

#[pg_guard]
pub unsafe extern "C-unwind" fn tuple_satisfies_mvcc(
    htup: HeapTuple,
    snapshot: Snapshot,
    buffer: Buffer,
) -> bool {
    let tuple = (*htup).t_data;
    Assert((*snapshot).regd_count > 0 || (*snapshot).active_count > 0);
    Assert(ItemPointerIsValid(&raw const (*htup).t_self));
    Assert((*htup).t_tableOid != InvalidOid);

    if !HeapTupleHeaderXminCommited(tuple) {
        if HeapTupleHeaderXminInvalid(tuple) {
            return false;
        }

        if (*tuple).t_infomask & HEAP_MOVED_OFF as u16 != 0 {
        } else if (*tuple).t_infomask & HEAP_MOVED_IN as u16 != 0 {
        } else if TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmin(tuple)) {
            if HeapTupleHeaderGetCmin(tuple) >= (*snapshot).curcid {
                return false;
            }

            if (*tuple).t_infomask & HEAP_XMAX_INVALID as u16 != 0 {
                return true;
            }

            if xmax_is_locked_only((*tuple).t_infomask) {
                return true;
            }

            if (*tuple).t_infomask & HEAP_XMAX_IS_MULTI as u16 != 0 {
                let xmax = HeapTupleGetUpdateXid(tuple);
                Assert(xmax != 0.into());
                if !TransactionIdIsCurrentTransactionId(xmax) {
                    return true;
                } else if HeapTupleHeaderGetCmax(tuple) >= (*snapshot).curcid {
                    return true;
                } else {
                    return false;
                };
            }

            if !TransactionIdIsCurrentTransactionId((*tuple).t_choice.t_heap.t_xmax) {
                SetHintBits(tuple, buffer, HEAP_XMAX_INVALID as u16, 0.into());
                return true;
            }

            if HeapTupleHeaderGetCmax(tuple) >= (*snapshot).curcid {
                return true;
            } else {
                return false;
            };
        } else if XidInMVCCSnapshot(HeapTupleHeaderGetRawXmin(tuple), snapshot) {
            return false;
        } else if TransactionIdDidCommit(HeapTupleHeaderGetRawXmin(tuple)) {
            SetHintBits(
                tuple,
                buffer,
                HEAP_XMIN_COMMITTED as u16,
                HeapTupleHeaderGetRawXmin(tuple),
            );
        } else {
            SetHintBits(tuple, buffer, HEAP_XMIN_INVALID as u16, 0.into());
            return false;
        }
    } else {
        if !HeapTupleHeaderXminFrozen(tuple)
            && XidInMVCCSnapshot(HeapTupleHeaderGetRawXmin(tuple), snapshot)
        {
            return false;
        }
    }

    if ((*tuple).t_infomask & HEAP_XMAX_INVALID as u16) != 0 {
        return true;
    }

    if xmax_is_locked_only((*tuple).t_infomask) {
        return true;
    }

    if (*tuple).t_infomask & HEAP_XMAX_IS_MULTI as u16 != 0 {
        Assert(!xmax_is_locked_only((*tuple).t_infomask));
        let xmax = HeapTupleGetUpdateXid(tuple);
        Assert(xmax != 0.into());

        if TransactionIdIsCurrentTransactionId(xmax) {
            if HeapTupleHeaderGetCmax(tuple) >= (*snapshot).curcid {
                return true;
            } else {
                return false;
            };
        }

        if XidInMVCCSnapshot(xmax, snapshot) {
            return true;
        }

        if TransactionIdDidCommit(xmax) {
            return false;
        }
        return true;
    }

    if (*tuple).t_infomask & HEAP_XMAX_COMMITTED as u16 == 0 {
        if TransactionIdIsCurrentTransactionId((*tuple).t_choice.t_heap.t_xmax) {
            if HeapTupleHeaderGetCmax(tuple) >= (*snapshot).curcid {
                return true;
            } else {
                return false;
            };
        }

        if XidInMVCCSnapshot((*tuple).t_choice.t_heap.t_xmax, snapshot) {
            return true;
        }

        if TransactionIdDidCommit((*tuple).t_choice.t_heap.t_xmax) {
            return false;
        }

        SetHintBits(
            tuple,
            buffer,
            HEAP_XMAX_COMMITTED as u16,
            (*tuple).t_choice.t_heap.t_xmax,
        );
    } else {
        if XidInMVCCSnapshot((*tuple).t_choice.t_heap.t_xmax, snapshot) {
            return true;
        }
    }

    false
}

pub unsafe extern "C-unwind" fn tuple_satisfies_historic_mvcc(
    htup: HeapTuple,
    snapshot: Snapshot,
    buffer: Buffer,
) -> bool {
    let tuple = (*htup).t_data;
    let xmin = (*tuple).t_choice.t_heap.t_xmin;
    let mut xmax = (*tuple).t_choice.t_heap.t_xmax;
    Assert(ItemPointerIsValid(&raw mut (*htup).t_self));
    Assert((*htup).t_tableOid != InvalidOid);

    if HeapTupleHeaderXminInvalid(tuple) {
        Assert(!TransactionIdDidCommit(xmin));
        return false;
    } else if TransactionIdInArray(xmin, (*snapshot).subxip, (*snapshot).subxcnt as u32) {
        let mut cmin = (*tuple).t_choice.t_heap.t_field3.t_cid;
        let mut cmax = InvalidCommandId;

        let resolved = ResolveCminCmaxDuringDecoding(
            HistoricSnapshotGetTupleCids(),
            snapshot,
            htup,
            buffer,
            &raw mut cmin,
            &raw mut cmax,
        );

        if !resolved {
            return false;
        }

        Assert(cmin != InvalidCommandId);

        if cmin >= (*snapshot).curcid {
            return false;
        }
    } else if TransactionIdPrecedes(xmin, (*snapshot).xmin) {
        Assert(!(HeapTupleHeaderXminCommited(tuple) && !TransactionIdDidCommit(xmin)));
        if !HeapTupleHeaderXminCommited(tuple) && !TransactionIdDidCommit(xmin) {
            return false;
        }
    } else if TransactionIdFollowsOrEquals(xmin, (*snapshot).xmax) {
        return false;
    } else if TransactionIdInArray(xmin, (*snapshot).xip, (*snapshot).xcnt) {
    } else {
        return false;
    }

    // xmin is visible, check xmax

    if (*tuple).t_infomask & HEAP_XMAX_INVALID as u16 != 0 {
        return true;
    } else if xmax_is_locked_only((*tuple).t_infomask) {
        return true;
    } else if (*tuple).t_infomask & HEAP_XMAX_IS_MULTI as u16 != 0 {
        xmax = HeapTupleGetUpdateXid(tuple);
    }

    if TransactionIdInArray(xmax, (*snapshot).subxip, (*snapshot).subxcnt as u32) {
        let mut cmax = (*tuple).t_choice.t_heap.t_field3.t_cid;
        let mut cmin = 0;
        let resolved = ResolveCminCmaxDuringDecoding(
            HistoricSnapshotGetTupleCids(),
            snapshot,
            htup,
            buffer,
            &raw mut cmin,
            &raw mut cmax,
        );

        if !resolved || cmax == InvalidCommandId {
            return true;
        }

        if cmax >= (*snapshot).curcid {
            return true;
        } else {
            return false;
        }
    } else if TransactionIdPrecedes(xmax, (*snapshot).xmin) {
        Assert(
            !((*tuple).t_infomask & HEAP_XMAX_COMMITTED as u16 != 0
                && TransactionIdDidCommit(xmax)),
        );

        if (*tuple).t_infomask & HEAP_XMAX_COMMITTED as u16 != 0 {
            return false;
        }

        return !TransactionIdDidCommit(xmax);
    } else if TransactionIdFollowsOrEquals(xmax, (*snapshot).xmax) {
        return true;
    } else if TransactionIdInArray(xmax, (*snapshot).xip, (*snapshot).xcnt) {
        return false;
    } else {
        return true;
    }
}

#[pg_guard]
pub unsafe extern "C-unwind" fn tuple_satisfies_visibility(
    htup: HeapTuple,
    snapshot: Snapshot,
    buffer: Buffer,
) -> bool {
    match (*snapshot).snapshot_type {
        SNAPSHOT_MVCC => tuple_satisfies_mvcc(htup, snapshot, buffer),
        SNAPSHOT_HISTORIC_MVCC => tuple_satisfies_historic_mvcc(htup, snapshot, buffer),
        _ => false,
    }
}
