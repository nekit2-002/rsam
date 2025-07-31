// import types
use pgrx::pg_sys::{
    Buffer, CommandId, Datum, HeapTupleData, HeapTupleHeaderData, ItemPointer, LockTupleMode,
    LockWaitPolicy, MultiXactId, MultiXactMember, MultiXactStatus, Page, PageHeader, Relation,
    TransactionId, LOCKMODE,
};

//import constants
use pg_sys::IndexAttrBitmapKind::INDEX_ATTR_BITMAP_IDENTITY_KEY;
use pg_sys::MultiXactStatus::*;
use pgrx::pg_sys::LockTupleMode::*;
use pgrx::pg_sys::LockWaitPolicy::*;
use pgrx::pg_sys::{
    AccessExclusiveLock, AccessShareLock, ExclusiveLock, FirstLowInvalidHeapAttributeNumber,
    InvalidTransactionId, MaxHeapAttributeNumber, RowShareLock, HEAP_COMBOCID, HEAP_KEYS_UPDATED,
    HEAP_LOCK_MASK, HEAP_MOVED, HEAP_XMAX_COMMITTED, HEAP_XMAX_INVALID, HEAP_XMAX_IS_MULTI,
    HEAP_XMAX_KEYSHR_LOCK, HEAP_XMAX_LOCK_ONLY, HEAP_XMAX_SHR_LOCK, REPLICA_IDENTITY_FULL,
    REPLICA_IDENTITY_NOTHING,
};

// import functions
use pgrx::pg_sys::{
    bms_free, bms_is_member, heap_deform_tuple, heap_form_tuple, pfree, ConditionalLockTuple,
    GetMultiXactIdMembers, HeapTupleGetUpdateXid, HeapTupleSetHintBits, LockTuple,
    MultiXactIdCreate, MultiXactIdExpand, MultiXactIdIsRunning, RelationGetIndexAttrBitmap,
    TransactionIdDidCommit, TransactionIdIsCurrentTransactionId, TransactionIdIsInProgress,
    TransactionIdIsNormal, TransactionIdPrecedes,
};
use pgrx::prelude::*;

use crate::include::general::Assert;
use crate::scan::visibility::xmax_is_locked_only;
use crate::{RelationGetDescr, RelationIsLogicallyLogged};

pub struct Info(pub LOCKMODE, pub i32, pub i32);
#[allow(non_upper_case_globals)]
pub const tupleLockExtraInfo: [Info; (LockTupleExclusive + 1) as usize] = [
    Info(
        AccessShareLock as i32,
        MultiXactStatusForKeyShare as i32,
        -1,
    ),
    Info(RowShareLock as i32, MultiXactStatusForShare as i32, -1),
    Info(
        ExclusiveLock as i32,
        MultiXactStatusForNoKeyUpdate as i32,
        MultiXactStatusNoKeyUpdate as i32,
    ),
    Info(
        AccessExclusiveLock as i32,
        MultiXactStatusForUpdate as i32,
        MultiXactStatusUpdate as i32,
    ),
];

#[allow(non_upper_case_globals)]
const MultiXactStatusLock: [LockTupleMode::Type; (MultiXactStatusUpdate + 1) as usize] = [
    LockTupleKeyShare,
    LockTupleShare,
    LockTupleNoKeyExclusive,
    LockTupleExclusive,
    LockTupleNoKeyExclusive,
    LockTupleExclusive,
];
const HEAP_XMAX_EXCL_LOCK: u16 = 0x0040;

#[inline]
#[allow(non_snake_case)]
pub fn HEAP_LOCKED_UPGRADED(mask: u16) -> bool {
    (mask & HEAP_XMAX_IS_MULTI as u16) != 0
        && (mask & HEAP_XMAX_LOCK_ONLY as u16) != 0
        && (mask & (HEAP_XMAX_EXCL_LOCK | HEAP_XMAX_KEYSHR_LOCK as u16)) == 0
}

#[inline]
#[allow(non_snake_case)]
pub fn HEAP_XMAX_IS_KEYSHR_LOCKED(mask: u16) -> bool {
    (mask & HEAP_LOCK_MASK as u16) == HEAP_XMAX_KEYSHR_LOCK as u16
}

#[inline]
#[allow(non_snake_case)]
pub fn HEAP_XMAX_IS_SHR_LOCKED(mask: u16) -> bool {
    (mask & HEAP_LOCK_MASK as u16) == HEAP_XMAX_SHR_LOCK as u16
}

#[inline]
#[allow(non_snake_case)]
pub fn HEAP_XMAX_IS_EXCL_LOCKED(mask: u16) -> bool {
    (mask & HEAP_LOCK_MASK as u16) == HEAP_XMAX_EXCL_LOCK
}

#[pg_guard]
#[allow(non_upper_case_globals)]
pub unsafe extern "C-unwind" fn aquire_tuplock(
    rel: Relation,
    tid: ItemPointer,
    mode: LockTupleMode::Type,
    wait_policy: LockWaitPolicy::Type,
    have_tuple_lock: *mut bool,
) -> bool {
    if *have_tuple_lock {
        return true;
    }

    match wait_policy {
        LockWaitBlock => LockTuple(rel, tid, tupleLockExtraInfo[mode as usize].0),
        LockWaitSkip => {
            if !ConditionalLockTuple(rel, tid, mode as i32) {
                return false;
            }
        }
        LockWaitError => {
            if !ConditionalLockTuple(rel, tid, mode as i32) {
                ereport!(
                    PgLogLevel::ERROR,
                    PgSqlErrorCode::ERRCODE_LOCK_NOT_AVAILABLE,
                    "could not obtain lock on row in relation"
                );
            }
        }
        _ => {}
    }

    *have_tuple_lock = true;
    true
}

#[pg_guard]
pub unsafe extern "C-unwind" fn xmax_infomask_changed(new_mask: u16, old_mask: u16) -> bool {
    let interesting = (HEAP_XMAX_IS_MULTI | HEAP_XMAX_LOCK_ONLY | HEAP_LOCK_MASK) as u16;
    if new_mask & interesting != old_mask & interesting {
        return true;
    }
    false
}

#[pg_guard]
#[allow(non_snake_case)]
pub unsafe extern "C-unwind" fn UpdateXmaxHintBits(
    tuple: *mut HeapTupleHeaderData,
    buffer: Buffer,
    xid: TransactionId,
) {
    Assert((*tuple).t_choice.t_heap.t_xmax == xid);
    Assert((*tuple).t_infomask & HEAP_XMAX_IS_MULTI as u16 == 0);

    if (*tuple).t_infomask & (HEAP_XMAX_COMMITTED | HEAP_XMAX_INVALID) as u16 == 0 {
        if !xmax_is_locked_only((*tuple).t_infomask) && TransactionIdDidCommit(xid) {
            HeapTupleSetHintBits(tuple, buffer, HEAP_XMAX_COMMITTED as u16, xid);
        } else {
            HeapTupleSetHintBits(
                tuple,
                buffer,
                HEAP_XMAX_INVALID as u16,
                InvalidTransactionId,
            );
        }
    }
}

#[pg_guard]
pub unsafe extern "C-unwind" fn tuple_header_get_update_xid(
    tup: *mut HeapTupleHeaderData,
) -> TransactionId {
    let mask = (*tup).t_infomask;
    return if ((*tup).t_infomask & HEAP_XMAX_INVALID as u16) == 0
        && mask & HEAP_XMAX_IS_MULTI as u16 != 0
        && mask & HEAP_XMAX_LOCK_ONLY as u16 == 0
    {
        HeapTupleGetUpdateXid(tup)
    } else {
        (*tup).t_choice.t_heap.t_xmax
    };
}

#[pg_guard]
#[allow(non_snake_case)]
pub unsafe extern "C-unwind" fn MultiXactIdGetUpdateXid(
    xmax: TransactionId,
    t_infomask: u16,
) -> TransactionId {
    let mut update_xact = InvalidTransactionId;
    Assert(t_infomask & HEAP_XMAX_LOCK_ONLY as u16 == 0);
    Assert(t_infomask & HEAP_XMAX_IS_MULTI as u16 != 0);

    let mut members: *mut MultiXactMember = std::ptr::null_mut();
    let nmembers = GetMultiXactIdMembers(xmax, &raw mut members, false, false);
    if nmembers > 0 {
        // Here, as nmembers > 0, members is a valid C array and conversion nmembers to usize is safe
        let members_slice = std::slice::from_raw_parts_mut(members, nmembers as usize);
        for m in members_slice {
            if m.status <= MultiXactStatusForUpdate {
                continue;
            }

            Assert(update_xact == InvalidTransactionId);
            update_xact = m.xid;
        }

        pfree(members.cast());
    }

    update_xact
}

#[pg_guard]
pub unsafe extern "C-unwind" fn get_mxact_status_for_lock(
    mode: LockTupleMode::Type,
    is_update: bool,
) -> MultiXactStatus::Type {
    let mode = mode as usize;

    let res = if is_update {
        tupleLockExtraInfo[mode].2
    } else {
        tupleLockExtraInfo[mode].1
    };

    if res == -1 {
        ereport!(
            PgLogLevel::ERROR,
            PgSqlErrorCode::ERRCODE_ASSERT_FAILURE,
            "invalid lock tuple mode"
        )
    }

    (res as u32).into()
}

#[pg_guard]
pub unsafe extern "C-unwind" fn extract_replica_identity(
    rel: Relation,
    tp: *mut HeapTupleData,
    key_required: bool,
    copy: *mut bool,
) -> *mut HeapTupleData {
    let desc = RelationGetDescr!(rel);
    let replident = (*(*rel).rd_rel).relreplident;
    let mut nulls: [bool; MaxHeapAttributeNumber as usize] =
        [false; MaxHeapAttributeNumber as usize];
    let mut values: [Datum; MaxHeapAttributeNumber as usize] =
        [Datum::null(); MaxHeapAttributeNumber as usize];

    *copy = false;
    if !RelationIsLogicallyLogged!(rel) {
        return std::ptr::null_mut();
    }

    if replident == REPLICA_IDENTITY_NOTHING as i8 {
        return std::ptr::null_mut();
    }

    if replident == REPLICA_IDENTITY_FULL as i8 {
        // TODO: toast case
        // if HeapHasExternal(tp) {}
        return tp;
    }

    if !key_required {
        return std::ptr::null_mut();
    }

    let id_attrs = RelationGetIndexAttrBitmap(rel, INDEX_ATTR_BITMAP_IDENTITY_KEY);
    if id_attrs.is_null() {
        return std::ptr::null_mut();
    }

    heap_deform_tuple(tp, desc, values.as_mut_ptr(), nulls.as_mut_ptr());
    for i in 0..(*desc).natts {
        if bms_is_member(i + 1 - FirstLowInvalidHeapAttributeNumber, id_attrs) {
            Assert(!nulls[i as usize]);
        } else {
            nulls[i as usize] = true;
        }
    }

    let key_tuple = heap_form_tuple(desc, values.as_ptr(), nulls.as_ptr());
    *copy = true;

    bms_free(id_attrs);
    // todo: here should be toast stuff
    // if HeapHasExternal(tp) {}
    key_tuple
}

#[pg_guard]
#[allow(non_upper_case_globals)]
pub unsafe extern "C-unwind" fn get_multi_xact_id_hint_bits(
    multi: MultiXactId,
    new_mask: *mut u16,
    new_mask2: *mut u16,
) {
    let mut bits = HEAP_XMAX_IS_MULTI as u16;
    let mut bits2 = 0u16;
    let mut has_update = false;
    let mut strongest: LockTupleMode::Type = LockTupleKeyShare;
    let mut members: *mut MultiXactMember = std::ptr::null_mut();
    let nmembers = GetMultiXactIdMembers(multi, &raw mut members, false, false);

    if nmembers > 0 {
        let members_slice = std::slice::from_raw_parts_mut(members, nmembers as usize);
        for m in members_slice {
            let mode = MultiXactStatusLock[m.status as usize];
            if mode > strongest {
                strongest = mode;
            }

            match m.status {
                MultiXactStatusForKeyShare => {}
                MultiXactStatusForShare => {}
                MultiXactStatusForNoKeyUpdate => {}
                MultiXactStatusForUpdate => {
                    bits2 |= HEAP_KEYS_UPDATED as u16;
                }
                MultiXactStatusNoKeyUpdate => {
                    has_update = true;
                }
                MultiXactStatusUpdate => {
                    bits2 |= HEAP_KEYS_UPDATED as u16;
                    has_update = true;
                }
                _ => {}
            }
        }
        pfree(members.cast());
    }

    if strongest == LockTupleExclusive || strongest == LockTupleNoKeyExclusive {
        bits |= HEAP_XMAX_EXCL_LOCK;
    } else if strongest == LockTupleShare {
        bits |= HEAP_XMAX_SHR_LOCK as u16;
    } else if strongest == LockTupleKeyShare {
        bits |= HEAP_XMAX_KEYSHR_LOCK as u16;
    }

    if !has_update {
        bits |= HEAP_XMAX_LOCK_ONLY as u16;
    }

    *new_mask = bits;
    *new_mask2 = bits2;
}

#[pg_guard]
#[allow(non_upper_case_globals)]
pub unsafe extern "C-unwind" fn compute_new_xmax_infomask(
    xmax: TransactionId,
    old_infomask: u16,
    old_infomask2: u16,
    add_to_xmax: TransactionId,
    mode: LockTupleMode::Type,
    is_update: bool,
    result_xmax: *mut TransactionId,
    result_infomask: *mut u16,
    result_infomask2: *mut u16,
) {
    let mut mode = mode;
    let mut old_infomask = old_infomask;

    Assert(TransactionIdIsCurrentTransactionId(add_to_xmax));
    let (new_xmax, new_infomask, new_infomask2) = loop {
        let mut new_infomask = 0;
        let mut new_infomask2 = 0;

        let new_xmax = if old_infomask & HEAP_XMAX_INVALID as u16 != 0 {
            if is_update {
                if mode == LockTupleExclusive {
                    new_infomask2 |= HEAP_KEYS_UPDATED as u16;
                }
            } else {
                new_infomask |= HEAP_XMAX_LOCK_ONLY as u16;
                new_infomask |= match mode {
                    LockTupleKeyShare => HEAP_XMAX_KEYSHR_LOCK as u16,
                    LockTupleShare => HEAP_XMAX_SHR_LOCK as u16,
                    LockTupleNoKeyExclusive => HEAP_XMAX_EXCL_LOCK,
                    LockTupleExclusive => {
                        new_infomask2 |= HEAP_KEYS_UPDATED as u16;
                        HEAP_XMAX_EXCL_LOCK
                    }
                    _ => {
                        ereport!(
                            PgLogLevel::ERROR,
                            PgSqlErrorCode::ERRCODE_LOCK_NOT_AVAILABLE,
                            "invalid lock mode"
                        );
                        0 // to keep compiler quiet
                    }
                }
            }
            add_to_xmax
        } else if old_infomask & HEAP_XMAX_IS_MULTI as u16 != 0 {
            Assert(old_infomask & HEAP_XMAX_COMMITTED as u16 == 0);

            if HEAP_LOCKED_UPGRADED(old_infomask) {
                old_infomask &= !HEAP_XMAX_IS_MULTI as u16;
                old_infomask |= HEAP_XMAX_INVALID as u16;
                continue;
            }

            if !MultiXactIdIsRunning(xmax, xmax_is_locked_only(old_infomask)) {
                if xmax_is_locked_only(old_infomask)
                    || !TransactionIdDidCommit(MultiXactIdGetUpdateXid(xmax, old_infomask))
                {
                    old_infomask &= !HEAP_XMAX_IS_MULTI as u16;
                    old_infomask |= HEAP_XMAX_INVALID as u16;
                    continue;
                }
            }

            let new_status = get_mxact_status_for_lock(mode, is_update);
            let new_xmax = MultiXactIdExpand(xmax, add_to_xmax, new_status);
            get_multi_xact_id_hint_bits(new_xmax, &raw mut new_infomask, &raw mut new_infomask2);
            new_xmax
        } else if old_infomask & HEAP_XMAX_COMMITTED as u16 != 0 {
            let status = if old_infomask & HEAP_KEYS_UPDATED as u16 != 0 {
                MultiXactStatusUpdate
            } else {
                MultiXactStatusForNoKeyUpdate
            };

            let new_status = get_mxact_status_for_lock(mode, is_update);
            let new_xmax = MultiXactIdCreate(xmax, status, add_to_xmax, new_status);
            get_multi_xact_id_hint_bits(new_xmax, &raw mut new_infomask, &raw mut new_infomask2);

            new_xmax
        } else if TransactionIdIsInProgress(xmax) {
            let old_status = if xmax_is_locked_only(old_infomask) {
                if HEAP_XMAX_IS_KEYSHR_LOCKED(old_infomask) {
                    MultiXactStatusForKeyShare
                } else if HEAP_XMAX_IS_SHR_LOCKED(old_infomask) {
                    MultiXactStatusForShare
                } else if HEAP_XMAX_IS_EXCL_LOCKED(old_infomask) {
                    if old_infomask2 & HEAP_KEYS_UPDATED as u16 != 0 {
                        MultiXactStatusForUpdate
                    } else {
                        MultiXactStatusForNoKeyUpdate
                    }
                } else {
                    ereport!(
                        PgLogLevel::WARNING,
                        PgSqlErrorCode::ERRCODE_ACTIVE_SQL_TRANSACTION,
                        "LOCK_ONLY found for Xid in progress"
                    );
                    old_infomask |= HEAP_XMAX_INVALID as u16;
                    old_infomask &= !HEAP_XMAX_LOCK_ONLY as u16;
                    continue;
                }
            } else {
                if old_infomask2 & HEAP_KEYS_UPDATED as u16 != 0 {
                    MultiXactStatusUpdate
                } else {
                    MultiXactStatusNoKeyUpdate
                }
            };

            let old_mode = MultiXactStatusLock[old_status as usize];
            if xmax == add_to_xmax {
                Assert(xmax_is_locked_only(old_infomask));
                if mode < old_mode {
                    mode = old_mode;
                }

                old_infomask |= HEAP_XMAX_INVALID as u16;
                continue;
            }

            let new_status = get_mxact_status_for_lock(mode, is_update);
            let new_xmax = MultiXactIdCreate(xmax, old_status, add_to_xmax, new_status);
            get_multi_xact_id_hint_bits(new_xmax, &raw mut new_infomask, &raw mut new_infomask2);

            new_xmax
        } else if !xmax_is_locked_only(old_infomask) && TransactionIdDidCommit(xmax) {
            let status = if old_infomask2 & HEAP_KEYS_UPDATED as u16 != 0 {
                MultiXactStatusUpdate
            } else {
                MultiXactStatusNoKeyUpdate
            };

            let new_status = get_mxact_status_for_lock(mode, is_update);
            let new_xmax = MultiXactIdCreate(xmax, status, add_to_xmax, new_status);
            get_multi_xact_id_hint_bits(new_xmax, &raw mut new_infomask, &raw mut new_infomask2);

            new_xmax
        } else {
            old_infomask |= HEAP_XMAX_INVALID as u16;
            continue;
        };

        break (new_xmax, new_infomask, new_infomask2);
    };

    *result_infomask = new_infomask;
    *result_infomask2 = new_infomask2;
    *result_xmax = new_xmax
}

#[pg_guard]
#[allow(non_snake_case)]
pub unsafe extern "C-unwind" fn PageSetPrunable(page: Page, xid: TransactionId) {
    Assert(TransactionIdIsNormal(xid));
    if !((*(page as PageHeader)).pd_prune_xid == 0.into())
        || TransactionIdPrecedes(xid, (*(page as PageHeader)).pd_prune_xid)
    {
        (*(page as PageHeader)).pd_prune_xid = xid;
    }
}

#[pg_guard]
pub unsafe extern "C-unwind" fn header_set_cmax(
    tup: *mut HeapTupleHeaderData,
    cid: CommandId,
    is_combo: bool,
) {
    Assert(!((*tup).t_infomask & HEAP_MOVED as u16 != 0));
    (*tup).t_choice.t_heap.t_field3.t_cid = cid;
    if is_combo {
        (*tup).t_infomask |= HEAP_COMBOCID as u16;
    } else {
        (*tup).t_infomask &= !HEAP_COMBOCID as u16;
    }
}
