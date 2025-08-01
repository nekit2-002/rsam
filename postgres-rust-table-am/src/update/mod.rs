use pgrx::pg_sys::{
    bms_add_member, bms_add_members, bms_free, bms_next_member, bms_overlap, heap_getattr, pfree,
    pgstat_count_heap_update, varattrib_1b, varlena, visibilitymap_clear, visibilitymap_pin,
    BufferGetBlockNumber, BufferGetPage, BufferIsValid, ConditionalXactLockTableWait, Datum,
    DatumGetObjectId, DatumGetPointer, DoLockModesConflict, FirstLowInvalidHeapAttributeNumber,
    FirstOffsetNumber, FormData_pg_attribute, GetCurrentTransactionId, GetMultiXactIdMembers,
    HeapTupleGetUpdateXid, HeapTupleHeaderAdjustCmax, HeapTupleHeaderGetCmax,
    HeapTupleHeaderGetNatts, HeapTupleSatisfiesUpdate, IsInParallelMode, ItemPointerEquals,
    ItemPointerGetBlockNumber, ItemPointerGetOffsetNumber, ItemPointerIsValid, LockBuffer,
    MarkBufferDirty, MultiXactIdSetOldestMember, PageClearAllVisible, PageGetHeapFreeSpace,
    PageGetItem, PageGetItemId, PageGetMaxOffsetNumber, PageIsAllVisible, PageSetFull, ReadBuffer,
    RelationGetIndexAttrBitmap, RelationSupportsSysCache, ReleaseBuffer, TableOidAttributeNumber,
    TransactionIdDidAbort, TransactionIdIsCurrentTransactionId, TransactionIdIsInProgress,
    TupleDesc, UnlockReleaseBuffer, UnlockTuple, XactLockTableWait, MAXALIGN,
};
use pgrx::pg_sys::{
    Bitmapset, Buffer, CommandId, HeapTuple, HeapTupleData, IndexAttrBitmapKind::*, ItemPointer,
    LockTupleMode, LockTupleMode::*, MultiXactId, MultiXactStatus, MultiXactStatus::*, Relation,
    Snapshot, TM_FailureData, TM_Result, TM_Result::*, TU_UpdateIndexes, TU_UpdateIndexes::*,
    TransactionId, XLTW_Oper, XLTW_Oper::*, LOCKMODE,
};
use pgrx::pg_sys::{
    InvalidBuffer, InvalidCommandId, InvalidTransactionId, BUFFER_LOCK_EXCLUSIVE,
    BUFFER_LOCK_SHARE, BUFFER_LOCK_UNLOCK, HEAP2_XACT_MASK, HEAP_HOT_UPDATED, HEAP_KEYS_UPDATED,
    HEAP_MOVED, HEAP_ONLY_TUPLE, HEAP_UPDATED, HEAP_XACT_MASK, HEAP_XMAX_BITS, HEAP_XMAX_INVALID,
    HEAP_XMAX_IS_MULTI, HEAP_XMAX_KEYSHR_LOCK, HEAP_XMAX_LOCK_ONLY, VISIBILITYMAP_VALID_BITS,
};

use pgrx::pg_sys::LockWaitPolicy::LockWaitBlock;
use pgrx::pg_sys::VISIBILITYMAP_ALL_FROZEN;
use pgrx::prelude::*;
use std::slice::from_raw_parts_mut;

use crate::include::general::*;
use crate::insert::{
    heap_tuple_header_set_Cmax, heap_tuple_header_set_Cmin, heap_tuple_header_set_Xmax,
    heap_tuple_header_set_Xmin, relation_put_tuple, RelationGetBufferForTuple,
};
use crate::scan::visibility::{tuple_satisfies_visibility, xmax_is_locked_only};
use crate::{delete::*, RelationGetDescr, RelationGetRelId};

// #[pg_guard]
// #[allow(non_snake_case)]
// pub unsafe extern "C-unwind" fn datumGetSize(val: Datum, typ_by_val: bool, typLen: i32) -> usize {
//     if typ_by_val {
//         Assert(typLen > 0 && typLen as usize <= std::mem::size_of::<Datum>());
//         typLen as usize
//     } else {
//         if typLen > 0 {
//             typLen as usize
//         } else if typLen == -1 {
//             let s: *const varlena = DatumGetPointer(val).cast();
//             if s.is_null() {
//                 ereport!(
//                     PgLogLevel::ERROR,
//                     PgSqlErrorCode::ERRCODE_DATA_EXCEPTION,
//                     "invalid Datum pointer"
//                 );
//             }

//             0 // TODO: fix this
//         } else if typLen == -2 {
//             let s = DatumGetPointer(val);
//             if s.is_null() {
//                 ereport!(
//                     PgLogLevel::ERROR,
//                     PgSqlErrorCode::ERRCODE_DATA_EXCEPTION,
//                     "invalid Datum pointer"
//                 );
//             }
//             libc::strlen(s) + 1
//         } else {
//             ereport!(
//                 PgLogLevel::ERROR,
//                 PgSqlErrorCode::ERRCODE_DATATYPE_MISMATCH,
//                 "invalid type len"
//             );
//             0
//         }
//     }
// }

// #[pg_guard]
// #[allow(non_snake_case)]
// pub unsafe extern "C-unwind" fn datumIsEqual(
//     val1: Datum,
//     val2: Datum,
//     typ_by_val: bool,
//     typLen: i32,
// ) -> bool {
//     if typ_by_val {
//         val1 == val2
//     } else {
//         let size1 = datumGetSize(val1, typ_by_val, typLen);
//         let size2 = datumGetSize(val1, typ_by_val, typLen);
//         if size1 != size2 {
//             false
//         } else {
//             let s1 = from_raw_parts_mut(DatumGetPointer(val1), size1);
//             let s2 = from_raw_parts_mut(DatumGetPointer(val2), size1);
//             s1 == s2
//         }
//     }
// }

// #[pg_guard]
// pub unsafe extern "C-unwind" fn attr_equals(
//     tupdesc: TupleDesc,
//     att_num: i32,
//     val1: Datum,
//     val2: Datum,
//     isnull1: bool,
//     isnull2: bool,
// ) -> bool {
//     if isnull1 != isnull2 {
//         return false;
//     }

//     if isnull1 {
//         return true;
//     }

//     if att_num <= 0 {
//         DatumGetObjectId(val1) == DatumGetObjectId(val2)
//     } else {
//         let att = tuple_desc_attr(tupdesc, att_num - 1);
//         datumIsEqual(val1, val2, (*att).attbyval, (*att).attlen as i32)
//     }
// }

#[pg_guard]
pub unsafe extern "C-unwind" fn tuple_desc_attr(
    tupdesc: TupleDesc,
    i: i32,
) -> *mut FormData_pg_attribute {
    (*tupdesc).attrs.as_mut_ptr().offset(i as isize)
}

#[pg_guard]
pub unsafe extern "C-unwind" fn determine_columns_info(
    rel: Relation,
    interesting_cols: *mut Bitmapset,
    _external_cols: *mut Bitmapset, // without external for now
    old_tup: HeapTuple,
    new_tup: HeapTuple,
    _has_external: *mut bool, // without external for now
) -> *mut Bitmapset {
    let mut modified = std::ptr::null_mut();
    let tupdesc = RelationGetDescr!(rel);
    let mut att_id = -1;
    att_id = bms_next_member(interesting_cols, att_id);
    while att_id >= 0 {
        let att_num = att_id + FirstLowInvalidHeapAttributeNumber;
        if att_num == 0 {
            modified = bms_add_member(modified, att_id);
            att_id = bms_next_member(interesting_cols, att_id);
            continue;
        }

        if att_num < 0 {
            if att_num != TableOidAttributeNumber {
                modified = bms_add_member(modified, att_id);
                att_id = bms_next_member(interesting_cols, att_id);
                continue;
            }
        }

        let mut isnull1 = false;
        let _val1 = heap_getattr(old_tup, att_num, tupdesc, &mut isnull1);
        let mut isnull2 = false;
        let _val2 = heap_getattr(new_tup, att_num, tupdesc, &mut isnull2);
        // if !attr_equals(tupdesc, att_num, val1, val2, isnull1, isnull2) {
        //     modified = bms_add_member(modified, att_id);
        //     att_id = bms_next_member(interesting_cols, att_id);
        //     continue;
        // }

        if att_num < 0 || isnull1 || (*tuple_desc_attr(tupdesc, att_num - 1)).attlen != -1 {
            att_id = bms_next_member(interesting_cols, att_id);
            continue;
        }

        // if (VARATT_IS_EXTERNAL((struct varlena *) DatumGetPointer(value1)) &&
        //     bms_is_member(attidx, external_cols))

        att_id = bms_next_member(interesting_cols, att_id);
    }

    modified
}

pub unsafe extern "C-unwind" fn tuple_clear_hot_updated(tup: *mut HeapTupleData) {
    (*(*tup).t_data).t_infomask2 &= !HEAP_HOT_UPDATED as u16;
}

pub unsafe extern "C-unwind" fn tuple_clear_heap_only(tup: *mut HeapTupleData) {
    (*(*tup).t_data).t_infomask2 &= !HEAP_ONLY_TUPLE as u16;
}

pub unsafe extern "C-unwind" fn tuple_set_hot_updated(tup: *mut HeapTupleData) {
    (*(*tup).t_data).t_infomask2 |= HEAP_HOT_UPDATED as u16;
}

pub unsafe extern "C-unwind" fn tuple_set_heap_only(tup: *mut HeapTupleData) {
    (*(*tup).t_data).t_infomask2 |= HEAP_ONLY_TUPLE as u16;
}

#[pg_guard]
#[allow(non_snake_case)]
pub unsafe extern "C-unwind" fn DoesMultiXactIdConflict(
    multi: MultiXactId,
    infomask: u16,
    lock_mode: LockTupleMode::Type,
    current_is_member: *mut bool,
) -> bool {
    let mut result = false;
    let mut members = std::ptr::null_mut();
    let wanted = tupleLockExtraInfo[lock_mode as usize].0;
    if HEAP_LOCKED_UPGRADED(infomask) {
        return false;
    }
    let nmembers = GetMultiXactIdMembers(
        multi,
        &raw mut members,
        false,
        xmax_is_locked_only(infomask),
    );

    if nmembers >= 0 {
        let members_slice = from_raw_parts_mut(members, nmembers as usize);
        for m in members_slice {
            if result && (current_is_member.is_null() || *current_is_member) {
                break;
            }

            let mem_lock_mode: LOCKMODE = LOCKMODE_from_mxstatus!(m.status);
            let mem_xid = m.xid;
            if TransactionIdIsCurrentTransactionId(mem_xid) {
                if !current_is_member.is_null() {
                    *current_is_member = true;
                    continue;
                } else if result {
                    continue;
                }

                if !DoLockModesConflict(mem_lock_mode, wanted) {
                    continue;
                }

                if m.status > MultiXactStatusForUpdate {
                    if TransactionIdDidAbort(mem_xid) {
                        continue;
                    }
                } else {
                    if !TransactionIdIsInProgress(mem_xid) {
                        continue;
                    }
                }

                result = true;
            }
        }
        pfree(members.cast());
    }

    result
}

#[pg_guard]
#[allow(non_snake_case)]
pub unsafe extern "C-unwind" fn MultiXactIdWait(
    multi: MultiXactId,
    status: MultiXactStatus::Type,
    infomask: u16,
    nowait: bool,
    rel: Relation,
    ctid: ItemPointer,
    oper: XLTW_Oper::Type,
    remaining: *mut i32,
) -> bool {
    let mut result = false;
    let mut members = std::ptr::null_mut();
    let mut remain = 0;
    let nmembers = if HEAP_LOCKED_UPGRADED(infomask) {
        -1
    } else {
        GetMultiXactIdMembers(
            multi,
            &raw mut members,
            false,
            xmax_is_locked_only(infomask),
        )
    };

    if nmembers >= 0 {
        let members_slice = from_raw_parts_mut(members, nmembers as usize);
        for m in members_slice {
            let mem_xid = m.xid;
            let mem_status = m.status;
            if TransactionIdIsCurrentTransactionId(mem_xid) {
                remain += 1;
                continue;
            }

            if !DoLockModesConflict(
                LOCKMODE_from_mxstatus!(mem_status),
                LOCKMODE_from_mxstatus!(status),
            ) {
                if !remaining.is_null() && TransactionIdIsInProgress(mem_xid) {
                    remain += 1
                }
                continue;
            }

            if nowait {
                result = ConditionalXactLockTableWait(mem_xid);
                if !result {
                    break;
                }
            } else {
                XactLockTableWait(mem_xid, rel, ctid, oper);
            }
        }

        pfree(members.cast());
    }
    if !remaining.is_null() {
        *remaining = remain;
    }

    result
}

#[pg_guard]
pub unsafe extern "C-unwind" fn tuple_update(
    rel: Relation,
    otid: ItemPointer,
    new_tup: HeapTuple,
    cid: CommandId,
    crosscheck: Snapshot,
    wait: bool,
    tmfd: *mut TM_FailureData,
    lock_mode: *mut LockTupleMode::Type,
    update_indexes: *mut TU_UpdateIndexes::Type,
) -> TM_Result::Type {
    let mut cid = cid;
    let xid = GetCurrentTransactionId();
    let mut old_key_tuple: HeapTuple = std::ptr::null_mut();
    let mut old_key_copied = false;
    let mut vmbuffer = InvalidBuffer as i32;
    let mut vmbuffer_new = InvalidBuffer as i32;
    let mut have_tuple_lock = false;
    let mut use_hot_update = false;
    let mut summarized_update = false;
    // let mut all_visible_cleared = false;
    // let mut all_visible_cleared_new = false;
    let mut id_has_external = false;

    Assert(ItemPointerIsValid(otid));
    Assert(HeapTupleHeaderGetNatts((*new_tup).t_data) <= (*(*rel).rd_rel).relnatts as u16);
    if IsInParallelMode() {
        ereport!(
            PgLogLevel::ERROR,
            PgSqlErrorCode::ERRCODE_INVALID_TRANSACTION_STATE,
            "cannot update tuples during a parallel operation"
        );
    }

    let hot_attrs = RelationGetIndexAttrBitmap(rel, INDEX_ATTR_BITMAP_HOT_BLOCKING);
    let sum_attrs = RelationGetIndexAttrBitmap(rel, INDEX_ATTR_BITMAP_SUMMARIZED);
    let key_attrs = RelationGetIndexAttrBitmap(rel, INDEX_ATTR_BITMAP_KEY);
    let id_attrs = RelationGetIndexAttrBitmap(rel, INDEX_ATTR_BITMAP_IDENTITY_KEY);

    let mut interesting_attrs = std::ptr::null_mut();
    interesting_attrs = bms_add_members(interesting_attrs, hot_attrs);
    interesting_attrs = bms_add_members(interesting_attrs, sum_attrs);
    interesting_attrs = bms_add_members(interesting_attrs, key_attrs);
    interesting_attrs = bms_add_members(interesting_attrs, id_attrs);

    let block = ItemPointerGetBlockNumber(otid);
    let buffer = ReadBuffer(rel, block);
    let page = BufferGetPage(buffer);

    if PageIsAllVisible(page) {
        visibilitymap_pin(rel, block, &raw mut vmbuffer);
    }

    LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE as i32);
    let lp = PageGetItemId(page, ItemPointerGetOffsetNumber(otid));
    if !ItemIdIsNormal!(lp) {
        Assert(RelationSupportsSysCache((*rel).rd_id));
        UnlockReleaseBuffer(buffer);
        Assert(!have_tuple_lock);
        if vmbuffer != InvalidBuffer as i32 {
            ReleaseBuffer(vmbuffer);
        }

        (*tmfd).ctid = *otid;
        (*tmfd).xmax = InvalidTransactionId;
        (*tmfd).cmax = InvalidCommandId;
        *update_indexes = TU_None;
        bms_free(hot_attrs);
        bms_free(sum_attrs);
        bms_free(key_attrs);
        bms_free(id_attrs);
        bms_free(interesting_attrs);
        return TM_Deleted;
    }

    let mut old_tup: HeapTupleData = HeapTupleData {
        t_len: (*lp).lp_len(),
        t_self: *otid,
        t_tableOid: (*rel).rd_id,
        t_data: PageGetItem(page, lp).cast(),
    };

    (*new_tup).t_tableOid = (*rel).rd_id;
    let modified_attrs = determine_columns_info(
        rel,
        interesting_attrs,
        id_attrs,
        &raw mut old_tup,
        new_tup,
        &raw mut id_has_external,
    );

    let (mxact_status, key_intact) = if !bms_overlap(modified_attrs, key_attrs) {
        *lock_mode = LockTupleNoKeyExclusive;
        MultiXactIdSetOldestMember();
        (MultiXactStatusNoKeyUpdate, true)
    } else {
        *lock_mode = LockTupleExclusive;
        (MultiXactStatusUpdate, false)
    };

    let (checked_lockers, locker_remains) = loop {
        let mut checked_lockers = false;
        let mut locker_remains = false;
        let mut result = HeapTupleSatisfiesUpdate(&raw mut old_tup, cid, buffer);
        Assert(result != TM_BeingModified || wait);

        if result == TM_Invisible {
            UnlockReleaseBuffer(buffer);
            ereport!(
                PgLogLevel::ERROR,
                PgSqlErrorCode::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
                "attempted to update invisible tuple"
            );
        } else if result == TM_BeingModified && wait {
            let mut can_continue = false;
            let xwait = (*old_tup.t_data).t_choice.t_heap.t_xmax;
            let mask = (*old_tup.t_data).t_infomask;
            if mask & HEAP_XMAX_IS_MULTI as u16 != 0 {
                let mut current_is_member = false;
                let mut update_xact: TransactionId = 0.into();
                if DoesMultiXactIdConflict(xwait, mask, *lock_mode, &raw mut current_is_member) {
                    LockBuffer(buffer, BUFFER_LOCK_UNLOCK as i32);

                    if !current_is_member {
                        aquire_tuplock(
                            rel,
                            &raw mut (old_tup.t_self),
                            *lock_mode,
                            LockWaitBlock,
                            &raw mut have_tuple_lock,
                        );
                    }

                    let mut remain: i32 = 0;
                    MultiXactIdWait(
                        xwait,
                        mxact_status,
                        mask,
                        false,
                        rel,
                        &raw mut (old_tup.t_self),
                        XLTW_Update,
                        &raw mut remain,
                    );

                    checked_lockers = true;
                    locker_remains = remain != 0;
                    LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE as i32);
                    if xmax_infomask_changed((*old_tup.t_data).t_infomask, mask)
                        || (*old_tup.t_data).t_choice.t_heap.t_xmax != xwait
                    {
                        continue;
                    }

                    update_xact = if !xmax_is_locked_only((*old_tup.t_data).t_infomask) {
                        HeapTupleGetUpdateXid(old_tup.t_data)
                    } else {
                        InvalidTransactionId
                    };

                    if update_xact == 0.into() || TransactionIdDidAbort(update_xact) {
                        can_continue = true;
                    }
                }
            } else if TransactionIdIsCurrentTransactionId(xwait) {
                checked_lockers = true;
                locker_remains = true;
                can_continue = true;
            } else if HEAP_XMAX_IS_KEYSHR_LOCKED(mask) && key_intact {
                checked_lockers = true;
                locker_remains = true;
                can_continue = true;
            } else {
                LockBuffer(buffer, BUFFER_LOCK_UNLOCK as i32);
                aquire_tuplock(
                    rel,
                    &raw mut (old_tup.t_self),
                    *lock_mode,
                    LockWaitBlock,
                    &raw mut have_tuple_lock,
                );

                XactLockTableWait(xwait, rel, &raw mut (old_tup.t_self), XLTW_Update);
                checked_lockers = true;
                LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE as i32);
                if xmax_infomask_changed((*old_tup.t_data).t_infomask, mask)
                    || xwait != (*old_tup.t_data).t_choice.t_heap.t_xmax
                {
                    continue;
                }

                UpdateXmaxHintBits(old_tup.t_data, buffer, xwait);
                if (*old_tup.t_data).t_infomask & HEAP_XMAX_INVALID as u16 != 0 {
                    can_continue = true;
                }
            }

            result = if can_continue {
                TM_Ok
            } else if !ItemPointerEquals(
                &raw mut (old_tup.t_self),
                &raw mut (*old_tup.t_data).t_ctid,
            ) {
                TM_Updated
            } else {
                TM_Deleted
            };
        }

        if result != TM_Ok {
            Assert(
                result == TM_SelfModified
                    || result == TM_Updated
                    || result == TM_Deleted
                    || result == TM_BeingModified,
            );
            Assert(((*old_tup.t_data).t_infomask & HEAP_XMAX_INVALID as u16) == 0);
            Assert(
                result != TM_Updated
                    || !ItemPointerEquals(
                        &raw mut old_tup.t_self,
                        &raw mut (*old_tup.t_data).t_ctid,
                    ),
            );
        }

        if !crosscheck.is_null() && result == TM_Ok {
            if !tuple_satisfies_visibility(&raw mut old_tup, crosscheck, buffer) {
                result = TM_Updated
            }
        }

        if result != TM_Ok {
            (*tmfd).ctid = (*old_tup.t_data).t_ctid;
            (*tmfd).xmax = tuple_header_get_update_xid(old_tup.t_data);
            (*tmfd).cmax = if result == TM_SelfModified {
                HeapTupleHeaderGetCmax(old_tup.t_data)
            } else {
                InvalidCommandId
            };

            UnlockReleaseBuffer(buffer);
            if have_tuple_lock {
                UnlockTuple(
                    rel,
                    &raw mut (old_tup.t_self),
                    tupleLockExtraInfo[*lock_mode as usize].0,
                );
            }

            if vmbuffer != InvalidBuffer as i32 {
                ReleaseBuffer(vmbuffer);
            }

            *update_indexes = TU_None;
            bms_free(hot_attrs);
            bms_free(sum_attrs);
            bms_free(key_attrs);
            bms_free(id_attrs);
            bms_free(modified_attrs);
            bms_free(interesting_attrs);
            return result;
        }

        if vmbuffer == InvalidBuffer as i32 && PageIsAllVisible(page) {
            LockBuffer(buffer, BUFFER_LOCK_UNLOCK as i32);
            visibilitymap_pin(rel, block, &raw mut vmbuffer);
            LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE as i32);
            continue;
        }

        break (checked_lockers, locker_remains);
    };

    let mut xmax_old_tuple: TransactionId = 0.into();
    let mut infomask_old_tuple = 0u16;
    let mut infomask2_old_tuple = 0u16;

    compute_new_xmax_infomask(
        (*old_tup.t_data).t_choice.t_heap.t_xmax,
        (*old_tup.t_data).t_infomask,
        (*old_tup.t_data).t_infomask2,
        xid,
        *lock_mode,
        true,
        &raw mut xmax_old_tuple,
        &raw mut infomask_old_tuple,
        &raw mut infomask2_old_tuple,
    );

    let xmax_new_tuple = if (*old_tup.t_data).t_infomask & HEAP_XMAX_INVALID as u16 != 0
        || HEAP_LOCKED_UPGRADED((*old_tup.t_data).t_infomask)
        || (checked_lockers && !locker_remains)
    {
        InvalidTransactionId
    } else {
        (*old_tup.t_data).t_choice.t_heap.t_xmax
    };

    let mut infomask_new_tuple = 0u16;
    let mut infomask2_new_tuple = 0u16;
    if xmax_new_tuple == 0.into() {
        infomask_new_tuple = HEAP_XMAX_INVALID as u16;
        infomask2_new_tuple = 0;
    } else {
        if (*old_tup.t_data).t_infomask & HEAP_XMAX_IS_MULTI as u16 != 0 {
            get_multi_xact_id_hint_bits(
                xmax_new_tuple,
                &raw mut infomask_new_tuple,
                &raw mut infomask2_new_tuple,
            );
        } else {
            infomask_new_tuple = (HEAP_XMAX_KEYSHR_LOCK | HEAP_XMAX_LOCK_ONLY) as u16;
            infomask2_new_tuple = 0;
        }
    }

    (*(*new_tup).t_data).t_infomask &= !HEAP_XACT_MASK as u16;
    (*(*new_tup).t_data).t_infomask2 &= !HEAP2_XACT_MASK as u16;
    heap_tuple_header_set_Xmin((*new_tup).t_data, xid);
    heap_tuple_header_set_Cmin((*new_tup).t_data, cid);
    (*(*new_tup).t_data).t_infomask |= HEAP_UPDATED as u16 | infomask_new_tuple;
    (*(*new_tup).t_data).t_infomask2 |= infomask2_new_tuple;
    heap_tuple_header_set_Xmax((*new_tup).t_data, xmax_new_tuple);

    let mut is_combo = false;
    HeapTupleHeaderAdjustCmax(old_tup.t_data, &raw mut cid, &raw mut is_combo);
    // without toaster stuff at the moment
    let needs_toast = false;
    let mut pageFree = PageGetHeapFreeSpace(page);
    let newtupsize = MAXALIGN((*new_tup).t_len as usize);
    let (new_buf, heaptup) = if needs_toast || newtupsize > pageFree {
        let mut xmax_lock_old_tuple: TransactionId = 0.into();
        let mut infomask_lock_old_tuple = 0u16;
        let mut infomask2_lock_old_tuple = 0u16;
        // let mut cleared_all_frozen = false;

        compute_new_xmax_infomask(
            (*old_tup.t_data).t_choice.t_heap.t_xmax,
            (*old_tup.t_data).t_infomask,
            (*old_tup.t_data).t_infomask2,
            xid,
            *lock_mode,
            false,
            &raw mut xmax_lock_old_tuple,
            &raw mut infomask_lock_old_tuple,
            &raw mut infomask2_lock_old_tuple,
        );

        Assert(xmax_is_locked_only(infomask_lock_old_tuple));
        START_CRIT_SECTION!();
        (*old_tup.t_data).t_infomask &= !(HEAP_XMAX_BITS | HEAP_MOVED) as u16;
        (*old_tup.t_data).t_infomask2 &= !HEAP_KEYS_UPDATED as u16;
        tuple_clear_hot_updated(&raw mut old_tup);
        Assert(xmax_lock_old_tuple != 0.into());
        heap_tuple_header_set_Xmax(old_tup.t_data, xmax_lock_old_tuple);
        (*old_tup.t_data).t_infomask |= infomask_lock_old_tuple;
        (*old_tup.t_data).t_infomask2 |= infomask2_lock_old_tuple;
        heap_tuple_header_set_Cmax(old_tup.t_data, cid, is_combo);
        (*old_tup.t_data).t_ctid = old_tup.t_self;

        if PageIsAllVisible(page)
            && visibilitymap_clear(rel, block, vmbuffer, VISIBILITYMAP_ALL_FROZEN as u8)
        {
            // cleared_all_frozen = true;
        }

        MarkBufferDirty(buffer);
        // wal stuff
        // if RelationNeedsWal!(rel) {}

        END_CRIT_SECTION!();

        LockBuffer(buffer, BUFFER_LOCK_UNLOCK as i32);
        // here must be needs_toast condition
        let heaptup = new_tup;
        // let mut newbuf = InvalidBuffer as i32;
        let newbuf = loop {
            if newtupsize > pageFree {
                break RelationGetBufferForTuple(
                    rel,
                    (*heaptup).t_len as usize,
                    buffer,
                    0,
                    std::ptr::null_mut(),
                    &raw mut vmbuffer_new,
                    &raw mut vmbuffer,
                    0,
                );
            }

            if vmbuffer == InvalidBuffer as i32 && PageIsAllVisible(page) {
                visibilitymap_pin(rel, block, &raw mut vmbuffer);
            }

            LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE as i32);
            pageFree = PageGetHeapFreeSpace(page);
            if newtupsize > pageFree || (vmbuffer == InvalidBuffer as i32 && PageIsAllVisible(page))
            {
                LockBuffer(buffer, BUFFER_LOCK_UNLOCK as i32);
            } else {
                break buffer;
            }
        };
        (newbuf, heaptup)
    } else {
        (buffer, new_tup)
    };

    if new_buf == buffer {
        if !bms_overlap(modified_attrs, hot_attrs) {
            use_hot_update = true;
            if bms_overlap(modified_attrs, sum_attrs) {
                summarized_update = true;
            }
        }
    } else {
        PageSetFull(page);
    }

    old_key_tuple = extract_replica_identity(
        rel,
        &raw mut old_tup,
        bms_overlap(modified_attrs, id_attrs) || id_has_external,
        &raw mut old_key_copied,
    );

    START_CRIT_SECTION!();
    PageSetPrunable(page, xid);
    if use_hot_update {
        tuple_set_hot_updated(&raw mut old_tup);
        tuple_set_heap_only(heaptup);
        tuple_set_heap_only(new_tup);
    } else {
        tuple_clear_hot_updated(&raw mut old_tup);
        tuple_clear_heap_only(heaptup);
        tuple_clear_heap_only(new_tup);
    }
    relation_put_tuple(rel, new_buf, heaptup);

    (*old_tup.t_data).t_infomask &= !(HEAP_XMAX_BITS | HEAP_MOVED) as u16;
    (*old_tup.t_data).t_infomask2 &= !HEAP_KEYS_UPDATED as u16;
    Assert(xmax_old_tuple != 0.into());
    heap_tuple_header_set_Xmax(old_tup.t_data, xmax_old_tuple);
    (*old_tup.t_data).t_infomask |= infomask_old_tuple;
    (*old_tup.t_data).t_infomask2 |= infomask2_old_tuple;
    heap_tuple_header_set_Cmax(old_tup.t_data, cid, is_combo);
    (*old_tup.t_data).t_ctid = (*heaptup).t_self;

    if PageIsAllVisible(BufferGetPage(buffer)) {
        // all_visible_cleared = true;
        PageClearAllVisible(BufferGetPage(buffer));
        visibilitymap_clear(
            rel,
            BufferGetBlockNumber(buffer),
            vmbuffer,
            VISIBILITYMAP_VALID_BITS as u8,
        );
    }

    if new_buf != buffer && PageIsAllVisible(BufferGetPage(new_buf)) {
        // all_visible_cleared_new = true;
        PageClearAllVisible(BufferGetPage(new_buf));
        visibilitymap_clear(
            rel,
            BufferGetBlockNumber(new_buf),
            vmbuffer_new,
            VISIBILITYMAP_VALID_BITS as u8,
        );
    }

    if new_buf != buffer {
        MarkBufferDirty(new_buf);
    }

    MarkBufferDirty(buffer);
    // wal stuff here

    END_CRIT_SECTION!();
    if new_buf != buffer {
        LockBuffer(new_buf, BUFFER_LOCK_UNLOCK as i32);
    }
    LockBuffer(buffer, BUFFER_LOCK_UNLOCK as i32);

    if new_buf != buffer {
        ReleaseBuffer(new_buf);
    }
    ReleaseBuffer(buffer);

    if BufferIsValid(vmbuffer_new) {
        ReleaseBuffer(vmbuffer_new);
    }

    if BufferIsValid(vmbuffer) {
        ReleaseBuffer(vmbuffer);
    }

    if have_tuple_lock {
        UnlockTuple(
            rel,
            &raw mut (old_tup.t_self),
            tupleLockExtraInfo[*lock_mode as usize].0,
        );
    }

    pgstat_count_heap_update(rel, use_hot_update, new_buf != buffer);
    if heaptup != new_tup {
        (*new_tup).t_self = (*heaptup).t_self;
        pfree(heaptup.cast());
    }

    *update_indexes = if use_hot_update {
        if summarized_update {
            TU_Summarizing
        } else {
            TU_None
        }
    } else {
        TU_All
    };

    if !old_key_tuple.is_null() && old_key_copied {
        pfree(old_key_tuple.cast());
    }

    bms_free(hot_attrs);
    bms_free(sum_attrs);
    bms_free(key_attrs);
    bms_free(id_attrs);
    bms_free(modified_attrs);
    bms_free(interesting_attrs);

    TM_Ok
}

#[pg_guard]
pub unsafe extern "C-unwind" fn SerializationNeededForRead(rel: Relation, snapshot: Snapshot) {
    // if  {

    // }
}

#[pg_guard]
pub unsafe extern "C-unwind" fn PredicateLockTID(
    rel: Relation,
    tid: ItemPointer,
    snapshot: Snapshot,
    tuple_xid: TransactionId,
) {
    // if  {

    // }
}

#[pg_guard]
pub unsafe extern "C-unwind" fn fetch_tuple(
    rel: Relation,
    snapshot: Snapshot,
    tup: HeapTuple,
    user_buf: *mut Buffer,
    keep_buf: bool,
) -> bool {
    let tid = &raw mut (*tup).t_self;
    let buffer = ReadBuffer(rel, ItemPointerGetBlockNumber(tid));

    LockBuffer(buffer, BUFFER_LOCK_SHARE as i32);

    let page = BufferGetPage(buffer);
    let offnum = ItemPointerGetOffsetNumber(tid);

    if offnum < FirstOffsetNumber || offnum > PageGetMaxOffsetNumber(page) {
        LockBuffer(buffer, BUFFER_LOCK_UNLOCK as i32);
        ReleaseBuffer(buffer);
        *user_buf = InvalidBuffer as i32;
        (*tup).t_data = std::ptr::null_mut();
        return false;
    }

    let lp = PageGetItemId(page, offnum);
    if !ItemIdIsNormal!(lp) {
        LockBuffer(buffer, BUFFER_LOCK_UNLOCK as i32);
        ReleaseBuffer(buffer);
        *user_buf = InvalidBuffer as i32;
        (*tup).t_data = std::ptr::null_mut();
        return false;
    }

    (*tup).t_data = PageGetItem(page, lp).cast();
    (*tup).t_len = (*lp).lp_len();
    (*tup).t_tableOid = RelationGetRelId!(rel);

    let valid = tuple_satisfies_visibility(tup, snapshot, buffer);
    if valid {
        // PredicateLockTID(
        //     rel,
        //     &raw mut (*tup).t_self,
        //     snapshot,
        //     HeapTupleHeaderGetXmin((*tup).t_data),
        // );
    }
    LockBuffer(buffer, BUFFER_LOCK_UNLOCK as i32);
    if valid {
        *user_buf = buffer;
        return true;
    }

    *user_buf = if keep_buf {
        buffer
    } else {
        ReleaseBuffer(buffer);
        (*tup).t_data = std::ptr::null_mut();
        InvalidBuffer as i32
    };
    false
}
