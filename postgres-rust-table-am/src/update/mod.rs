use std::result;

use pgrx::pg_sys::Bitmapset;
use pgrx::pg_sys::HeapTupleData;
use pgrx::pg_sys::IndexAttrBitmapKind::*;
use pgrx::pg_sys::LockTupleMode::*;
use pgrx::pg_sys::MultiXactStatus::*;
use pgrx::pg_sys::TM_Result::*;
use pgrx::pg_sys::TU_UpdateIndexes::*;
use pgrx::pg_sys::TransactionId;
use pgrx::pg_sys::BUFFER_LOCK_UNLOCK;
use pgrx::pg_sys::HEAP_XMAX_INVALID;
use pgrx::pg_sys::HEAP_XMAX_IS_MULTI;
use pgrx::pg_sys::{
    bms_add_members, bms_free, bms_overlap, heap_getattr, visibilitymap_pin, BufferGetPage,
    GetCurrentTransactionId, HeapTupleHeaderGetNatts, HeapTupleSatisfiesUpdate, IsInParallelMode,
    ItemPointerEquals, ItemPointerGetBlockNumber, ItemPointerGetOffsetNumber, ItemPointerIsValid,
    LockBuffer, MultiXactIdSetOldestMember, PageGetItem, PageGetItemId, PageIsAllVisible,
    ReadBuffer, RelationGetIndexAttrBitmap, RelationSupportsSysCache, ReleaseBuffer,
    UnlockReleaseBuffer,
};
use pgrx::pg_sys::{
    CommandId, HeapTuple, InvalidBuffer, InvalidCommandId, InvalidTransactionId, ItemPointer,
    LockTupleMode, Relation, Snapshot, TM_FailureData, TM_Result, TU_UpdateIndexes,
    BUFFER_LOCK_EXCLUSIVE, HEAP_XMAX_KEYSHR_LOCK, HEAP_XMAX_LOCK_ONLY,
};

use pgrx::prelude::*;

use crate::delete::compute_new_xmax_infomask;
use crate::delete::get_multi_xact_id_hint_bits;
use crate::delete::HEAP_LOCKED_UPGRADED;
use crate::include::general::*;

#[pg_guard]
pub unsafe extern "C-unwind" fn determine_columns_info(
    rel: Relation,
    interesting_cols: *mut Bitmapset,
    external_cols: *mut Bitmapset,
    old_tup: HeapTuple,
    new_tup: HeapTuple,
    has_external: *mut bool,
) -> *mut Bitmapset {
    todo!("")
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
    lockMode: *mut LockTupleMode::Type,
    update_indexes: *mut TU_UpdateIndexes::Type,
) -> TM_Result::Type {
    let xid = GetCurrentTransactionId();
    let mut old_key_tuple: HeapTuple = std::ptr::null_mut();
    let mut old_key_copied = false;
    let mut vmbuffer = InvalidBuffer as i32;
    let mut vmbuffer_new = InvalidBuffer as i32;
    let mut have_tuple_lock = false;
    let mut use_hot_update = false;
    let mut summarized_update = false;
    let mut all_visible_cleared = false;
    let mut all_visible_cleared_new = false;
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
        *lockMode = LockTupleNoKeyExclusive;
        MultiXactIdSetOldestMember();
        (MultiXactStatusNoKeyUpdate, true)
    } else {
        *lockMode = LockTupleExclusive;
        (MultiXactStatusUpdate, false)
    };

    let mut infomask_new_tuple = 0u16;
    let mut infomask2_new_tuple = 0u16;

    let (mut checked_lockers, mut locker_remains) = loop {
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

        if !crosscheck.is_null() && result == TM_Ok {}

        if result != TM_Ok {}

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
        *lockMode,
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

    TM_Ok
}
