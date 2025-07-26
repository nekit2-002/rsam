#[macro_export]
macro_rules! RelationGetTargetPageFreeSpace {
    ($rel:expr, $defaultff: expr) => {
        pgrx::pg_sys::BLCKSZ as usize * (100 - RelationGetFillFactor!($rel, $defaultff) / 100) as usize
    };
}

pub use RelationGetTargetPageFreeSpace;

#[macro_export]
macro_rules! RelationIsPermanent {
    ($rel:expr) => {
        (*(*$rel).rd_rel).relpersistence == pgrx::pg_sys::RELPERSISTENCE_PERMANENT as i8
    };
}

pub use RelationIsPermanent;

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

pub use RelationGetFillFactor;

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

pub use RelationGetTargetBlock;

#[macro_export]
macro_rules! RelationNeedsWal {
    ($rel:expr) => {
        RelationIsPermanent!($rel)
            && (XLogIsNeeded!()
                || (*$rel).rd_createSubid == 0 && (*$rel).rd_firstRelfilelocatorSubid == 0)
    };
}

pub use RelationNeedsWal;

#[macro_export]
macro_rules! RelationIsLocal {
    ($rel:expr) => {
        (*$rel).rd_islocaltemp || (*$rel).rd_createSubid != 0
    };
}

pub use RelationIsLocal;

#[macro_export]
macro_rules! RelationGetDescr {
    ($rel:expr) => {
        (*$rel).rd_att
    };
}

pub use RelationGetDescr;

#[macro_export]
macro_rules! RelationGetRelId {
    ($rel:expr) => {
        (*$rel).rd_id
    };
}

pub use RelationGetRelId;
