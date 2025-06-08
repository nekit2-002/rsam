use pgrx::pg_sys::{TupleTableSlot, TupleTableSlotOps};

// TODO: This struct is uncompleted
pub struct RsTableSlot {
    pub base: TupleTableSlot,
}

unsafe extern "C" {
    // TODO: Implement rsam tuple table slot operations
    pub static TTSOpsRsAmTuple: TupleTableSlotOps;
}
