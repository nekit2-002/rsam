use pgrx::{
    callconv::{Arg, ArgAbi, BoxRet, FcInfo},
    prelude::*,
};
use pgrx_sql_entity_graph::metadata::{
    ArgumentError, Returns, ReturnsError, SqlMapping, SqlTranslatable,
};

pub use pg_sys::TableAmRoutine;

pub const fn new_table_am_routine() -> TableAmRoutine {
    let mut vtable: TableAmRoutine = unsafe { std::mem::MaybeUninit::zeroed().assume_init() };
    vtable.type_ = pg_sys::NodeTag::T_TableAmRoutine;
    vtable
}

/// A special kind of [`pgrx::Internal`] for `GetTableAmRoutine`.
pub struct TableAmArgs;

unsafe impl<'fcx> ArgAbi<'fcx> for TableAmArgs {
    #[inline(always)]
    unsafe fn unbox_arg_unchecked(_arg: Arg<'_, 'fcx>) -> Self {
        Self
    }

    #[inline(always)]
    fn is_virtual_arg() -> bool {
        true
    }
}

unsafe impl SqlTranslatable for TableAmArgs {
    #[inline(always)]
    fn type_name() -> &'static str {
        "internal"
    }

    #[inline(always)]
    fn argument_sql() -> Result<SqlMapping, ArgumentError> {
        Ok(SqlMapping::literal("internal"))
    }

    #[inline(always)]
    fn return_sql() -> Result<Returns, ReturnsError> {
        Ok(Returns::One(SqlMapping::literal("internal")))
    }
}

/// A proper wrapper for pg's `table_am_handler` type.
#[derive(Clone, Copy)]
pub struct TableAmHandler(pub &'static TableAmRoutine);

unsafe impl BoxRet for TableAmHandler {
    #[inline(always)]
    unsafe fn box_into<'fcx>(self, fcinfo: &mut FcInfo<'fcx>) -> pgrx::datum::Datum<'fcx> {
        let datum = pg_sys::Datum::from(self.0 as *const _);
        fcinfo.return_raw_datum(datum)
    }
}

unsafe impl SqlTranslatable for TableAmHandler {
    #[inline(always)]
    fn type_name() -> &'static str {
        "table_am_handler"
    }

    #[inline(always)]
    fn argument_sql() -> Result<SqlMapping, ArgumentError> {
        Ok(SqlMapping::literal("table_am_handler"))
    }

    #[inline(always)]
    fn return_sql() -> Result<Returns, ReturnsError> {
        Ok(Returns::One(SqlMapping::literal("table_am_handler")))
    }
}
