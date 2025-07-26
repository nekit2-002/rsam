use pgrx::pg_sys::ExceptionalCondition;

#[allow(non_snake_case)]
pub unsafe extern "C-unwind" fn Assert(condition: bool) {
    if !condition {
        let file = file!().as_ptr();
        let line = line!();
        let condition = condition.to_string().as_str().as_ptr();
        ExceptionalCondition(condition.cast(), file.cast(), line as i32);
    }
}

#[macro_export]
macro_rules! ItemIdIsNormal {
    ($itemId:expr) => {
        (*$itemId).lp_flags() == pgrx::pg_sys::LP_NORMAL
    };
}

pub use ItemIdIsNormal;
