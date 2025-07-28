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

#[macro_export]
macro_rules! START_CRIT_SECTION {
    () => {
        pgrx::pg_sys::CritSectionCount += 1;
    };
}

pub use START_CRIT_SECTION;

#[macro_export]
macro_rules! END_CRIT_SECTION {
    () => {
        if pgrx::pg_sys::CritSectionCount > 0 {
            pgrx::pg_sys::CritSectionCount -= 1;
        }
    };
}

pub use END_CRIT_SECTION;
