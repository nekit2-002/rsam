use pgrx::pg_sys::ExceptionalCondition;

#[allow(non_snake_case)]
pub unsafe fn Assert(condition: bool) {
    if !condition {
        let file = file!().as_ptr();
        let line = line!();
        let condition = condition.to_string().as_str().as_ptr();
        ExceptionalCondition(condition.cast(), file.cast(), line as i32);
    }
}
