-- cargo pgrx init
-- cargo pgrx run pg17

create extension rsam;
create table foo(id int) using rsam;
