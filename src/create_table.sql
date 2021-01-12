-- PostgreSQL 13
-- Create a table to hold the queue
drop table if exists app_ec2q_tbl;
create table app_ec2q_tbl (
    id text PRIMARY KEY,
    queue integer check (queue >= 0),
    app_lock bool default true,
    last_update timestamp default current_timestamp
);
-- insert some data
insert into app_ec2q_tbl values ('001', 0);
insert into app_ec2q_tbl values ('002', 0);
update app_ec2q_tbl set queue = 1, app_lock = true, last_update = current_timestamp where id = '001';
-- take a look
select * from app_ec2q_tbl order by last_update desc;
-- run some tests
select * from "app_ec2q_tbl"
where id = "test"
;
