-- PostgreSQL 13
-- Create a table to hold the queue
drop table if exists app_ec2_queue;
create table app_ec2_queue (
    id text PRIMARY KEY,
    queue integer,
    app_lock bool default true,
    last_update timestamp default current_timestamp
);
-- take a look
select *
from app_ec2_queue
order by last_update desc
;
-- insert some data
insert into app_ec2_queue values ('001', 0);
insert into app_ec2_queue values ('002', 0);
update app_ec2_queue set queue = 1, app_lock = true, last_update = current_timestamp where id = '001';
-- run some tests
select * from "app_ec2_queue"
where id = "test"
;
