drop table if exists tag_group_cte;

create temporary table tag_group_cte as
with cte as
(
select tag_group_key,
a.tag_key,
string_agg(tag_desc,',') tags
	
from "dwh".tag_group_bridge a
join dwh.tag_dim b
on a.tag_key = b.tag_key
group by 1,2
order by 1,3
)
select tag_group_key,
string_agg(tags,',') tags
from cte
group by 1
;

drop table if exists staging.fetch_new_tags;

create table "staging".fetch_new_tags as
select a.tags,
case when b.tags is not null then b.tag_group_key else null end tag_group_key,
insert_date

from raw.questions_cleaned a
left join tag_group_cte b
on trim(a.tags) = trim(b.tags)
;

drop table if exists staging.new_tags;

create table "staging".new_tags as
select tags,
insert_date
from "staging".fetch_new_tags
where tag_group_key is null;

drop table if exists "staging".group_bridge;

create table "staging".group_bridge as
with cte as
(
select tags,
insert_date
from "staging".new_tags a
group by 1,2
)
select max_key + row_number() over (order by tags) tag_group_key,
tags,
insert_date
from cte a
LEFT JOIN (SELECT MAX(tag_group_key) max_key FROM dwh.tag_group_dim) final_grp ON 1=1;

drop table if exists "staging".deduped_tags;

create table "staging".deduped_tags as
select
trim(unnest(string_to_array(tags, ','))) tags

from "staging".group_bridge
group by 1
;

truncate table staging.dim_tag;

insert into staging.dim_tag(tag)
select tags from "staging".deduped_tags;

drop table if exists "staging".tag_group_dim;

create table "staging".tag_group_dim as
with cte as
(
select tag_group_key,
trim(unnest(string_to_array(tags, ','))) tags,
insert_date
from staging.group_bridge
)
select tag_group_key,
tag_id,
tags,
insert_date

from cte 
join "staging".dim_tag b
on cte.tags = b.tag;