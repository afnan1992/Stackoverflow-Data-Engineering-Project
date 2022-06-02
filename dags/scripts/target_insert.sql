
delete from "dwh".tag_group_dim where insert_date = '{{ ds }}';

insert into "dwh".tag_group_dim 
select tag_group_key,
insert_date
from "staging".tag_group_dim group by 1,2;



delete from "dwh".tag_dim where insert_date = '{{ ds }}';

insert into dwh.tag_dim
select tag_id tag_key,
tags tag_desc,
insert_date
from "staging".tag_group_dim
group by 1,2,3;



delete from "dwh".tag_group_bridge where insert_date = '{{ ds }}';


insert into "dwh".tag_group_bridge
select tag_group_key,
tag_id tag_key,
insert_date

from "staging".tag_group_dim
group by 1,2,3;


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

delete from dwh.question_answer_fact where insert_date = '{{ ds }}';

insert into dwh.question_answer_fact
select
question_asked_date,
coalesce(c.question_key,-1) question_key,
coalesce(d.answer_key,-1) answer_key,
coalesce(e.user_key,-1) question_owner_key,
coalesce(i.user_key,-1)answer_owner_key,
coalesce(g.tag_group_key,-1) tag_group_key,
coalesce(answer_count,0) answer_count,
coalesce(question_score,0) question_score,
coalesce(question_view_count,0) question_view_count,
coalesce(is_answered,0) is_answered,
coalesce(is_answer_accepted,0) is_answer_accepted,
coalesce(accepted_answer_score,0) accepted_answer_score,
a.insert_date

from "staging".questions_answer_staging a
left join "staging".questions_answer_staging_answer_count b
on a.question_id = b.question_id

left join "dwh".question_dim c
on a.question_id = c.question_id
left join "dwh".answer_dim d
on a.accepted_answer_id = d.answer_id
left join dwh.user_dim e
on a.question_owner_id = e.user_id
left join staging.questions_answer_user h
on a.question_id = h.question_id
left join dwh.user_dim i
on h.answer_owner_id = i.user_id
left join tag_group_cte g
on a.tags = g.tags
;



