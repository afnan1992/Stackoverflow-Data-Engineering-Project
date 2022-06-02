
--applying business rules and transformations

truncate table  "staging".questions_answer_staging;

insert into "staging".questions_answer_staging 
select a.question_id,
tags,
b.answer_id accepted_answer_id,
dt question_asked_date,
a.owner_user_id question_owner_id,
b.owner_user_id answer_owner_id,
is_answered,
case when b.answer_id is null then 0 else 1 end is_answer_accepted,
view_count question_view_count,
a.score question_score,
b.score accepted_answer_score,
a.insert_date

from "raw".questions_cleaned a
left join "raw".answers_cleaned b
on a.accepted_answer_id = b.answer_id;

truncate table staging.questions_answer_user_tagged;

insert into staging.questions_answer_user_tagged 
select a.question_id,
b.answer_id,
b.owner_user_id answer_owner_id

from raw.questions_cleaned a
left join raw.answers_cleaned b
on a.question_id = b.question_id
where is_accepted = 'True';

truncate table staging.questions_answer_user;

insert into staging.questions_answer_user 
select a.question_id,
case when a.answer_owner_id is null then b.answer_owner_id else a.answer_owner_id end answer_owner_id

from "staging".questions_answer_staging a
left join staging.questions_answer_user_tagged b
on a.question_id = b.question_id;


truncate table  "staging".questions_answer_staging_answer_count;

insert into "staging".questions_answer_staging_answer_count
select question_id,
count(answer_id) answer_count

from "raw".answers_cleaned a
group by 1;

--users scd 3

truncate table "staging".users_staging_indicators;

insert into "staging".users_staging_indicators 
select 
    s.* , 
    case when c.user_id is null then 1 else 0 end new_ind,
    case when c.user_id is not null and s.last_access_date::timestamp <> c.last_access_date::timestamp then 1 else 0 end track_ind
from "raw".users_cleaned s
left join "dwh".user_dim c
on s.user_id = c.user_id
;



begin transaction;


update "dwh".user_dim
set end_date = start_date 
from "staging".users_staging_indicators 
where end_date is NULL
and track_ind = 1;

insert into "dwh".user_dim
(
	user_id ,creation_date,last_modified_date,last_access_date,display_name,user_type,accept_rate,badge_counts_bronze, badge_counts_silver,badge_counts_gold,reputation,reputation_change_in_year,reputation_change_in_quarter,reputation_change_month,reputation_change_week,reputation_change_day,"location",start_date,end_date
) 
select
    user_id ,creation_date,last_modified_date,last_access_date,display_name,user_type,accept_rate,badge_counts_bronze, badge_counts_silver,badge_counts_gold,reputation,reputation_change_in_year,reputation_change_in_quarter,reputation_change_month,reputation_change_week,reputation_change_day,"location", 
	current_timestamp as start_date, 
	null
from "staging".users_staging_indicators
where track_ind = 1 or new_ind = 1;

end transaction;


--answers scd 1


insert into dwh.answer_dim (answer_id,answer_date, question_id,owner_user_id,creation_date,last_activity_date,last_edit_date)
select  answer_id,answer_date, question_id,owner_user_id,creation_date,last_activity_date,last_edit_date
from raw.answers_cleaned b
on conflict (answer_id) do 
update set answer_id = excluded.answer_id,
answer_date = excluded.answer_date,
question_id = excluded.question_id,
owner_user_id = excluded.owner_user_id,
creation_date = excluded.last_activity_date,
last_edit_date = excluded.last_edit_date;


--questions scd 1


insert into dwh.question_dim (question_id,dt, creation_date,activity_date,last_edit_date,title,closed_date,closed_reason,locked_date)
select  question_id,dt, creation_date,activity_date,last_edit_date,title,closed_date,closed_reason,locked_date
from raw.questions_cleaned b
on conflict (question_id) do 
update set question_id = excluded.question_id,
dt = excluded.dt,
creation_date = excluded.creation_date,
activity_date = excluded.activity_date,
last_edit_date = excluded.last_edit_date,
title = excluded.title,
closed_date = excluded.closed_date,
closed_reason = excluded.closed_reason,
locked_date = excluded.locked_date;













