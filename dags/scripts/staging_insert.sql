/*truncate table "raw".question;
truncate table "raw".answer;
truncate table "raw".account;*/

/*delete from "raw".question where insert_date = '2022-05-26';
delete from "raw".answer where insert_date = '2022-05-26';
delete from "raw".account where insert_date = '2022-05-26';*/


truncate table "raw".questions_cleaned;

insert into "raw".questions_cleaned 
select cast(question_id as bigint) question_id,
tags,
CAST(case when owner_user_id = 'None' then null else RTRIM(owner_user_id,'.0') end as bigint) owner_user_id,
cast(to_timestamp(CAST(creation_date as bigint)) as date) DT,
case when is_answered = 'True' then 1 else 0 end is_answered,
cast(view_count as integer) view_count,
cast(answer_count as integer) answer_count,
cast(score as integer) score,
cast(case when accepted_answer_id ='None' then null else RTRIM(accepted_answer_id,'.0') end as bigint) accepted_answer_id,
to_timestamp(CAST(creation_date as bigint)) creation_date,
to_timestamp(CAST(last_activity_date as bigint)) activity_date,
to_timestamp(CAST(case when last_edit_date = 'None' then null else RTRIM(last_edit_date,'.0') end as bigint)) last_edit_date,
title,
to_timestamp(CAST(case when closed_date = 'None' then null else RTRIM(closed_date,'.0') end as bigint)) closed_date,
case when closed_reason = 'Duplicate' then 'Duplicate' else 'NA' end closed_reason,
to_timestamp(CAST(case when locked_date = '' then null else RTRIM(locked_date,'.0') end as bigint)) locked_date,
insert_date

from "raw".question;



truncate table "raw".answers_cleaned;

insert into "raw".answers_cleaned
select 
cast(answer_id as bigint) answer_id,
cast(to_timestamp(CAST(creation_date as bigint)) as date) DT,
cast(question_id as bigint) question_id,
cast(owner_user_id as bigint) owner_user_id,
to_timestamp(CAST(creation_date as bigint)) creation_date,
to_timestamp(CAST(last_activity_date as bigint)) as last_activity_date,
to_timestamp(CAST(case when last_edit_date = '' then null else RTRIM(last_edit_date,'.0') end as bigint)) last_edit_date,
is_accepted,
cast(score as integer) score,
insert_date

from "raw".answer;

truncate table "raw".users_cleaned;

insert into "raw".users_cleaned
select cast(user_id as bigint) user_id,
to_timestamp(CAST(creation_date as bigint)) creation_date,
to_timestamp(CAST(case when last_modified_date = '' then null else RTRIM(last_modified_date,'.0') end as bigint)) last_modified_date,
to_timestamp(CAST(last_access_date as bigint)) last_access_date,
display_name,
user_type,
case when accept_rate = '' then null else cast(accept_rate as float) end accept_rate,
cast(badge_counts_bronze as integer) badge_counts_bronze,
cast(badge_counts_silver as integer) badge_counts_silver ,
cast(badge_counts_gold as integer) badge_counts_gold,
cast(reputation as integer) reputation,
cast(reputation_change_year as integer) reputation_change_in_year,
cast(reputation_change_quarter as integer) reputation_change_in_quarter,
cast(reputation_change_month as integer) reputation_change_month,
cast(reputation_change_week as integer) reputation_change_week,
cast(reputation_change_day as integer) reputation_change_day,
"country",
insert_date
from "raw".account;




