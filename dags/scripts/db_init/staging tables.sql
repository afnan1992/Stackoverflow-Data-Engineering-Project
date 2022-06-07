drop table "staging".questions_staging;

CREATE TABLE IF NOT EXISTS "staging".questions_staging
(
	question_key serial primary key,
    question_id bigint ,
	tags text,
    dt date,
    creation_date timestamp,
    activity_date timestamp,
    last_edit_date timestamp,
    title text,
    closed_date timestamp,
    closed_reason text,
    locked_date timestamp ,
	insert_date date
	
) ;


DROP TABLE "staging".answers_staging ;

CREATE TABLE IF NOT EXISTS "staging".answers_staging
(
	answer_key serial primary key,
	answer_id bigint,
	answer_date date,
	question_id bigint,
	owner_user_id bigint,
	creation_date timestamp,
	last_activity_date timestamp,
	last_edit_date timestamp,
	insert_date date
) ;



DROP TABLE "staging".users_staging;

CREATE TABLE IF NOT EXISTS "staging".users_staging
(
	user_key serial primary key,
    user_id bigint,
	creation_date timestamp,
	last_modified_date timestamp ,
    last_access_date timestamp,
	display_name varchar(50),
	user_type varchar(50),
	accept_rate float,
	badge_counts_bronze integer,
	badge_counts_silver integer,
	badge_counts_gold integer,
	reputation integer,
	reputation_change_in_year integer,
	reputation_change_in_quarter integer,
	reputation_change_month integer,
	reputation_change_week integer,
	reputation_change_day integer,
	"location" varchar(50),
	insert_date date
);

CREATE TABLE "staging".tags_group_dim
(
	tag_group_key serial primary key,
	tags text
);

create table "staging".tags_group_dim_temp 
(
	tag_group_key bigint,
	tag_key serial primary key,
	tag_desc text
);




