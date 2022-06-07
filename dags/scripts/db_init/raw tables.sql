	
drop table "staging".questions_cleaned;

CREATE TABLE IF NOT EXISTS "staging".questions_cleaned
(
    question_id bigint ,
	owner_user_id bigint,
    dt date,
    is_answered integer,
    view_count integer,
    answer_count integer,
    score integer,
	accepted_answer_id bigint,
    creation_date timestamp with time zone,
    activity_date timestamp with time zone,
    last_edit_date timestamp with time zone,
    title character varying COLLATE pg_catalog."default",
    closed_date timestamp with time zone,
    closed_reason text COLLATE pg_catalog."default",
    locked_date timestamp with time zone,
	insert_date date
	
) PARTITION BY RANGE (dt);

CREATE TABLE "staging".questions_cleaned_2022_01 PARTITION OF "staging".questions_cleaned 
     FOR VALUES FROM ('2022-01-01') TO ('2022-01-31');
	 

DROP TABLE "staging".answers_cleaned;

CREATE TABLE IF NOT EXISTS "staging".answers_cleaned
(
	answer_id bigint,
	answer_date date,
	question_id bigint,
	owner_user_id bigint,
	creation_date timestamp with time zone,
	last_activity_date timestamp with time zone,
	last_edit_date timestamp with time zone,
    is_accepted varchar(10),
    score integer,
	insert_date date
) PARTITION BY RANGE (answer_date);

CREATE TABLE "staging".answers_cleaned_2022 PARTITION OF "staging".answers_cleaned 
     FOR VALUES FROM ('2022-01-01') TO ('2022-03-31');
	 
DROP TABLE if exists "staging".users_cleaned;


CREATE TABLE IF NOT EXISTS "staging".users_cleaned
(
    user_id bigint,
	creation_date timestamp with time zone,
	last_modified_date timestamp with time zone,
    last_access_date timestamp with time zone,
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

create table "raw".dim_tag
(
tag_id serial primary key,
tag varchar(50) unique
);