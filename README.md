
# Stackoverflow Data Engineering Project

A fully managed data pipeline orchestrated using Airflow. I used the stackoveflow api to ingest raw data into csv files using python, and then wrote SQL to transform the data into a star schema. For the purposes of this project, I tracked python and pandas tags on stackoverflow, and tracked their trend over time. The unit of time to build the pipeline is day, you can easily backfill data using Airflow.

# Architecture

!(/stackoverflow_data_engineering_project.png)

# Data Model

1. Fact Questions Answer is the main fact table, each row basically tells the question asked and the correct answer associated with it
2. User dimension is used to find the user who asked the question, and the person who answered it.
3. Answer dimension is used to find attributes related to the answer
4. Question dimension is used to find attributes related to the question.
5. Tag dimension is basically a bridge table that tells which tags are associated with the question. I had to use a bridge to avoid many to many joins to the fact table.


# How to Run
  Make sure that you have a local instance of Postgres running on port 5432
  Go to the root directory of this project
  Run docker-compose build
  Run docker-compose up
 