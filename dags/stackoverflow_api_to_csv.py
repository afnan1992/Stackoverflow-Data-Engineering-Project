from airflow.decorators import dag, task
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow import DAG
from datetime import datetime, timedelta

from ingestion_functions import stackoverflow_api_to_csv,insert_all_csv_files_into_postgres_table


dag = DAG(
    'api_to_csv_dag',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'email': ['afnanshahid1992@gmail.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function,
        # 'on_success_callback': some_other_function,
        # 'on_retry_callback': another_function,
        # 'sla_miss_callback': yet_another_function,
        # 'trigger_rule': 'all_success'
    },
    description='ETL DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 5, 26),
    end_date = datetime(2022,5,27),
    max_active_runs= 1,
    catchup=True,
    tags=['api'],
) 

ApiToRaw = PythonOperator(
dag=dag,
task_id="api_to_raw",
python_callable=stackoverflow_api_to_csv,

) 

RawtoPostgres = PythonOperator(
dag=dag,
task_id="raw_to_postgres",
python_callable=insert_all_csv_files_into_postgres_table,

) 

StagingInsert = PostgresOperator(
    task_id="staging_insert",
    postgres_conn_id="LOCAL",
    sql="scripts/staging_insert.sql",

)

StagingTransform = PostgresOperator(
    task_id="staging_transform",
    postgres_conn_id="LOCAL",
    sql="scripts/staging_transform.sql",

)

CreateTagBridgeTable = PostgresOperator(
    task_id="create_bridge_table",
    postgres_conn_id="LOCAL",
    sql="scripts/create_tag_bridge_table.sql",

)

InsertintoWareHouse = PostgresOperator(
    task_id="target_insert",
    postgres_conn_id="LOCAL",
    sql="scripts/target_insert.sql",

)

ApiToRaw >> RawtoPostgres >> StagingInsert >> [StagingTransform , CreateTagBridgeTable,] >> InsertintoWareHouse