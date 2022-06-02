from airflow.decorators import dag, task
from airflow.operators.python_operator import PythonOperator
from airflow import DAG
from datetime import datetime, timedelta

from utils import insert_csv_into_table


dag = DAG(
    'csv_to_postgres',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
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
    description='csv to postgres load',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 4, 2),
    max_active_runs= 1,
    catchup=False,
    tags=['csv'],
) 

RawToTable = PythonOperator(
dag=dag,
task_id="raw_to_table",
python_callable=insert_csv_into_table,

)

RawToTable