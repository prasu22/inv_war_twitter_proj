from airflow import DAG
from ssl import CERT_NONE
import pickle, copyreg, ssl
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from mongo_conn import *



default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2015, 6, 1),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

with DAG("Mongo_Conn",default_args=default_args, schedule_interval="@daily", catchup=False) as dag:
    t1=PythonOperator(task_id="Mongo-Conn", python_callable=mongodb_connection)

t1