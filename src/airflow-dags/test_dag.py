import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from src.common.app_config import APP_CONFIG
from src.common.utilities import test_util
from src.mongodb.mongo_data_connector import mongodb_connection

LOGGER = logging.getLogger(__name__)


def my_func():
    test_util()
    LOGGER.info(APP_CONFIG.sections())
    LOGGER.info("test dag")


def check_mongo_conn():
    mongo_conn = mongodb_connection()
    db_name = "tweet_db"
    db = mongo_conn[db_name]
    coll = db["tweet_data"]
    x = (list(coll.find(limit=5)))
    LOGGER.info(x)


dag = DAG('test_dag',
          description='Python DAG',
          schedule_interval='*/5 * * * *',
          start_date=datetime(2018, 11, 1),
          catchup=False)

start = EmptyOperator(task_id='start', dag=dag)
test_utils = PythonOperator(task_id='test_utils', python_callable=my_func)
test_mongo = PythonOperator(task_id='test_mongo', python_callable=check_mongo_conn)
end = EmptyOperator(task_id='end', dag=dag, trigger_rule=TriggerRule.ONE_SUCCESS)

start >> test_utils >> test_mongo >> end
