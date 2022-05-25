import logging
from datetime import datetime
import re

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from src.common.app_config import APP_CONFIG
from src.common.variable_files import COUNTRY_CODE_MAPPED, COUNTRY_NAME_KEY, COUNTRY_CODE_KEY, COUNTRY_NAME, \
    COUNTRY_CODE, COVID_KEYWORD_KEY, TWEET_KEY, DONATION_AMOUNT_KEY, DEFAULT_AMOUNT, CURRENCY_MAPPING, \
    CURRENCY_NAME_KEY, DEFAULT_CURRENCY_NAME, DONATION_KEYWORDS_KEY, COLL_OF_RAW_DATA, PREVENTION_KEYWORDS_KEY, \
    WHO_KEYWORDS_KEY, COVID_TRENDING_KEYWORD_KEY, ECONOMY_TRENDING_KEYWORD_KEY
from src.encryption_and_decryption_data.encrypt_and_decryption_data import encrypt_string
from src.mongodb.mongo_data_connector import mongodb_connection
from src.pub_sub.data_extract.covid_keywords_extractor import COVID_KEYWORDS

LOGGER = logging.getLogger(__name__)

mongo_conn = mongodb_connection()
db_name = "tweet_new_db"
db = mongo_conn[db_name]
coll = db["tweet_raw_data"]


DONATION_KEYWORDS = list(map(str, APP_CONFIG.getlist('keywords', 'DONATION_KEYWORDS')))
PREVENTION_KEYWORDS = list(map(str, APP_CONFIG.getlist('keywords', 'PREVENTION_KEYWORDS')))
WHO_KEYWORDS = list(map(str, APP_CONFIG.getlist('keywords', 'WHO_KEYWORDS')))
COVID_KEYS = list(map(str, APP_CONFIG.getlist('keywords', 'COVID_KEYS')))
ECONOMY_KEYS = list(map(str, APP_CONFIG.getlist('keywords', 'ECONOMY_KEYS')))



# get// call
#  change name of functions

def get_country_code(message,ti):
    list_of_country = list(COUNTRY_CODE_MAPPED.keys())
    country = re.compile('|'.join(list_of_country), re.IGNORECASE).search(message[COUNTRY_NAME_KEY])
    try:

        if country:
            country = re.compile('|'.join(list_of_country), re.IGNORECASE).search(message[COUNTRY_NAME_KEY]).group(0).title()
            country_code = COUNTRY_CODE_MAPPED[country]
            message[COUNTRY_NAME_KEY] = country
            message[COUNTRY_CODE_KEY] = country_code
            updated_message = ti.xcom_push(key='updated_message', value=message)
            # return message
            print(updated_message)


        else:
            message[COUNTRY_NAME_KEY] = COUNTRY_NAME
            message[COUNTRY_CODE_KEY] = COUNTRY_CODE
            updated_message = ti.xcom_push(key='updated_message', value=message)

            print(updated_message)

    except Exception as e:
        LOGGER.error(f"ERROR:{e}")

def get_covid_keywords(ti):

    message = ti.xcom_pull(task_ids='get_country_code', key='updated_message')
    list_of_covid_keywords = []
    try:
        if re.compile('|'.join(COVID_KEYWORDS), re.IGNORECASE).search(message[TWEET_KEY]):
            list_of_covid_keywords = re.compile('|'.join(COVID_KEYWORDS), re.IGNORECASE).findall(message[TWEET_KEY])
        else:
            LOGGER.info(f"MESSAGE: Data Not found in tweet! ")
        message[COVID_KEYWORD_KEY] = list_of_covid_keywords
        updated_message = ti.xcom_push(key='updated_message', value=message)
        print(message)
    except Exception as e:
        LOGGER.error(f"ERROR:{e} ")


def get_donation_amount(ti):

    message = ti.xcom_pull(task_ids='get_covid_keywords', key='updated_message')
    try:
        if re.compile('[$¢£¤¥֏؋৲৳৻૱௹฿៛₹](\s?)(\d[ 0-9,.]+)(k)?(m)?(b)?(M)?(B)?(cr)?(Cr)?').search(message[TWEET_KEY]):
            amount = re.compile('[$¢£¤¥֏؋৲৳৻૱௹฿៛₹](\s?)(\d[ 0-9,.]+)(k)?(m)?(b)?(M)?(B)?(cr)?(Cr)?').search(message[TWEET_KEY]).group(0)
            value = float(re.sub(r'[^\d.]', '', amount))
            message[DONATION_AMOUNT_KEY] = value
            updated_message = ti.xcom_push(key='updated_message', value=message)
            print(message)

        elif re.compile(r"(\d[ 0-9,.]+)(k)?(m)?(b)?(M)?(B)?(cr)?(Cr)?(\s?)(USD\b|\bINR\b)", re.IGNORECASE).search(message[TWEET_KEY]):
            amount = re.compile(r"(\d[ 0-9,.]+)(k)?(m)?(b)?(M)?(B)?(cr)?(Cr)?(\s?)(USD\b|\bINR\b)", re.IGNORECASE).search(message[TWEET_KEY]).group(0)
            value = float(re.sub(r'[^\d.]', '', amount))
            message[DONATION_AMOUNT_KEY] = value
            updated_message = ti.xcom_push(key='updated_message', value=message)
            print(message)

        else:
            message[DONATION_AMOUNT_KEY] = DEFAULT_AMOUNT
            updated_message = ti.xcom_push(key='updated_message', value=message)
            print(message)
    except Exception as e:
        LOGGER.error(f"ERROR:{e} ")


def get_donation_currency(ti):
    message = ti.xcom_pull(task_ids='get_donation_amount', key='updated_message')
    print("donation currencey")
    try:
        if re.compile('[$¢£¥฿€₹](\s?)(\d[ 0-9,.]+)').search(message[TWEET_KEY]):
            currency_symbol = re.compile('[$¢£¥฿€₹]').search(message[TWEET_KEY]).group(0)
            message[CURRENCY_NAME_KEY] = CURRENCY_MAPPING[currency_symbol]
            updated_message = ti.xcom_push(key='updated_message', value=message)
            print(message)

        elif re.compile(r"(\d[ 0-9,.]+)(k)?(m)?(b)?(M)?(B)?(cr)?(Cr)?(\s?)(USD\b|\bINR\b)", re.IGNORECASE).search(
                message[TWEET_KEY]):
            currency_name = re.compile(r"\bUSD\b|\bINR\b", re.IGNORECASE).search(message[TWEET_KEY]).group(0)
            message[CURRENCY_NAME_KEY] = currency_name.upper()
            updated_message = ti.xcom_push(key='updated_message', value=message)
            print(message)

        else:
            message[CURRENCY_NAME_KEY] = DEFAULT_CURRENCY_NAME
            updated_message = ti.xcom_push(key='updated_message', value=message)
            print(message)
    except Exception as e:
        LOGGER.error(f"ERROR:{e} ")

def get_donation_keywords(ti):
    message = ti.xcom_pull(task_ids='get_donation_currency', key='updated_message')
    list_of_donation_keywords = []
    try:
        if re.compile('|'.join(DONATION_KEYWORDS), re.IGNORECASE).search(message[TWEET_KEY]):
            list_of_donation_keywords = re.compile('|'.join(DONATION_KEYWORDS), re.IGNORECASE).findall(message[TWEET_KEY])
        else:
            LOGGER.info(f"Data Not found in tweet! ")
        message[DONATION_KEYWORDS_KEY] = list_of_donation_keywords
        updated_message = ti.xcom_push(key='updated_message', value=message)

        print(message)
    except Exception as e:
        LOGGER.error(f"ERROR:{e} ")


def get_prevention_keywords(ti):
    message = ti.xcom_pull(task_ids='get_donation_keywords', key='updated_message')
    list_of_prevention_keywords = []
    try:
        if re.compile('|'.join(PREVENTION_KEYWORDS), re.IGNORECASE).search(message[TWEET_KEY]):
            list_of_prevention_keywords = re.compile('|'.join(PREVENTION_KEYWORDS), re.IGNORECASE).findall(
                message[TWEET_KEY])
        else:
            LOGGER.info(f"MESSAGE:Data is not found ")
        message[PREVENTION_KEYWORDS_KEY] = list_of_prevention_keywords
        updated_message = ti.xcom_push(key='updated_message', value=message)
        print(message)
    except Exception as e:
        LOGGER.error(f"ERROR:{e} ")



def get_who_keywords(ti):
    message = ti.xcom_pull(task_ids='get_prevention_keywords', key='updated_message')
    list_of_who_keywords = []
    try:
        if re.compile('|'.join(WHO_KEYWORDS), re.IGNORECASE).search(message[TWEET_KEY]):
            list_of_who_keywords = re.compile('|'.join(WHO_KEYWORDS), re.IGNORECASE).findall(message[TWEET_KEY])
        else:
            LOGGER.info(f"MESSAGE:Data is not found ")
        message[WHO_KEYWORDS_KEY] = list_of_who_keywords
        updated_message = ti.xcom_push(key='updated_message', value=message)
        print(message)
    except Exception as e:
        LOGGER.error(f"ERROR:{e} ")

def get_tweets_with_trending_covid_keywords(ti):
    message = ti.xcom_pull(task_ids='get_who_keywords', key='updated_message')
    try:
        list_of_covid_keywords = []
        if re.compile('|'.join(COVID_KEYS), re.IGNORECASE).search(message[TWEET_KEY]):
            list_of_covid_keywords = re.compile('|'.join(COVID_KEYS), re.IGNORECASE).findall(message[TWEET_KEY])
        else:
            LOGGER.info(f"MESSAGE: Data is not found")
        message[COVID_TRENDING_KEYWORD_KEY] = list_of_covid_keywords
        updated_message = ti.xcom_push(key='updated_message', value=message)
        print(message)
    except Exception as e:
        LOGGER.error(f"ERROR:{e} ")


def get_tweets_with_trending_economy_keywords(ti):
    message = ti.xcom_pull(task_ids='get_tweets_with_trending_covid_keywords', key='updated_message')
    try:
        list_of_economy_keywords = []
        if re.compile('|'.join(ECONOMY_KEYS), re.IGNORECASE).search(message[TWEET_KEY]):
            list_of_economy_keywords = re.compile('|'.join(ECONOMY_KEYS), re.IGNORECASE).findall(message[TWEET_KEY])
        else:
            LOGGER.info(f"MESSAGE: Data is not found")
        message[ECONOMY_TRENDING_KEYWORD_KEY] = list_of_economy_keywords
        updated_message = ti.xcom_push(key='updated_message', value=message)
        print(message)
    except Exception as e:
        LOGGER.error(f"ERROR:{e} ")




def last_updated_messages(ti):
    message = ti.xcom_pull(task_ids ='get_tweets_with_trending_economy_keywords',key ='updated_message')
    # list_tweets.append(message)
    updated_message = ti.xcom_push(key='updated_message', value=message)
    print(message)



def insert_preprocessed_data(ti):

    message = ti.xcom_pull(task_ids ='last_updated_messages',key ='updated_message')
    try:
        tweet_raw_data = db[COLL_OF_RAW_DATA]
        print("message is ", message)
        if message:
            message[TWEET_KEY] = encrypt_string(message[TWEET_KEY])
            tweet_raw_data.insert_one(message)
            print("he;llo", message)
        else:
            LOGGER.info(f"Message:data is not in proper format {message}")
            print("hello")
    except Exception as e:
        LOGGER.error(f"Error:{e}")
        print('error',e)


dag = DAG('consumer4_dag',
          description='Python DAG',
          schedule_interval='*/5 * * * *',
          start_date=datetime(2018, 11, 1),
          catchup=False)



mess = {'_id': '1526774199133409281', 'tweet': '@DarylTractor Oh you beat me to it! Not a good look smiling while PM is talking about deaths! Attention span of a gnat has #StephAsher', 'country': 'Australia', 'created_at': '2022-05-18T03:58:29.000Z'}

start = EmptyOperator(task_id='start', dag=dag)

country_code = PythonOperator(task_id = 'get_country_code',python_callable = get_country_code,op_kwargs={'message':mess})
covid_keywords = PythonOperator(task_id ='get_covid_keywords',python_callable = get_covid_keywords)
donation_amount = PythonOperator(task_id = 'get_donation_amount',python_callable = get_donation_amount)
donation_currency = PythonOperator(task_id = 'get_donation_currency',python_callable = get_donation_currency)
donation_keywords = PythonOperator(task_id = 'get_donation_keywords',python_callable = get_donation_keywords)
prevention_keywords = PythonOperator(task_id = 'get_prevention_keywords',python_callable = get_prevention_keywords)
who_keywords = PythonOperator(task_id = 'get_who_keywords',python_callable = get_who_keywords)
covid_trends = PythonOperator(task_id ='get_tweets_with_trending_covid_keywords',python_callable = get_tweets_with_trending_covid_keywords)
economy_trends = PythonOperator(task_id ='get_tweets_with_trending_economy_keywords',python_callable = get_tweets_with_trending_economy_keywords)
updated_msg = PythonOperator(task_id = 'last_updated_messages',python_callable = last_updated_messages)
insert_mongo = PythonOperator(task_id ='insert_mongo',python_callable = insert_preprocessed_data)
end = EmptyOperator(task_id='end', dag=dag, trigger_rule=TriggerRule.ONE_SUCCESS)


start  >> country_code >> covid_keywords >> donation_amount >> donation_currency >> donation_keywords >> prevention_keywords >> who_keywords >> covid_trends >> economy_trends >> updated_msg >> insert_mongo >> end