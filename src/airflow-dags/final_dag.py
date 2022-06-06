import json
import logging
from datetime import datetime

from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from kafka import KafkaConsumer

from src.common.variable_files import GROUP_ID, TOPIC1, BOOTSTRAP_SERVER, TOTAL_TWEETS_BEFORE, TOTAL_TWEETS_AFTER, \
    PREVENTION_BEFORE, PREVENTION_AFTER
from src.mongodb.insert_data_in_mongo import insert_preprocessed_data
from src.mongodb.mongo_data_connector import mongodb_connection
from src.pub_sub.data_analytics.overall_tweets_per_country import updated_list_total_tweets
from src.pub_sub.data_analytics.top_100_words_overall import updated_list_top_words
from src.pub_sub.data_analytics.top_10_precautions import updated_list_top_10_precautions
from src.pub_sub.data_analytics.total_number_of_donation import updated_donation_list_total_tweets
from src.pub_sub.data_analytics.total_tweets_on_trends import updated_covid_list_total_tweets
from src.pub_sub.data_analytics.total_tweets_on_trends_per_day import updated_trend_list_total_tweets
from src.pub_sub.data_analytics.total_tweets_per_country_on_daily_basis import updated_list_daily_tweets
from src.pub_sub.data_extract.country_code_extractor import parse_country_codes
from src.pub_sub.data_extract.covid_keywords_extractor import parse_covid_keywords
from src.pub_sub.data_extract.donation_data_extractor import parse_donation_amount, parse_donation_currency, \
    parse_donation_keywords
from src.pub_sub.data_extract.tweet_keywords_extractor import parse_tweet_keywords
from src.pub_sub.data_extract.preventive_keywords_extractor import parse_prevention_keywords, parse_who_keywords
from src.pub_sub.data_extract.trending_keywords_extractor import parse_trending_covid_keywords, \
    parse_trending_economy_keywords

from testing import *

mongo_conn = mongodb_connection()
db_name = DATABASE_TWEET_NEW_DB
db = mongo_conn[db_name]
coll = db[COLL_OF_RAW_DATA]
coll_name = db[COLL_METADATA]

LOGGER = logging.getLogger(__name__)


my_consumer = KafkaConsumer(
    TOPIC1,
    bootstrap_servers=[BOOTSTRAP_SERVER],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id=GROUP_ID,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)


def duplicate_country(ti):
    li = []

    for message in my_consumer:
        message = message.value
        li.append(message)
        if len(li) >= 100:
            my_consumer.close()
            break

    # li = [{'_id': '15300957049', 'tweet': 'Wayfinding and wear mask covid unemployment gdp down corona COVID-19 https://t.co/o2K04lkGmV', 'country': 'no country', 'created_at': '2022-06-03 04:29:18' }, {'_id': '158015433873', 'tweet': '@elisled2 And monkeypox most likely covid donation $ 200.', 'country': 'no country', 'created_at': '2022-06-03 04:29:18'}]
    li_message = parse_country_codes(li)
    print('helo',li_message)
    ti.xcom_push(key='message_list', value=li_message)



def collection_for_validation(ti):
    messages = ti.xcom_pull(task_ids='get_country_code', key='message_list')

    start_datetime = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
    list_ids = []
    for message in messages:
        list_ids.append(message['_id'])

    date_time_list_id = {'start_datetime': start_datetime, RECORD_IDS: list_ids}

    ti.xcom_push(key='datetime_record', value=date_time_list_id)
    coll_name.insert_one({'start_datetime': start_datetime, RECORD_IDS: list_ids})
    print('inserted')


def duplicate_covid_keywords(ti):
    message = ti.xcom_pull(task_ids='get_country_code', key='message_list')
    li_message = parse_covid_keywords(message)
    ti.xcom_push(key='message_list', value=li_message)
    print(li_message)


def duplicate_donation_data(ti):
    message = ti.xcom_pull(task_ids='get_covid_keywords', key='message_list')
    donation_amount = parse_donation_amount(message)
    donation_currency = parse_donation_currency(donation_amount)
    donation_keywords = parse_donation_keywords(donation_currency)
    li_message = donation_keywords
    ti.xcom_push(key='message_list', value=li_message)
    print(li_message)


def duplicate_preventive_data(ti):
    message = ti.xcom_pull(task_ids='donation_data', key='message_list')
    preventive_keywords = parse_prevention_keywords(message)
    who_keywords = parse_who_keywords(preventive_keywords)
    li_message = who_keywords
    ti.xcom_push(key='message_list', value=li_message)
    print(li_message)


def duplicate_trend_data(ti):
    message = ti.xcom_pull(task_ids='prevention_data', key='message_list')
    covid_trend = parse_trending_covid_keywords(message)
    economy_trend = parse_trending_economy_keywords(covid_trend)
    li_message = economy_trend
    ti.xcom_push(key='message_list', value=li_message)
    print(li_message)


def duplicate_tweet_keywords(ti):
    message = ti.xcom_pull(task_ids='trend_data', key='message_list')
    li_message = parse_tweet_keywords(message)
    ti.xcom_push(key='message_list', value=li_message)
    print(li_message)


def insert_mongo(ti):
    message = ti.xcom_pull(task_ids='keyword_data', key='message_list')
    try:
        print(coll.count_documents({}), 'before')
        insert_preprocessed_data(message, db)
        print(coll.count_documents({}), 'after')
    except:
        pass


# def helper()

### now analytics part
#########################################################################

def total_tweets(ti):

    message = ti.xcom_pull(task_ids='keyword_data', key='message_list')
    li_message = updated_list_total_tweets(message, db)
    print('bye', li_message)
    ti.xcom_push(key="total_tweets_data", value=li_message)


def tweets_daily_basis(ti):

    message = ti.xcom_pull(task_ids='keyword_data', key='message_list')
    li_message = updated_list_daily_tweets(message, db)
    print('hello', li_message)
    ti.xcom_push(key="tweets_daily_basis", value=li_message)


################################################################################

def get_top_preventions(ti):
    # {'IN': [{'Sanitiser': 2}, {'Mask': 1}], 'GB': [{'Mask': 1}]}

    message = ti.xcom_pull(task_ids='keyword_data', key='message_list')
    new_dict = updated_list_top_10_precautions(message, db)
    print(new_dict)
    ti.xcom_push(key="top_prevention", value = new_dict)


def get_top_words(ti):
    message = ti.xcom_pull(task_ids='keyword_data', key='message_list')
    new_dict = updated_list_top_words(message, db)
    print(new_dict)
    ti.xcom_push(key="get_top_words", value=new_dict)


def get_donation_data(ti):
    message = ti.xcom_pull(task_ids='keyword_data', key='message_list')
    li_message = updated_donation_list_total_tweets(message, db)
    print(li_message)
    ti.xcom_push(key="donation_tweets", value=li_message)


def get_trend_data(ti):
    message = ti.xcom_pull(task_ids='keyword_data', key='message_list')
    print(message)
    trend_list = updated_covid_list_total_tweets(message, db)
    print('covid', trend_list[0])
    print('economy', trend_list[1])


def get_trend_daily(ti):
    message = ti.xcom_pull(task_ids='keyword_data', key='message_list')
    trend_list = updated_trend_list_total_tweets(message, db)
    print('trend_list',trend_list)





############################################################
### test validation

def test_tweets_daily_basis(ti):
    messages = ti.xcom_pull(task_ids='get_country_code', key='message_list')

    li_ids = []

    for i in messages:
        li_ids.append(i[ID_KEY])

    dict1 = test_tweets_daily_basis_metadata(li_ids)
    dict2 = test_raw_daily_basis(li_ids)
    print(dict1)
    assert (dict1 == dict2)
    if dict1 == dict2:
        print("Yes")
    else:
        print("Noo")


def test_top_100(ti):
    messages = ti.xcom_pull(task_ids='get_country_code', key='message_list')

    li_ids = []

    for i in messages:
        li_ids.append(i[ID_KEY])

    dict1 = test_top_100_metadata(li_ids)
    dict2 = test_top_100_words_from_raw_data(li_ids)
    assert dict1 == dict2
    if dict1 == dict2:
        print("Yes")
    else:
        print("Noo")


def test_total_tweets(ti):
    messages = ti.xcom_pull(task_ids='get_country_code', key='message_list')

    li_ids = []

    for i in messages:
        li_ids.append(i[ID_KEY])

    dict1 = test_top_100_country_basis_metadata(li_ids)
    dict2 = test_top_100_words_from_raw_data_country_basis(li_ids)
    assert dict1 == dict2
    if dict1 == dict2:
        print("Yes")
    else:
        print("Noo")


def test_donation(ti):
    messages = ti.xcom_pull(task_ids='get_country_code', key='message_list')

    li_ids = []

    for i in messages:
        li_ids.append(i[ID_KEY])

    dict1 = test_donation_metadata(li_ids)
    dict2 = test_donation_raw_data(li_ids)
    assert(dict1 == dict2)
    if dict1 == dict2:
        print("Yes")
    else:
        print("No")

################################################################################


def mid(ti):

    date_time_list = ti.xcom_pull(task_ids='make_collection', key='datetime_record')
    start_datetime = date_time_list['start_datetime']
    li_message = ti.xcom_pull(task_ids='total_tweets', key='total_tweets_data')
    li_message_1 = ti.xcom_pull(task_ids = 'tweets_daily' , key ='tweets_daily_basis')
    new_dict = ti.xcom_pull(task_ids = 'top_preventions', key = 'top_prevention')
    new_dict_1 =ti.xcom_pull(task_ids = 'top_words',key='get_top_words')
    li_message_2 =ti.xcom_pull(task_ids ='donation_coll',key ='donation_tweets')



    batch_list = list(coll_name.find().sort('start_datetime', -1))
    if len(batch_list) == 1:
        before_batch = batch_list[0]
    else:
        before_batch = batch_list[1]

    print('before start time ', before_batch['start_datetime'])
    print('abhi wala', start_datetime)

    if before_batch['start_datetime'] == start_datetime:

        coll_name.update_one({'start_datetime': start_datetime},
                              {'$set': {TOTAL_TWEETS_BEFORE: {}, TOTAL_TWEETS_AFTER : li_message,TWEET_DAILY_BEFORE: {}, TWEET_DAILY_AFTER: li_message_1,PREVENTION_BEFORE: {}, PREVENTION_AFTER : new_dict,
                                        TOP_WORDS_BEFORE: {}, TOP_WORDS_AFTER: new_dict_1,DONATION_BEFORE: {}, DONATION_AFTER: li_message_2}})


    else:

        temp_dict = before_batch[TOTAL_TWEETS_AFTER]
        temp_dict_1 = before_batch[TWEET_DAILY_AFTER]
        temp_dict_2 = before_batch[PREVENTION_AFTER]
        temp_dict_3 = before_batch[TOP_WORDS_AFTER]
        temp_dict_4 = before_batch[DONATION_AFTER]


        ## total tweets
        for key, value in temp_dict.items():
            if key not in li_message.keys():
                li_message[key] = value
            else:
                li_message[key] += value

        ## total tweets daily
        for key, value in temp_dict_1.items():
            if key not in li_message_1.keys():
                li_message_1[key] = value
            else:
                li_message_1[key] += value


        ### preventions
        for key, value in temp_dict_2.items():
            if key not in new_dict.keys():
                new_dict[key] = value

            else:
                updated_dict = {}
                old_list = temp_dict_2[key]
                for dictionary in old_list:
                    for key1, val1 in dictionary.items():
                        updated_dict[key1] = val1

                new_list = new_dict[key]
                for dictionary in new_list:
                    for key1, val1 in dictionary.items():
                        updated_dict[key1] = updated_dict.get(key1, 0) + val1

                new_updated_list = []
                for key1, val1 in updated_dict.items():
                    new_updated_list.append({key1: val1})

                new_dict[key] = new_updated_list

        ## top words
        for key, value in temp_dict_3.items():
            if key not in new_dict_1.keys():
                new_dict_1[key] = value

            else:
                updated_dict = {}
                old_list = temp_dict_3[key]
                for dictionary in old_list:
                    for key1, val1 in dictionary.items():
                        updated_dict[key1] = val1

                new_list = new_dict_1[key]
                for dictionary in new_list:
                    for key1, val1 in dictionary.items():
                        updated_dict[key1] = updated_dict.get(key1, 0) + val1

                new_updated_list = []
                for key1, val1 in updated_dict.items():
                    new_updated_list.append({key1: val1})

                new_dict_1[key] = new_updated_list

        ### donation

        for key, value in temp_dict_4.items():
            if key not in li_message_2.keys():
                li_message_2[key] = value
            else:
                li_message_2[key] += value

        coll_name.update_one({'start_datetime':start_datetime},
                              {'$set': {TOTAL_TWEETS_BEFORE: temp_dict, TOTAL_TWEETS_AFTER: li_message ,TWEET_DAILY_BEFORE: temp_dict_1, TWEET_DAILY_AFTER: li_message_1,
                                        PREVENTION_BEFORE: temp_dict_2, PREVENTION_AFTER: new_dict,TOP_WORDS_BEFORE: temp_dict_3, TOP_WORDS_AFTER: new_dict_1,DONATION_BEFORE: temp_dict_4, DONATION_AFTER: li_message_2}})



        print('updated_successfully')


#############################################################################################

dag = DAG('consumer777_dag',
          description='Python DAG',
          schedule_interval='@daily',
          start_date=datetime(2018, 11, 1),
          catchup=False)

start = EmptyOperator(task_id='start', dag=dag)

### extractor

############################################################################

country_code = PythonOperator(task_id='get_country_code', python_callable=duplicate_country)
covid_keywords = PythonOperator(task_id='get_covid_keywords', python_callable=duplicate_covid_keywords)
donation_data = PythonOperator(task_id='donation_data', python_callable=duplicate_donation_data)
prevention_data = PythonOperator(task_id='prevention_data', python_callable=duplicate_preventive_data)
trend_data = PythonOperator(task_id='trend_data', python_callable=duplicate_trend_data)
keywords_data = PythonOperator(task_id='keyword_data', python_callable=duplicate_tweet_keywords)
#############################################################################

insert_mongo = PythonOperator(task_id='insert_mongo', python_callable=insert_mongo)

#############################################################################
total_tweets = PythonOperator(task_id='total_tweets', python_callable=total_tweets)
tweets_daily_basis = PythonOperator(task_id='tweets_daily', python_callable=tweets_daily_basis)
top_preventions = PythonOperator(task_id='top_preventions', python_callable=get_top_preventions)
top_words = PythonOperator(task_id='top_words', python_callable=get_top_words)
donation_coll = PythonOperator(task_id='donation_coll', python_callable=get_donation_data)
trends_tweet = PythonOperator(task_id ='trending_data',python_callable =get_trend_data)
trends_tweet_daily = PythonOperator(task_id ='trending_data_daily',python_callable =get_trend_daily)

# ###################################################

mid = PythonOperator(task_id='mid', python_callable=mid)

test_total_tweets = PythonOperator(task_id='test_total_tweets', python_callable=test_total_tweets)
test_tweets_daily_basis = PythonOperator(task_id='test_tweets_daily_basis', python_callable=test_tweets_daily_basis)
test_top_100 = PythonOperator(task_id='test_top_100', python_callable=test_top_100)
test_donation = PythonOperator(task_id='test_donation', python_callable=test_donation)

# ###################################################

make_collection = PythonOperator(task_id='make_collection', python_callable=collection_for_validation)

end = EmptyOperator(task_id='end', dag=dag, trigger_rule=TriggerRule.ONE_SUCCESS)

start >> country_code >> [covid_keywords,
                          make_collection] >> donation_data >> prevention_data >> trend_data >> keywords_data >> insert_mongo >> [
    total_tweets, tweets_daily_basis, top_preventions, top_words, donation_coll,trends_tweet,trends_tweet_daily] >> mid >> [test_total_tweets, test_tweets_daily_basis, test_top_100, test_donation] >> end
