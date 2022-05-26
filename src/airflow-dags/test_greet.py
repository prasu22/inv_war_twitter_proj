import logging
from datetime import datetime

from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from src.mongodb.insert_data_in_mongo import insert_preprocessed_data
from src.mongodb.mongo_data_connector import mongodb_connection
from src.pub_sub.data_analytics.overall_tweets_per_country import  updated_list_total_tweets
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
from src.pub_sub.data_extract.tweet_keyword_extractor import parse_tweet_keywords
from src.pub_sub.data_extract.preventive_keywords_extractor import parse_prevention_keywords, parse_who_keywords
from src.pub_sub.data_extract.trending_keywords_extractor import parse_trending_covid_keywords, \
    parse_trending_economy_keywords

mongo_conn = mongodb_connection()
db_name = "tweet_db"
db = mongo_conn[db_name]
coll = db["test_airflow"]
coll_name = db['metadata table']



LOGGER = logging.getLogger(__name__)

def duplicate_country(ti):
    li = []
    mess1 = {'_id': '142226724199133409281',
             'tweet': '@DarylTractor Oh you beat @WHO me to it! corona donation Not a good $ 2332 sanitiser look unemployment smiling while PM is mask talking about deaths! Attention span of a gnat has #StephAsher',
             'country': 'Australia', 'created_at': '2022-05-18 03:58:29'}

    mess2 = {'_id': '23223376199537409281',
             'tweet': '@Stanneel Oh you beat me @WHO to it! Not a good look donation smiling $2222 while PM is talking mask corona about deaths! Attention span of a gnat has #StephAsher',
             'country': 'China', 'created_at': '2022-05-19 03:58:29'}
    mess3 = {'_id': '82222356199537409281',
             'tweet': '@aditya Oh you beat me to it! @WHO Not a good look smiling donation $123 while sanitiser corona PM is unemployment talking about deaths! Attention span of a gnat has #StephAsher',
             'country': 'India', 'created_at': '2022-05-18 03:58:29'}

    li.append(mess1)
    li.append(mess2)
    li.append(mess3)
    li_message = parse_country_codes(li)
    message_list = ti.xcom_push(key='message_list', value=li_message)
    print('hello ,',message_list)


def collection_for_validation(ti):

    messages = ti.xcom_pull(task_ids='get_country_code', key='message_list')

    start_datetime = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
    list_ids = []
    for message in messages:
        list_ids.append(message['_id'])

    date_time_list_id = {}
    date_time_list_id['start_datetime'] = start_datetime
    date_time_list_id['record_ids']=list_ids

    ti.xcom_push(key='datetime_record', value=date_time_list_id)
    coll_name.insert_one({'start_datetime':start_datetime,'record_ids':list_ids})
    print('inserted')

def duplicate_covid_keywords(ti):

    message = ti.xcom_pull(task_ids='get_country_code', key='message_list')
    li_message = parse_covid_keywords(message)
    updated_message_list = ti.xcom_push(key='message_list', value=li_message)
    print(li_message)





def duplicate_donation_data(ti):

    message = ti.xcom_pull(task_ids='get_covid_keywords', key='message_list')
    donation_amount = parse_donation_amount(message)
    donation_currency = parse_donation_currency(donation_amount)
    donation_keywords = parse_donation_keywords(donation_currency)
    li_message = donation_keywords
    updated_message_list = ti.xcom_push(key='message_list', value=li_message)
    print(li_message)


def duplicate_preventive_data(ti):

    message = ti.xcom_pull(task_ids='donation_data', key='message_list')
    preventive_keywords = parse_prevention_keywords(message)
    who_keywords = parse_who_keywords(preventive_keywords)
    li_message = who_keywords
    updated_message_list = ti.xcom_push(key='message_list', value=li_message)
    print(li_message)

def duplicate_trend_data(ti):

    message = ti.xcom_pull(task_ids='prevention_data', key='message_list')
    covid_trend = parse_trending_covid_keywords(message)
    economy_trend = parse_trending_economy_keywords(covid_trend)
    li_message = economy_trend
    updated_message_list = ti.xcom_push(key='message_list', value=li_message)
    print(li_message)


def duplicate_tweet_keywords(ti):

    message = ti.xcom_pull(task_ids='trend_data', key='message_list')
    li_message = parse_tweet_keywords(message)
    updated_message_list = ti.xcom_push(key='message_list', value=li_message)
    print(li_message)

def insert_mongo(ti):


    message = ti.xcom_pull(task_ids='keyword_data',key='message_list')
    print(db)
    print(coll)
    insert_preprocessed_data(message,db)
    print('bye')



### now analytics part
#########################################################################

def total_tweets(ti) :

    # {'IN 05 2022': 2, 'GB 05 2022': 1}

    message = ti.xcom_pull(task_ids='keyword_data',key='message_list')
    date_time_list = ti.xcom_pull(task_ids = 'make_collection', key = 'datetime_record')
    start_datetime = date_time_list['start_datetime']
    li_message = updated_list_total_tweets(message,db)

    print(li_message)

    batch_list = list(coll_name.find().sort('start_datetime',-1))
    if len(batch_list) == 1:
        before_batch = batch_list[0]
    else:
        before_batch = batch_list[1]

    print('before start time ', before_batch['start_datetime'])
    print('abhi wala',start_datetime)

    if before_batch['start_datetime'] == start_datetime:
        print('1st')
        coll_name.update_one({'start_datetime':start_datetime},{'$set':{'total_tweets_before':'Null','total_tweets_after':li_message}})
    else:
        temp_dict = before_batch['total_tweets_after']
        print('purana')
        for key,value in temp_dict.items():
            if key not in li_message.keys():
                li_message[key] = value
            else:
                li_message[key] += value

        coll_name.update_one({'start_datetime': start_datetime},
                             {'$set': {'total_tweets_before': temp_dict, 'total_tweets_after': li_message}})


def tweets_daily_basis(ti) :

    # {'IN 2022-05-18': 2, 'GB 2022-05-19': 1}

    message = ti.xcom_pull(task_ids='keyword_data',key='message_list')
    li_message = updated_list_daily_tweets(message,db)
    print('hello',li_message)

    date_time_list = ti.xcom_pull(task_ids='make_collection', key='datetime_record')
    start_datetime = date_time_list['start_datetime']

    batch_list = list(coll_name.find().sort('start_datetime', -1))
    if len(batch_list) == 1:
        before_batch = batch_list[0]
    else:
        before_batch = batch_list[1]

    print('before start time ', before_batch['start_datetime'])
    print('abhi wala', start_datetime)

    if before_batch['start_datetime'] == start_datetime:
        print('1st')
        coll_name.update_one({'start_datetime': start_datetime},
                             {'$set': {'tweet_daily_before': 'Null', 'tweet_daily_after': li_message}})

    else:
        temp_dict = before_batch['tweet_daily_after']
        print('purana')
        for key,value in temp_dict.items():
            if key not in li_message.keys():
                li_message[key] = value
            else:
                li_message[key] += value

        coll_name.update_one({'start_datetime': start_datetime},
                             {'$set': {'tweet_daily_before': temp_dict, 'tweet_daily_after': li_message}})


################################################################################

def get_top_preventions(ti):

    # {'IN': [{'Sanitiser': 2}, {'Mask': 1}], 'GB': [{'Mask': 1}]}

    message = ti.xcom_pull(task_ids='keyword_data', key='message_list')
    new_dict = updated_list_top_10_precautions(message, db)
    print(new_dict)

    date_time_list = ti.xcom_pull(task_ids='make_collection', key='datetime_record')
    start_datetime = date_time_list['start_datetime']

    batch_list = list(coll_name.find().sort('start_datetime', -1))
    if len(batch_list) == 1:
        before_batch = batch_list[0]
    else:
        before_batch = batch_list[1]

    print('before start time ', before_batch['start_datetime'])
    print('abhi wala', start_datetime)

    if before_batch['start_datetime'] == start_datetime:
        print('1st')
        coll_name.update_one({'start_datetime': start_datetime},
                             {'$set': {'prevention_before': 'Null', 'prevention_after': new_dict}})

    else:

        # {'IN': [{'Sanitiser': 2}, {'Mask': 1}], 'GB': [{'Mask': 1}]}

        temp_dict = before_batch['prevention_after']

        for key, value in temp_dict.items():
            if key not in new_dict.keys():
                new_dict[key] = value

            else:
                updated_dict = {}
                old_list = temp_dict[key]
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


        coll_name.update_one({'start_datetime': start_datetime},
                             {'$set': {'prevention_before': temp_dict, 'prevention_after': new_dict}})


def get_trend_data(ti):

    message = ti.xcom_pull(task_ids='keyword_data', key='message_list')
    print(message)
    trend_list = updated_covid_list_total_tweets(message, db)
    print('covid', trend_list[0])
    print('economy',trend_list[1])

    covid_trend_list = trend_list[0]
    economy_trend_list = trend_list[1]



    ###################covid############################################

    date_time_list = ti.xcom_pull(task_ids='make_collection', key='datetime_record')
    start_datetime = date_time_list['start_datetime']

    batch_list = list(coll_name.find().sort('start_datetime', -1))
    if len(batch_list) == 1:
        before_batch = batch_list[0]
    else:
        before_batch = batch_list[1]

    print('before start time ', before_batch['start_datetime'])
    print('abhi wala', start_datetime)

    if before_batch['start_datetime'] == start_datetime:
        print('1st')
        coll_name.update_one({'start_datetime': start_datetime},
                             {'$set': {'covid_trend_before': 'Null', 'covid_trend_after': covid_trend_list}})

    else:
        temp_dict = before_batch['covid_trend_after']
        print('purana')
        for key, value in temp_dict.items():
            if key not in covid_trend_list.keys():
                covid_trend_list[key] = value
            else:
                covid_trend_list[key] += value

        coll_name.update_one({'start_datetime': start_datetime},
                             {'$set': {'donation_before': temp_dict, 'donation_after': covid_trend_list}})

    #################end covid ##################################################

    ######################economy ###############################################

    date_time_list = ti.xcom_pull(task_ids='make_collection', key='datetime_record')
    start_datetime = date_time_list['start_datetime']

    batch_list = list(coll_name.find().sort('start_datetime', -1))
    if len(batch_list) == 1:
        before_batch = batch_list[0]
    else:
        before_batch = batch_list[1]

    print('before start time ', before_batch['start_datetime'])
    print('abhi wala', start_datetime)

    if before_batch['start_datetime'] == start_datetime:
        print('1st')
        coll_name.update_one({'start_datetime': start_datetime},
                             {'$set': {'covid_trend_before': 'Null', 'covid_trend_after': covid_trend_list}})

    else:
        temp_dict = before_batch['covid_trend_after']
        print('purana')
        for key, value in temp_dict.items():
            if key not in covid_trend_list.keys():
                covid_trend_list[key] = value
            else:
                covid_trend_list[key] += value

        coll_name.update_one({'start_datetime': start_datetime},
                             {'$set': {'donation_before': temp_dict, 'donation_after': covid_trend_list}})








def get_top_words(ti):

    message = ti.xcom_pull(task_ids='keyword_data', key='message_list')
    new_dict = updated_list_top_words(message,db)
    print(new_dict)

    date_time_list = ti.xcom_pull(task_ids='make_collection', key='datetime_record')
    start_datetime = date_time_list['start_datetime']

    batch_list = list(coll_name.find().sort('start_datetime', -1))
    if len(batch_list) == 1:
        before_batch = batch_list[0]
    else:
        before_batch = batch_list[1]

    print('before start time ', before_batch['start_datetime'])
    print('abhi wala', start_datetime)

    if before_batch['start_datetime'] == start_datetime:
        print('1st')
        coll_name.update_one({'start_datetime': start_datetime},
                             {'$set': {'top_words_before': 'Null', 'top_words_after': new_dict}})

    else:

        temp_dict = before_batch['top_words_after']

        for key, value in temp_dict.items():
            if key not in new_dict.keys():
                new_dict[key] = value

            else:
                updated_dict = {}
                old_list = temp_dict[key]
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

        coll_name.update_one({'start_datetime': start_datetime},
                             {'$set': {'top_words_before': temp_dict, 'top_words_after': new_dict}})


def get_donation_data(ti):
    message = ti.xcom_pull(task_ids='keyword_data', key='message_list')
    li_message = updated_donation_list_total_tweets(message,db)
    print(li_message)

    date_time_list = ti.xcom_pull(task_ids='make_collection', key='datetime_record')
    start_datetime = date_time_list['start_datetime']

    batch_list = list(coll_name.find().sort('start_datetime', -1))
    if len(batch_list) == 1:
        before_batch = batch_list[0]
    else:
        before_batch = batch_list[1]

    print('before start time ', before_batch['start_datetime'])
    print('abhi wala', start_datetime)

    if before_batch['start_datetime'] == start_datetime:
        print('1st')
        coll_name.update_one({'start_datetime': start_datetime},
                             {'$set': {'donation_before': 'Null', 'donation_after': li_message}})

    else:
        temp_dict = before_batch['donation_after']
        print('purana')
        for key, value in temp_dict.items():
            if key not in li_message.keys():
                li_message[key] = value
            else:
                li_message[key] += value

        coll_name.update_one({'start_datetime': start_datetime},
                             {'$set': {'donation_before': temp_dict, 'donation_after': li_message}})


def get_trend_daily(ti):
    message = ti.xcom_pull(task_ids='keyword_data', key='message_list')
    trend_list = updated_trend_list_total_tweets(message,db)
    print(trend_list)

#############################################################################################

dag = DAG('consumer555_dag',
          description='Python DAG',
          schedule_interval='*/5 * * * *',
          start_date=datetime(2018, 11, 1),
          catchup=False)


start = EmptyOperator(task_id='start', dag=dag)

### extractor

############################################################################

country_code = PythonOperator(task_id='get_country_code', python_callable=duplicate_country)
covid_keywords = PythonOperator(task_id='get_covid_keywords', python_callable=duplicate_covid_keywords)
donation_data = PythonOperator(task_id= 'donation_data',python_callable = duplicate_donation_data)
prevention_data = PythonOperator(task_id = 'prevention_data',python_callable = duplicate_preventive_data)
trend_data = PythonOperator(task_id = 'trend_data',python_callable = duplicate_trend_data)
keywords_data = PythonOperator(task_id ='keyword_data',python_callable =duplicate_tweet_keywords)
#############################################################################

insert_mongo = PythonOperator(task_id='insert_mongo',python_callable= insert_mongo)

#############################################################################
total_tweets = PythonOperator(task_id ='total_tweets',python_callable = total_tweets)
tweets_daily_basis = PythonOperator(task_id ='tweets_daily',python_callable = tweets_daily_basis)
top_preventions = PythonOperator(task_id = 'top_preventions',python_callable = get_top_preventions)
trending_data = PythonOperator(task_id = 'trending_data',python_callable = get_trend_data)
top_words = PythonOperator(task_id = 'top_words',python_callable = get_top_words)
donation_coll = PythonOperator(task_id ='donation_coll',python_callable = get_donation_data)
trend_data_daily = PythonOperator(task_id = 'trend_data_daily',python_callable = get_trend_daily)

# ###################################################

make_collection = PythonOperator(task_id = 'make_collection',python_callable = collection_for_validation)

end = EmptyOperator(task_id='end', dag=dag, trigger_rule=TriggerRule.ONE_SUCCESS)

start >> country_code >> [covid_keywords, make_collection] >> donation_data >> prevention_data >> trend_data >> keywords_data >>insert_mongo>> [total_tweets , tweets_daily_basis,top_preventions,trending_data,top_words,donation_coll,trend_data_daily] >> end