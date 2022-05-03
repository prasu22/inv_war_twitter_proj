import logging
import sys
import json
from kafka import KafkaConsumer
from src.mongodb.mongo_data_connector import mongodb_connection
from src.pub_sub.data_analytics.overall_tweets_per_country import overall_tweets_country_wise
from src.pub_sub.data_analytics.top_100_words_overall import analysis_top_100_words
from src.pub_sub.data_analytics.top_10_precautions import analysis_top_10_preventions
from src.pub_sub.data_analytics.total_number_of_donation import analysis_of_total_number_of_donation
from src.pub_sub.data_analytics.total_tweets_on_trends import analysis_overall_tweets_based_on_trends
from src.pub_sub.data_analytics.total_tweets_on_trends_per_day import analysis_overall_tweets_based_on_trends_per_day
from src.pub_sub.data_analytics.total_tweets_per_country_on_daily_basis import analysis_total_tweet_per_country
from src.pub_sub.data_extract.extract_trending_words import get_tweets_with_trending_covid_keywords, \
    get_tweets_with_trending_economy_keywords
from src.mongodb.insert_data_in_mongo import insert_preprocessed_data
from src.pub_sub.data_extract.extract_country_code import get_country_code
from src.pub_sub.data_extract.extract_donation_amount_and_currency import get_donation_amount, get_donation_currency, \
    get_donation_keywords
from src.pub_sub.data_extract.extract_prevention_keywords import get_prevention_keywords, get_who_keywords
from src.pub_sub.data_extract.extract_covid_keywords import get_covid_keywords
LOGGER = logging.getLogger(__name__)
LOGGER.error("this is error message")
"""
 fetch the data from topic using consumer and  preprocess that data with for different collection before 
 insertertion in collection
 :params
  my_consumer = initialize the kafka consumer
  message = access the data one by one from my_consumer
"""

my_consumer = KafkaConsumer(
    'sendingdata',
    bootstrap_servers=['localhost : 9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

try:
    conn = mongodb_connection()
    db = conn['tweet_new_db']
except Exception as e:
    LOGGER.error(F"ERROR:Connection faild {e}")
    sys.exit()

print(my_consumer)

for message in my_consumer:
    message = message.value
    print(message)
    try:
        if 'RT @' not in message['tweet'] and db['tweet_extract_data'].count_documents({"_id": message['_id']}) == 0:
            # extraction part
            updated_msg = get_country_code(message)
            updated_msg = get_covid_keywords(updated_msg)
            updated_msg = get_prevention_keywords(updated_msg)
            updated_msg = get_who_keywords(updated_msg)
            updated_msg = get_donation_amount(updated_msg)
            updated_msg = get_donation_currency(updated_msg)
            updated_msg = get_donation_keywords(updated_msg)
            updated_msg = get_tweets_with_trending_covid_keywords(updated_msg)
            updated_msg = get_tweets_with_trending_economy_keywords(updated_msg)
            insert_preprocessed_data(updated_msg, db)

            # now analytics start
            print("updated mag", updated_msg)
            message = updated_msg
            overall_tweets_country_wise(message, db)
            # query2
            analysis_total_tweet_per_country(message, db)
            # query3
            analysis_top_100_words(message, db)
            # query5
            analysis_top_10_preventions(message, db)
            # query6
            analysis_of_total_number_of_donation(message, db)
            # query 7
            analysis_overall_tweets_based_on_trends_per_day(message, db)
            # query8
            analysis_overall_tweets_based_on_trends(message, db)

    except Exception as e:
        LOGGER.error(f"ERROR:{e}")
        pass
# ======================================================================================================================
