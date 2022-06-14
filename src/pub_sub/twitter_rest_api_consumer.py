import logging
import sys
import json
import time

from kafka import KafkaConsumer
from src.common.variable_files import DATABASE_TWEET_NEW_DB, ID_KEY, COLL_OF_RAW_DATA, TWEET_KEY, BATCH_SIZE, \
    COLL_OF_TWEET_PER_COUNTRY_ON_DAILY_BASIS, TOPIC1, BOOTSTRAP_SERVER, GROUP_ID
from src.mongodb.mongo_data_connector import mongodb_connection
from src.pub_sub.data_analytics.overall_tweets_per_country import overall_tweets_country_wise, updated_list_total_tweets
from src.pub_sub.data_analytics.top_100_words_overall import analysis_top_100_words, updated_list_top_words
from src.pub_sub.data_analytics.top_10_precautions import analysis_top_10_preventions, updated_list_top_10_precautions
from src.pub_sub.data_analytics.total_number_of_donation import analysis_of_total_number_of_donation, \
    updated_donation_list_total_tweets
from src.pub_sub.data_analytics.total_tweets_on_trends import analysis_overall_tweets_based_on_trends, \
    updated_covid_list_total_tweets
from src.pub_sub.data_analytics.total_tweets_on_trends_per_day import analysis_overall_tweets_based_on_trends_per_day, \
    updated_trend_list_total_tweets
from src.pub_sub.data_analytics.total_tweets_per_country_on_daily_basis import analysis_total_tweet_per_country, \
    updated_list_daily_tweets
from src.pub_sub.data_extract.trending_keywords_extractor import get_tweets_with_trending_covid_keywords, \
    get_tweets_with_trending_economy_keywords, parse_trending_covid_keywords, parse_trending_economy_keywords
from src.mongodb.insert_data_in_mongo import insert_preprocessed_data
from src.pub_sub.data_extract.country_code_extractor import get_country_code, parse_country_codes
from src.pub_sub.data_extract.donation_data_extractor import get_donation_amount, get_donation_currency, \
    get_donation_keywords, parse_donation_keywords, parse_donation_amount, parse_donation_currency
from src.pub_sub.data_extract.preventive_keywords_extractor import get_prevention_keywords, get_who_keywords, \
    parse_who_keywords, parse_prevention_keywords
from src.pub_sub.data_extract.covid_keywords_extractor import get_covid_keywords, parse_covid_keywords
from src.pub_sub.data_extract.tweet_keywords_extractor import get_tweet_keywords, parse_tweet_keywords

LOGGER = logging.getLogger(__name__)
"""
 fetch the data from topic using consumer and  preprocess that data with for different collection before
 insertertion in collection
 :params
  my_consumer = initialize the kafka consumer
  message = access the data one by one from my_consumer
"""

my_consumer = KafkaConsumer(
    TOPIC1,
    bootstrap_servers=[BOOTSTRAP_SERVER],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id=GROUP_ID,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

try:
    conn = mongodb_connection()
    db = conn[DATABASE_TWEET_NEW_DB]
    LOGGER.info('connection done')

    print("connection", db)
except Exception as e:
    print("error occured")
    LOGGER.error(F"ERROR:Connection faild {e}")
    sys.exit()


def check_partition_in_topic():
    partitions = my_consumer.partitions_for_topic(TOPIC1)
    print(partitions)
    server_topics = my_consumer.topics()


# check_partition_in_topic()

def call_extractor_function(tweet_list):
    updated_tweet_list = parse_country_codes(tweet_list)
    updated_tweet_list = parse_covid_keywords(updated_tweet_list)
    updated_tweet_list = parse_tweet_keywords(updated_tweet_list)
    updated_tweet_list = parse_donation_keywords(updated_tweet_list)
    updated_tweet_list = parse_donation_amount(updated_tweet_list)
    updated_tweet_list = parse_donation_currency(updated_tweet_list)
    updated_tweet_list = parse_who_keywords(updated_tweet_list)
    updated_tweet_list = parse_prevention_keywords(updated_tweet_list)
    updated_tweet_list = parse_trending_covid_keywords(updated_tweet_list)
    updated_tweet_list = parse_trending_economy_keywords(updated_tweet_list)
    return updated_tweet_list


def call_analystic_function(tweet_list, db):
    try:
        print("\nfunction is called in analysis\n")
        updated_list_total_tweets(tweet_list, db)
        updated_list_daily_tweets(tweet_list, db)
        updated_list_top_words(tweet_list, db)
        updated_list_top_10_precautions(tweet_list, db)
        updated_donation_list_total_tweets(tweet_list, db)
        updated_trend_list_total_tweets(tweet_list, db)
        updated_covid_list_total_tweets(tweet_list, db)
        print("\nfunction is called in analysis end\n")
    except Exception as e:
        print("\nnew error ;", {e})


list_tweet_info = []
for msgs in my_consumer:
    msg = msgs.value
    # print("new list started",len(list_tweet_info))
    list_tweet_info.append(msg)
    print("consumer", len(list_tweet_info))
    timeout = time.time() + 60
    count = 0
    try:
        if len(list_tweet_info) >= BATCH_SIZE:
            print(list_tweet_info, len(list_tweet_info))
            # extract data from the raw data
            updated_list_tweet_info = call_extractor_function(list_tweet_info)
            # print("final with all extracted",updated_list_tweet_info,len(updated_list_tweet_info))
            list_tweet_info.clear()
            # insert in mongodb
            insert_preprocessed_data(updated_list_tweet_info, db)
            # now services start
            print("next function is called", len(updated_list_tweet_info))
            if updated_list_tweet_info:
                print("\n no duplicate record is present \n")
                call_analystic_function(updated_list_tweet_info, db)
            print("next function is called by call services")
            updated_list_tweet_info.clear()

            print("check list_clear", updated_list_tweet_info)
            print("check list_2", list_tweet_info)
            if timeout == time.time():
                LOGGER.info(f"Batch size {BATCH_SIZE} and Total_Tweet_count_per_min:{count}")
                count = 0
                timeout = time.time() + 60
    except Exception as e:
        LOGGER.error(f"ERROR:{e}")
        pass

# consumer_processing()

# validating_data()

# # ======================================================================================================================
