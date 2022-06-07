print("hello consumer1")
import logging
import sys
import json
from kafka import KafkaConsumer
print("import in consumer ")
from src.common.variable_files import DATABASE_TWEET_NEW_DB, ID_KEY, COLL_OF_RAW_DATA, TWEET_KEY, BATCH_SIZE, \
    COLL_OF_TWEET_PER_COUNTRY_ON_DAILY_BASIS, TOPIC1, BOOTSTRAP_SERVER, GROUP_ID
from src.mongodb.mongo_data_connector import mongodb_connection
from src.pub_sub.data_analytics.overall_tweets_per_country import overall_tweets_country_wise
from src.pub_sub.data_analytics.top_100_words_overall import analysis_top_100_words
from src.pub_sub.data_analytics.top_10_precautions import analysis_top_10_preventions
from src.pub_sub.data_analytics.total_number_of_donation import analysis_of_total_number_of_donation
from src.pub_sub.data_analytics.total_tweets_on_trends import analysis_overall_tweets_based_on_trends
from src.pub_sub.data_analytics.total_tweets_on_trends_per_day import analysis_overall_tweets_based_on_trends_per_day
from src.pub_sub.data_analytics.total_tweets_per_country_on_daily_basis import analysis_total_tweet_per_country
from src.pub_sub.data_extract.trending_keywords_extractor import get_tweets_with_trending_covid_keywords, \
    get_tweets_with_trending_economy_keywords
from src.mongodb.insert_data_in_mongo import insert_preprocessed_data
from src.pub_sub.data_extract.country_code_extractor import get_country_code
from src.pub_sub.data_extract.donation_data_extractor import get_donation_amount, get_donation_currency, \
    get_donation_keywords
from src.pub_sub.data_extract.preventive_keywords_extractor import get_prevention_keywords, get_who_keywords
from src.pub_sub.data_extract.covid_keywords_extractor import get_covid_keywords
from src.pub_sub.data_extract.tweet_keywords_extractor import get_tweet_keywords


LOGGER = logging.getLogger(__name__)
print("after import in consumer ")
"""
 fetch the data from topic using consumer and  preprocess that data with for different collection before
 insertertion in collection
 :params
  my_consumer = initialize the kafka consumer
  message = access the data one by one from my_consumer
"""
print("consumer1 ",TOPIC1,BOOTSTRAP_SERVER,GROUP_ID)
my_consumer = KafkaConsumer(
    TOPIC1,
    bootstrap_servers=[BOOTSTRAP_SERVER],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id=GROUP_ID,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)
print("after consumer")
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

   if TOPIC1 in server_topics:
       print("topic present in kafka")


check_partition_in_topic()
print(my_consumer)
#
print("consumer satrt 1 outside",my_consumer)

list_tweet_info = []
print("consumer satrt 1",my_consumer,type(my_consumer))
for msgs in my_consumer:
    print("inside consumer loop")
    msg = msgs.value
    print("msg",msg)
    list_tweet_info.append(msg)
    print(len(list_tweet_info))
    try:
        if len(list_tweet_info) >= BATCH_SIZE:
            print("list_tweet_info")
            list_updated_msg = []
            for message in list_tweet_info:
                if 'RT @' not in message[TWEET_KEY] and db[COLL_OF_RAW_DATA].count_documents(
                        {"_id": message[ID_KEY]}) == 0:
                    # extraction part
                    updated_msg = get_country_code(message)
                    print("get_countrycode")
                    updated_msg = get_covid_keywords(updated_msg)
                    updated_msg = get_tweet_keywords(updated_msg)
                    updated_msg = get_prevention_keywords(updated_msg)
                    updated_msg = get_who_keywords(updated_msg)
                    updated_msg = get_donation_amount(updated_msg)
                    updated_msg = get_donation_currency(updated_msg)
                    updated_msg = get_donation_keywords(updated_msg)
                    updated_msg = get_tweets_with_trending_covid_keywords(updated_msg)
                    print("get_tredning")
                    updated_msg = get_tweets_with_trending_economy_keywords(updated_msg)
                    list_updated_msg.append(updated_msg)
            print("updated mag", list_updated_msg)
            insert_preprocessed_data(list_updated_msg, db)
            list_tweet_info.clear()
            # now analytics start
            for message in list_updated_msg:
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
            list_updated_msg.clear()

            print("batch end consumer waiting for new data")
    except Exception as e:
        LOGGER.error(f"ERROR:{e}")
        pass
    # my_consumer.close()

# # ======================================================================================================================
