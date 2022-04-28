import json
from kafka import KafkaConsumer

from mongodb.mongo_data_connector import mongodb_connection
from pub_sub.data_analysis.overall_tweets_per_country import overall_tweets_country_wise
from pub_sub.data_analysis.top_100_words_overall import analysis_top_100_words
from pub_sub.data_analysis.top_10_precautions import analysis_top_10_preventions
from pub_sub.data_analysis.total_number_of_donation import analysis_of_total_number_of_donation
from pub_sub.data_analysis.total_tweets_on_trends import analysis_overall_tweets_based_on_trends
from pub_sub.data_analysis.total_tweets_per_country_on_daily_basis import analysis_total_tweet_per_country
from pub_sub.data_extraction.save_raw_data import get_tweets

"""
 fetch the data from topic using consumer and  preprocess that data with for different collection before insertertion in collection
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

conn = mongodb_connection()
db = conn['tweet_db']

for message in my_consumer:
    message = message.value
    print(message['created_at'])
    try:
        flag = 0
        if 'RT @' not in message['tweet'] and db['tweet_data'].count_documents({'_id':message['_id']})==0:
            # query1
            if overall_tweets_country_wise(message, db):
              flag=1
            # # query2
            if analysis_total_tweet_per_country(message,db):
                flag=1
            # # query3
            if analysis_top_100_words(message,db):
                flag=1
            # # query5
            if analysis_top_10_preventions(message,db):
                flag=1
            # query6
            if analysis_of_total_number_of_donation(message,db):
                flag=1
            # query8
            if analysis_overall_tweets_based_on_trends(message,db):
                flag=1
            if flag==1:
                get_tweets(message,db)
    except Exception as e:
        print(f"error {e}")
        pass
# ======================================================================================================================