# importing required libraries
import json
import logging
import os
import sys
from datetime import datetime
from json import dumps
from kafka import KafkaProducer
from bson.json_util import loads
from src.common.variable_files import DATABASE_TWEET_NEW_DB, TWEET_KEY, ID_KEY, CREATED_AT_KEY, COUNTRY_NAME_KEY, \
    COUNTRY_NAME, TOPIC1, BOOTSTRAP_SERVER, BATCH_SIZE
from src.encryption_and_decryption_data.encrypt_and_decryption_data import decrypt_string


LOGGER = logging.getLogger(__name__)
""""
  creating producer to call tweetcrawler api to fetch data from twitter and insert that in topic by the producer
  :params
  my_producer =  initialize the kafka producer
  crawler_object = creating object of class TweetCrawler
  keywords  = list of keyword based on which we fetch tweet

"""

my_producer = KafkaProducer(
    bootstrap_servers=[BOOTSTRAP_SERVER],
    api_version=(0, 11, 5),
    value_serializer=lambda x: dumps(x).encode('utf-8')
)


def process_historical_data(filename):
    file = open(filename,'r')
    data = json.load(file)
    sorted_data = sorted(data, key=lambda x: datetime.strptime(x['created_at'], "%Y-%m-%d %H:%M:%S"))
    count=0
    start=0
    for document in sorted_data:
        count+=1
        if type(document[TWEET_KEY]) == dict:
            doc = loads(json.dumps(document))
            id = doc[ID_KEY]
            tweet = decrypt_string(doc[TWEET_KEY])
            create_at = doc[CREATED_AT_KEY]
            country = doc[COUNTRY_NAME_KEY]
            if country == "No Country":
                country = COUNTRY_NAME
            my_data = {ID_KEY: id, TWEET_KEY: tweet, COUNTRY_NAME_KEY: country, CREATED_AT_KEY: create_at}
            print("encrypt",my_data)
            my_producer.send(TOPIC1, my_data)
        else:
            id = document[ID_KEY]
            tweet = document[TWEET_KEY]
            create_at = document[CREATED_AT_KEY]
            country = document[COUNTRY_NAME_KEY]
            if country == "No Country":
                country = COUNTRY_NAME
            my_data = {ID_KEY:id,TWEET_KEY:tweet,COUNTRY_NAME_KEY:country,CREATED_AT_KEY:create_at}
            print("non_encrypt", my_data)
            my_producer.send(TOPIC1,my_data)
    file.close()
    print("total_tweet: start,end",start,count)


file_path = "/Users/sauravverma/demo_project/inv_war_twitter_proj/data_files/tweet_raw_data.json"
process_historical_data(file_path)
