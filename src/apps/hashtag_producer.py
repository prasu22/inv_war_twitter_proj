# importing required libraries
from json import dumps
from kafka import KafkaProducer

from src.common.variable_files import BOOTSTRAP_SERVER
from src.pub_sub.producer_call_twitter_api.data_insert_in_producer_topic import search_api_to_insert_data_in_topic
from src.twitter import tweet_crawler as tc
from src.twitter.twitter_api_connector import connect_with_twitter

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

# ======================================================================================================================
# static keyword list


keywords =  ["#coronavirus", "#covid", "#pandemic", "#death", "#mask", "@DonationCovid", "@19-donation", "@COVID_WHO","@COVID_DRUGS", "#donation ", "#WHO ", "#CDC ", "@WHO ", "@CDC ", "@Health ", "@mask ",
             "#vaccination ", "#sanitiser", "#coronavirus","#precaution", "#prevention", "#vaccines", "@plasmahelp", "@DonationIND"]


api = connect_with_twitter()
crawler_object = tc.TweetCrawler(tc.config, api)

for keyword in keywords:
   search_api_to_insert_data_in_topic(crawler_object,my_producer,keyword)