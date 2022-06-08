# importing required libraries
from json import dumps
from kafka import KafkaProducer

from src.common.variable_files import BOOTSTRAP_SERVER
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

keywords = ['GDP', 'unemployment', 'employment', 'stock market', 'index', 'market', '#WHO', "vaccination", "vaccine",
            "booster dose", "Stay Safe", 'use sanitiser', 'stay home',
            'social distancing', 'wash hands', 'precaution', 'covid', 'precautions', 'prevention', 'death',
            'hospitalisation', "Money", '@WHO', 'mask', 'precaution', 'corona',
            "donate", "fund", "charity", 'donation', 'contribution', "amount donated", '#WHO', "vaccination", "vaccine",
            "booster dose", "Stay Safe", 'use sanitiser', 'stay home', 'social distancing', 'wash hands', 'precaution',
            'covid', 'precautions', 'prevention', 'corona', 'coronavirus', 'donation', 'fund', 'donating', 'donations']



api = connect_with_twitter()
crawler_object = tc.TweetCrawler(tc.config, api)

# sending data in topic after fetching from twitter stream
crawler_object.fetch_tweets_from_stream(keywords)