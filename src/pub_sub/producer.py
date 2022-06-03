from json import dumps
from kafka import KafkaProducer
from src.common.variable_files import BOOTSTRAP_SERVER

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