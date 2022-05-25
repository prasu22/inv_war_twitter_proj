# importing required libraries
from json import dumps
from kafka import KafkaProducer


""""
  creating producer to call tweetcrawler api to fetch data from twitter and insert that in topic by the producer
  :params
  my_producer =  initialize the kafka producer
  crawler_object = creating object of class TweetCrawler
  keywords  = list of keyword based on which we fetch tweet
  
"""

my_producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    api_version=(0, 11, 5),
    value_serializer=lambda x: dumps(x).encode('utf-8')
)




