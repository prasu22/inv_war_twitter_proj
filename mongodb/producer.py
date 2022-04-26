#importing required libraries
from datetime import datetime
from json import dumps
from time import sleep
from kafka import KafkaProducer

import twiiter.tweet_crawler as tc
from twiiter.twitter_api_connector import connect_with_twitter
# try:
#     from twitter import tweet_crawler
# except ImportError:
#     import sys
#     tweet_crawler = sys.modules[__package__ + '.tweet_crawler']


my_producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        api_version=(0, 11, 5),
        value_serializer=lambda x: dumps(x).encode('utf-8')
    )

# ======================================================================================================================
#static keyword list
keywords = ['death', 'hospitalisation', 'medicine','GDP', 'unemployment', 'employment', 'layoffs', 'market', 'stock', 'index','donation','fund','donating','donations','wear mask','use sanitiser','stay home','social distancing','wash hands','precaution','preventions','precautions','prevention','covid','corona']
# ======================================================================================================================

# ======================================================================================================================
# generating the numbers ranging from 1 to 500
# code used when we use collect_with_keyword
# send data to topic after fetch from twitter using search_tweet
crawler_object = tc.TweetCrawler(tc.config)


for keyword in keywords:
    tweets_data = crawler_object.fetch_tweets_from_search_api(keyword)
    for tweet in tweets_data:
        country = tweet._json['user']['location']
        if len(country) > 0:
            id = tweet._json['id']
            full_text = tweet._json['full_text']
            created_at = tweet._json['created_at']
            new_datetime = datetime.strptime(str(datetime.strptime(created_at, '%a %b %d %H:%M:%S +0000 %Y')),'%Y-%m-%d %H:%M:%S')
            try:
                my_data = {'_id': str(id),'tweet':full_text,'country':country,'created_at':str(new_datetime)}
                print("search",my_data)
                my_producer.send('sendingdata', value=my_data)
                sleep(2)
            except Exception as e:
                print(e)
                pass
# ======================================================================================================================
# ======================================================================================================================
# sending data in topic after fetching from twitter stream
crawler_object.fetch_tweets_from_stream(keywords)

# ======================================================================================================================
# sending data in topic after fetching data from search_30_days
# print("list of words",keywords)
# for keyword in keywords:
#     print("data stroed")
#     tweets_details = crawler_object.fetch_tweets_from_archive_api(keyword)
#     print(tweets_details.num_tweets)
#     for tweet in tweets_details:
#         print(tweet)
#         if (not tweet.retweeted) and ('RT @' not in tweet.text):
#             if tweet.user.location != None:
#                     status = api.get_status(tweet.id, tweet_mode='extended')
#                     id = tweet.id
#                     text = status.full_text
#                     # created_at = tweet.created_at.replace(tzinfo=None)
#                     new_dt = str(tweet.created_at)[:19]
#                     created_at = datetime.strptime(new_dt, '%Y-%m-%d %H:%M:%S')
#                     print(created_at)
#                     country = tweet.user.location
#                     try:
#                         my_data = {'_id': str(id), 'tweet': text, 'country': country, 'created_at': str(created_at)}
#                         print("new_data start:\n",my_data)
#                         my_producer.send('sendingdata', value=my_data)
#                         sleep(1)
#                     except Exception as e:
#                         print(e)
#                         pass
# ======================================================================================================================








