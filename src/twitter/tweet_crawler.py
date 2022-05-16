# data fetching apis
import sys
import tweepy
from datetime import datetime
from src.twitter.config import access_token, access_token_secret, consumer_secret, consumer_key
import logging

LOGGER = logging.getLogger(__name__)

config = [access_token_secret, access_token, consumer_secret, consumer_key]

last_time_for_search_api = ""
last_time_for_search_30_api = ""
current_time = datetime.now()


class StreamListener(tweepy.Stream):
    def on_status(self, status):
        if hasattr(status, 'extended_tweet') and status.user.location != None:
            full_text = status.extended_tweet['full_text']
            country = status.user.location
            created_at = status.created_at
            new_dt = str(created_at)[:19]
            created_date = datetime.strptime(new_dt, '%Y-%m-%d %H:%M:%S')
            tweet_id = status.id_str
            my_data = {'_id': str(tweet_id), 'tweet': full_text, 'country': country, 'created_at': str(created_date)}
            print("tweet cawler stream mydata",my_data)
            import src.pub_sub.producer as prod
            prod.my_producer.send('sendingdata', value=my_data)

    def on_error(self, status_code):
        LOGGER.error(f"ERROR;Encountered Streaming error ( {status_code} )" )
        if status_code == 420:
            return False
        sys.exit(1)

    def on_timeout(self):
        # This is called if there is a timeout
        LOGGER.error(f"ERROR;{sys.stderr}, 'Timeout.....'")
        return True


class TweetCrawler:

    def __init__(self, config, api):
        self.config = config
        self.api = api

    def fetch_tweets_from_stream(self, keywords):
        '''
        fetches tweets from twitter stream API and returns list of tweets in below format
        {
         id:
         tweet_text: <full_text of the tweet>
         created_at:
         country:
        }
        :return: list of dictionary
        if rate limit is reached, raises exception ratelimit reached
        '''

        try:
            streamListener = StreamListener(config[3], config[2], config[1], config[0])
            streamListener.filter(track=keywords, languages=['en'])
        except Exception as e:
            LOGGER.error(f"ERROR:{e}")

    def fetch_tweets_from_search_api(self, keywords1, last_time_for_search=''):
        '''
        fetches tweets from twitter search API and returns list of tweets in below format
        {
         id:
         tweet_text: <full_text of the tweet>
         created_at:
         country:
        }
        :return: list of dictionary / (json format)
        if rate limit is reached, raises exception ratelimit reached
        '''
        n = 200
        query = keywords1 + " covid" + " -filter:retweets"
        current_time = datetime.now()
        difference_time = 0
        global last_time_for_search_api
        last_time_for_search_api = last_time_for_search

        if last_time_for_search_api != "":
            difference_time = (current_time - last_time_for_search_api).total_seconds()

        if last_time_for_search_api == "" or difference_time > 900:

            try:
                tweets = tweepy.Cursor(self.api.search_tweets, q=query, lang="en", tweet_mode="extended").items(n)
                if tweets.next():
                    return tweets



            except tweepy.TooManyRequests as err:
                LOGGER.error(f"RATE LIMIT EXCEED ,{ err}")
                last_time_for_search_api = datetime.now()

            except Exception as e:
                LOGGER.error(f"ERROR:{e}")

        elif difference_time <= 900:
            LOGGER.info(f"DONT CALL WAIT FOR {int(900 - difference_time)}")

    
