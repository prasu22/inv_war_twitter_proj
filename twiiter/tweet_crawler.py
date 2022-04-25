import sys
import tweepy
from datetime import datetime
from twiiter.config import access_token,access_token_secret,consumer_secret,consumer_key
from twiiter.twitter_api_connector import connect_with_twitter




config = [access_token_secret,access_token,consumer_secret,consumer_key]


DEFAULT_THRESHOLD=10

class StreamListener(tweepy.Stream):

    def on_status(self,status):
        if hasattr(status,'extended_tweet') and status.user.location != None:
            full_text = status.extended_tweet['full_text']
            country = status.user.location
            # print("stream: ",status.created_at)
            created_at = status.created_at
            tweet_id = status.id_str
            my_data = {'_id': str(tweet_id),'tweet':full_text,'country':country,'created_at':str(created_at)}
            print(my_data)


    def on_error(self,status_code):
        print("Encountered Streaming error (",status_code,")")
        if status_code == 420:
            return False
        sys.exit(1)


class TweetCrawler:

    def __init__(self,config):
        self.config = config

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
            print("message is ",e)

    def fetch_tweets_from_search_api(self, keywords1: list):
        '''
        fetches tweets from twitter search API and returns list of tweets in below format
        {
         id:
         tweet_text: <full_text of the tweet>
         created_at:
         country:
        }
        :return: list of dictionary
        if rate limit is reached, raises exception ratelimit reached
        '''
        api = connect_with_twitter()
        n = 100
        query =keywords1 +" AND Covid"+  "-filter:retweets"
        tweets = tweepy.Cursor(api.search_tweets, q=query, lang="en", tweet_mode="extended").items(n)
        return tweets



    def fetch_tweets_from_archive_api(self, keywords1, start_date=None, end_date=None):
        query = keywords1+" lang:en"
        api = connect_with_twitter()
        # fetching the required data from the twitter api
        try:
            tweets = tweepy.Cursor(api.search_30_day, label="research", query=query, maxResults=100,fromDate=start_date, toDate=end_date).items()
            return tweets

        except Exception as e:
            print("message", e)

