import sys
import tweepy
from datetime import datetime
from twiiter.config import access_token, access_token_secret, consumer_secret, consumer_key

config = [access_token_secret, access_token, consumer_secret, consumer_key]

last_time_for_search_api = ""
last_time_for_search_30_api = ""
current_time = datetime.now()


class StreamListener(tweepy.Stream):
    def on_status(self, status):
        if hasattr(status, 'extended_tweet') and status.user.location != None:
            full_text = status.extended_tweet['full_text']
            country = status.user.location
            # print("stream: ",status.created_at)
            created_at = status.created_at
            new_dt = str(created_at)[:19]
            created_date = datetime.strptime(new_dt, '%Y-%m-%d %H:%M:%S')
            tweet_id = status.id_str
            my_data = {'_id': str(tweet_id), 'tweet': full_text, 'country': country, 'created_at': str(created_date)}
            print("stream", my_data)
            import pub_sub.producer as prod
            prod.my_producer.send('sendingdata', value=my_data)

    def on_error(self, status_code):
        print("Encountered Streaming error (", status_code, ")")
        if status_code == 420:
            return False
        sys.exit(1)

    def on_timeout(self):
        # This is called if there is a timeout
        print(sys.stderr, 'Timeout.....')
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
            print("message is ", e)

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
                print("RATE LIMIT EXCEED ,", err)
                last_time_for_search_api = datetime.now()

            except Exception as e:
                print("Some error happened ", e)

        elif difference_time <= 900:
            print("DONT CALL WAIT FOR ", int(900 - difference_time))

    def fetch_tweets_from_archive_api(self, keywords1, last_time_for_search='', start_date=None, end_date=None):

        query = keywords1 + " lang:en"
        current_time = datetime.now()
        difference_time = 0

        global last_time_for_search_30_api
        last_time_for_search_30_api = last_time_for_search

        if last_time_for_search_30_api != "":
            difference_time = (current_time - last_time_for_search_30_api).total_seconds()

        if last_time_for_search_30_api == "" or difference_time > 900:
            try:
                tweets = tweepy.Cursor(self.api.search_30_day, label="research", query=query, maxResults=100,
                                       romDate=start_date, toDate=end_date).items()
                if tweets.next():
                    return tweets

            except tweepy.TooManyRequests as err:
                print("RATE LIMIT EXCEED ,", err)
                last_time_for_search_30_api = datetime.now()
                print("last time ", last_time_for_search_30_api)

            except Exception as e:
                print("Some error happened , please check")

        elif difference_time <= 900:
            print("Wait for next call ", 900 - difference_time)
