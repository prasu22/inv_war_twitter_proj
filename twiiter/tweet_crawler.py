import sys
import tweepy
from datetime import datetime
from twiiter.twitter_api_connector import access_token, access_token_secret, consumer_secret, consumer_key
from twiiter.twitter_api_connector import connect_with_twitter

config = [access_token_secret, access_token, consumer_secret, consumer_key]

last_time_for_search_api = ""
last_time_for_search_30_api = ""
current_time = datetime.now()


class StreamListener(tweepy.Stream):

    def on_status(self, status):
        if hasattr(status, 'extended_tweet') and status.user.location != None:
            text = status.extended_tweet['full_text']
            location = status.user.location
            created_at = status.created_at
            tweet_id = status.id_str

            # call the mongo insertion function here some fixing need to be done here

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
        :return: all the tweets
        '''

        try:
            streamListener = StreamListener(config[3], config[2], config[1], config[0])
            streamListener.filter(track=keywords, languages=['en'])

        except Exception as e:
            print("Error occured please check  ", e)

    ######### through search api

    def fetch_tweets_from_search_api(self, keywords1, last_time_for_search_api):
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
        query = keywords1 + " -filter:retweets"
        current_time = datetime.now()
        difference_time = 0

        if last_time_for_search_api != "":
            difference_time = (current_time - last_time_for_search_api).total_seconds()

        if last_time_for_search_api == "" or difference_time > 900:

            try:
                tweets = tweepy.Cursor(api.search_tweets, q=query, lang="en", tweet_mode="extended").items(n)
                return tweets

            except tweepy.TooManyRequests as err:
                print("RATE LIMIT EXCEED ,", err)
                self.last_time_for_search_api = datetime.now()

            except Exception as e:
                print("Some error happened ")

        elif difference_time <= 900:
            print("DONT CALL WAIT FOR ", int(900 - difference_time))

    def fetch_tweets_from_archive_api(self, keywords1, last_time_for_search_30_api, start_date=None, end_date=None):
git
        query = keywords1 + " lang:en"
        current_time = datetime.now()
        difference_time = 0

        if last_time_for_search_30_api != "":
            difference_time = (current_time - last_time_for_search_30_api).total_seconds()

        if last_time_for_search_30_api == "" or difference_time > 900:

            try:
                tweets = tweepy.Cursor(self.api.search_30_day, label="research", query=query, maxResults=100,
                                       romDate=start_date, toDate=end_date).items()
                return tweets

            except tweepy.TooManyRequests as err:
                print("RATE LIMIT EXCEED ,", err)
                self.last_time_for_search_30_api = datetime.now()

            except Exception as e:
                print("Some error happened , please check")

        elif difference_time <= 900:
            print("Wait for next call ", 900 - difference_time)


keywords = ['precaution', 'preventions', 'precautions', 'prevention', 'covid', 'corona', 'donation', 'fund', 'donating',
            'donations']  # for stream
start_date = '202202271220'
end_date = '202203291220'
api = connect_with_twitter()
crawler_object = TweetCrawler(config, api)
keywords1 = "donation covid"

# tweets = crawler_object.fetch_tweets_from_archive_api(keywords1,last_time_for_search_30_api)
# tweets = crawler_object.fetch_tweets_from_search_api(keywords1,last_time_for_search_api)
# crawler_object.fetch_tweets_from_stream(keywords)
#
# tweets = crawler_object.fetch_tweets_from_search_api(keywords1)
# crawler_object.dump_in_database(tweets)
#
# for tweet in tweets:
#     print(tweet)




