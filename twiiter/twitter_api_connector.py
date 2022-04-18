# This module contains all functions/routine to fetch data from twitter using tweepy
#
# 1. raw data b/w dates
# 2. fitered by a keyword
#     -- keyword could be a string, could be regex, could be a raw keyword plus grammatical variations


import tweepy
from twiiter.config import consumer_key,consumer_secret,access_token,access_token_secret
last_id = None
def connect_with_twitter():

    try:
        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)
        api = tweepy.API(auth, wait_on_rate_limit=True)
        return api

    except:
        print("ERROR AUTHENTICATION FAILED ")



def collect_with_keyword(api,query):

    n = 200
    tweets = tweepy.Cursor(api.search_tweets, q="#covid", lang="en", tweet_mode="extended").items(n)
    return tweets



def collect_with_date(api,query,last_date):

    tweets = api.search_tweets(q = query , count =100, until = last_date,lang="en",tweet_mode='extended',max_id =last_id)
    return tweets


