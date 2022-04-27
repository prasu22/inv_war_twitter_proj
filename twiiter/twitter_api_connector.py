# This module contains all functions/routine to fetch data from twitter using tweepy



import tweepy
from twiiter.config import consumer_key,consumer_secret,access_token,access_token_secret

def connect_with_twitter():

    try:
        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)
        api = tweepy.API(auth, wait_on_rate_limit=True)
        return api

    except:
        print("ERROR AUTHENTICATION FAILED ")





