# twitter authentication
import tweepy
from src.twitter.config import consumer_key, consumer_secret, access_token, access_token_secret
import logging

LOGGER = logging.getLogger(__name__)

def connect_with_twitter():
    try:
        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)
        api = tweepy.API(auth, wait_on_rate_limit=True)
        return api

    except Exception as e:
        LOGGER.error(f"ERROR:Twitter authentication error, {e}")
