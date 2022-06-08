import logging

from src.common.app_config import APP_CONFIG

LOGGER = logging.getLogger(__name__)
import re
from src.common.variable_files import COVID_TRENDING_KEYWORD_KEY, ECONOMY_TRENDING_KEYWORD_KEY, TWEET_KEY

COVID_KEYS = list(map(str, APP_CONFIG.getlist('keywords', 'COVID_KEYS')))
ECONOMY_KEYS = list(map(str, APP_CONFIG.getlist('keywords', 'ECONOMY_KEYS')))

def get_tweets_with_trending_covid_keywords(message):
    try:
        list_of_covid_keywords = []
        if re.compile('|'.join(COVID_KEYS), re.IGNORECASE).search(message[TWEET_KEY]):
            list_of_covid_keywords = re.compile('|'.join(COVID_KEYS), re.IGNORECASE).findall(message[TWEET_KEY])
        else:
            LOGGER.info(f"MESSAGE: Data is not found")
        message[COVID_TRENDING_KEYWORD_KEY] = list_of_covid_keywords
        return message
    except Exception as e:
        LOGGER.error(f"ERROR:{e} ")


def get_tweets_with_trending_economy_keywords(message):
    try:
        list_of_economy_keywords = []
        if re.compile('|'.join(ECONOMY_KEYS), re.IGNORECASE).search(message[TWEET_KEY]):
            list_of_economy_keywords = re.compile('|'.join(ECONOMY_KEYS), re.IGNORECASE).findall(message[TWEET_KEY])
        else:
            LOGGER.info(f"MESSAGE: Data is not found")
        message[ECONOMY_TRENDING_KEYWORD_KEY] = list_of_economy_keywords
        return message
    except Exception as e:
        LOGGER.error(f"ERROR:{e} ")



def parse_trending_covid_keywords(tweet_list):
    list_tweet = list(map(lambda x: get_tweets_with_trending_covid_keywords(x), tweet_list))
    return list_tweet

def parse_trending_economy_keywords(tweet_list):
    list_tweet = list(map(lambda x: get_tweets_with_trending_economy_keywords(x), tweet_list))
    return list_tweet

