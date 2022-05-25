import logging
from src.common.app_config import APP_CONFIG

LOGGER = logging.getLogger(__name__)
import re
from src.common.variable_files import PREVENTION_KEYWORDS_KEY, WHO_KEYWORDS_KEY, TWEET_KEY


PREVENTION_KEYWORDS = list(map(str, APP_CONFIG.getlist('keywords', 'PREVENTION_KEYWORDS')))
WHO_KEYWORDS = list(map(str, APP_CONFIG.getlist('keywords', 'WHO_KEYWORDS')))

def get_prevention_keywords(message):
    list_of_prevention_keywords = []
    try:
        if re.compile('|'.join(PREVENTION_KEYWORDS), re.IGNORECASE).search(message[TWEET_KEY]):
            list_of_prevention_keywords = re.compile('|'.join(PREVENTION_KEYWORDS), re.IGNORECASE).findall(
                message[TWEET_KEY])
        else:
            LOGGER.info(f"MESSAGE:Data is not found ")
        message[PREVENTION_KEYWORDS_KEY] = list_of_prevention_keywords
        return message
    except Exception as e:
        LOGGER.error(f"ERROR:{e} ")



def get_who_keywords(message):
    list_of_who_keywords = []
    try:
        if re.compile('|'.join(WHO_KEYWORDS), re.IGNORECASE).search(message[TWEET_KEY]):
            list_of_who_keywords = re.compile('|'.join(WHO_KEYWORDS), re.IGNORECASE).findall(message[TWEET_KEY])
        else:
            LOGGER.info(f"MESSAGE:Data is not found ")
        message[WHO_KEYWORDS_KEY] = list_of_who_keywords
        return message
    except Exception as e:
        LOGGER.error(f"ERROR:{e} ")


def parse_who_keywords(tweet_list):
    list_tweet = list(map(lambda x: get_who_keywords(x), tweet_list))
    return list_tweet

def parse_prevention_keywords(tweet_list):
    list_tweet = list(map(lambda x: get_prevention_keywords(x), tweet_list))
    return list_tweet

