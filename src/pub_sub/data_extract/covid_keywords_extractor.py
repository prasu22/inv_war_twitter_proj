
import logging
from src.common.app_config import APP_CONFIG

LOGGER = logging.getLogger(__name__)
import re

from src.common.variable_files import COVID_KEYWORD_KEY, TWEET_KEY, IS_COVID_TWEET

COVID_KEYWORDS = list(map(str, APP_CONFIG.getlist('keywords', 'COVID_KEYWORDS')))

def get_covid_keywords(message):
    list_of_covid_keywords = []
    try:
        if re.compile('|'.join(COVID_KEYWORDS), re.IGNORECASE).search(message[TWEET_KEY]):
            list_of_covid_keywords = re.compile('|'.join(COVID_KEYWORDS), re.IGNORECASE).findall(message[TWEET_KEY])
        else:
            LOGGER.info(f"MESSAGE: Data Not found in tweet! ")
        message[COVID_KEYWORD_KEY] = list_of_covid_keywords

        if len(list_of_covid_keywords)>0:
            message[IS_COVID_TWEET] = True
        else:
            message[IS_COVID_TWEET] = False
        return message
    except Exception as e:
        LOGGER.error(f"ERROR:{e} ")



def parse_covid_keywords(tweet_list):
    list_tweet = list(map(lambda x: get_covid_keywords(x), tweet_list))
    return list_tweet



