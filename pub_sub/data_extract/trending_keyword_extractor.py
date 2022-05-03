import re

from common_variables.variable_files import COVID_KEYS, ECONOMY_KEYS, COVID_TRENDING_KEYWORD_KEY, \
    ECONOMY_TRENDING_KEYWORD_KEY, TWEET_KEY


def get_tweets_with_trending_covid_keywords(message):
    list_of_covid_keywords = []
    if re.compile('|'.join(COVID_KEYS), re.IGNORECASE).search(message[TWEET_KEY]):
        list_of_covid_keywords = re.compile('|'.join(COVID_KEYS), re.IGNORECASE).findall(message[TWEET_KEY])
    else:
        print('data is not find')
    message[COVID_TRENDING_KEYWORD_KEY] = list_of_covid_keywords
    return message


def get_tweets_with_trending_economy_keywords(message):
    list_of_economy_keywords = []
    if re.compile('|'.join(ECONOMY_KEYS), re.IGNORECASE).search(message[TWEET_KEY]):
        list_of_economy_keywords = re.compile('|'.join(ECONOMY_KEYS), re.IGNORECASE).findall(message[TWEET_KEY])
    else:
        print('data is not find')
    message[ECONOMY_TRENDING_KEYWORD_KEY] = list_of_economy_keywords
    return message
