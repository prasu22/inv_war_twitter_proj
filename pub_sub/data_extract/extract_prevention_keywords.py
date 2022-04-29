import re

from comman_variables.variable_files import PREVENTION_KEYWORDS, WHO_KEYWORDS, PREVENTION_KEYWORDS_KEY, \
    WHO_KEYWORDS_KEY, TWEET_KEY


def get_prevention_keywords(message):
    list_of_prevention_keywords = []
    if re.compile('|'.join(PREVENTION_KEYWORDS), re.IGNORECASE).search(message[TWEET_KEY]):
        list_of_prevention_keywords = re.compile('|'.join(PREVENTION_KEYWORDS), re.IGNORECASE).findall(message[TWEET_KEY])
    else:
        print("data is not found")
    message[PREVENTION_KEYWORDS_KEY] = list_of_prevention_keywords
    return message


def get_who_keywords(message):
    list_of_who_keywords = []
    if re.compile('|'.join(WHO_KEYWORDS), re.IGNORECASE).search(message[TWEET_KEY]):
        list_of_who_keywords = re.compile('|'.join(WHO_KEYWORDS), re.IGNORECASE).findall(message[TWEET_KEY])
    else:
        print("data is not found")
    message[WHO_KEYWORDS_KEY] = list_of_who_keywords
    return message
