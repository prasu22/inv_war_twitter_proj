import re

from common_variables.variable_files import COVID_KEYWORDS, COVID_KEYWORD_KEY, TWEET_KEY


def get_covid_keywords(message):
    list_of_covid_keywords = []
    if re.compile('|'.join(COVID_KEYWORDS), re.IGNORECASE).search(message[TWEET_KEY]):
        list_of_covid_keywords = re.compile('|'.join(COVID_KEYWORDS), re.IGNORECASE).findall(message[TWEET_KEY])

    else:
        print("data is not found")
    message[COVID_KEYWORD_KEY] = list_of_covid_keywords
    return message
