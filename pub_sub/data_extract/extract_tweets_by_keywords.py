# code for keywords

import re

COVID_KEYWORDS = ['covid', 'virus', 'coronavirus']

def get_tweets_with_keyword(message):
    list_of_covid_keywords = []
    if re.compile('|'.join(COVID_KEYWORDS ),re.IGNORECASE).search(message['tweet']):
        list_of_covid_keywords = re.compile('|'.join(COVID_KEYWORDS), re.IGNORECASE).findall(message['tweet'])
    message["covid_keywords"] = list_of_covid_keywords
    return message



# print(get_tweets_with_keyword({'tweet':'covid is not good'},['aditya','corona']))


