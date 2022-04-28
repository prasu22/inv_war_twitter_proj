# code for keywords

import re
keyword = ['covid', 'virus', 'coronavirus']

def get_tweets_with_keyword(message,keywords):
    if re.compile('|'.join(keywords),re.IGNORECASE).search(message['tweet']):
        list_of_covid_keywords = re.compile('|'.join(keywords), re.IGNORECASE).findall(message['tweet'])
        message["covid_keywords"] = list_of_covid_keywords
    return message



# print(get_tweets_with_keyword({'tweet':'covid is not good'},['aditya','corona']))


