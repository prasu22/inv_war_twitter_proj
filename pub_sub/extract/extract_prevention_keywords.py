# code for keywords

import re

PREVENTION_KEYWORDS = ['mask','sanitiser','stay home','social distancing','wash hands',"vaccination","vaccine","booster dose","Stay Safe"]
WHO_KEYWORDS = ["@WHO","#WHO","World health organization"]

def get_prevention_keywords(message):
    list_of_prevention_keywords =[]
    if re.compile('|'.join(PREVENTION_KEYWORDS),re.IGNORECASE).search(message['tweet']):
        list_of_prevention_keywords= re.compile('|'.join(PREVENTION_KEYWORDS), re.IGNORECASE).findall(message['tweet'])
    message["prevention_keywords"] = list_of_prevention_keywords
    return message

def get_who_keywords(message):
    list_of_who_keywords = []
    if re.compile('|'.join(WHO_KEYWORDS), re.IGNORECASE).search(message['tweet']):
        list_of_who_keywords = re.compile('|'.join(WHO_KEYWORDS), re.IGNORECASE).findall(message['tweet'])
    message["WHO_keywords"] = list_of_who_keywords
    return message


# result = get_prevention_keyword({'tweet':'covid is not good to preventention sanitiser by @WHO wash hands'})
# print(result)
# print(get_who_keywords(result))

# print(get_tweets_with_keyword({'tweet':'covid is not good'},['aditya','corona']))


