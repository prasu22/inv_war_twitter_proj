import re

COVID_KEYS = ['death', 'hospitalisation', 'medicine' ]
ECONOMY_KEYS = ['GDP', 'unemployment', 'employment', 'layoffs', 'market', 'stock', 'index']

def get_tweets_with_trending_covid_keywords(message):
    list_of_covid_keywords = []
    if re.compile('|'.join(COVID_KEYS ),re.IGNORECASE).search(message['tweet']):
        list_of_covid_keywords = re.compile('|'.join(COVID_KEYS), re.IGNORECASE).findall(message['tweet'])
    message["covid_trending_keywords"] = list_of_covid_keywords
    return message

def get_tweets_with_trending_economy_keywords(message):
    list_of_economy_keywords = []
    if re.compile('|'.join(ECONOMY_KEYS ),re.IGNORECASE).search(message['tweet']):
        list_of_economy_keywords = re.compile('|'.join(ECONOMY_KEYS), re.IGNORECASE).findall(message['tweet'])
    message["economy_trending_keywords"] = list_of_economy_keywords
    return message
