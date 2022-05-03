import re
from configparser import ConfigParser

from common_variables.variable_files import COVID_KEYWORD_KEY, TWEET_KEY

file = '../../common_variables/config.ini'
config = ConfigParser(converters={'list': lambda x: [i.strip() for i in x.split(',')]})
config.read(file)
COVID_KEYWORDS = list(map(str, config.getlist('keywords', 'COVID_KEYWORDS')))


def get_covid_keywords(message):
    list_of_covid_keywords = []
    if re.compile('|'.join(COVID_KEYWORDS), re.IGNORECASE).search(message[TWEET_KEY]):
        list_of_covid_keywords = re.compile('|'.join(COVID_KEYWORDS), re.IGNORECASE).findall(message[TWEET_KEY])

    else:
        print("data is not found")
    message[COVID_KEYWORD_KEY] = list_of_covid_keywords
    return message


print(get_covid_keywords({'tweet':'covid is true'}))