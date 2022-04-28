# get all the words that are being used frequently
from pub_sub.data_analytics.data_cleaning import clean_tweet


def get_all_the_words(message):
    list_of_words = clean_tweet(message['tweet'])
    message["list_of_extracted_words"]=list_of_words
    return message



# print(get_all_the_words({'tweet':'my name is a aditya gupta '}))

