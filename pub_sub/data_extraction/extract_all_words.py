# get all the words that are being used frequently
from rest_flask.data_cleaning import clean_tweet


def get_all_the_words(message):

    list_of_words = clean_tweet(message['tweet'])
    if len(list_of_words):
        return list_of_words
    else:
        return None


# print(get_all_the_words({'tweet':'my name is a aditya gupta '}))

