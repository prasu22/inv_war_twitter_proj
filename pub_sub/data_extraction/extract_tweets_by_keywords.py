# code for keywords

import re

def get_tweets_with_keyword(message,keywords):

    if re.compile('|'.join(keywords),re.IGNORECASE).search(message['tweet']):
        return message
    else:
        return None


# print(get_tweets_with_keyword({'tweet':'covid is not good'},['aditya','corona']))


