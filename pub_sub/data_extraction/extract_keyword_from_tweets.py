import re

def get_keywords(message,keywords):

    if re.compile('|'.join(keywords),re.IGNORECASE).search(message['tweet']):
        words_list = re.compile('|'.join(keywords), re.IGNORECASE).findall(message['tweet'])
        return words_list
    else:
        return None