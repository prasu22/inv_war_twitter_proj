import logging

from src.common.app_config import APP_CONFIG
from src.common.variable_files import TWEET_KEY, TWEET_KEYWORDS

LOGGER = logging.getLogger(__name__)
import re
import string
import nltk as nltk
from cleantext import clean

nltk.download('stopwords')
nltk.download('punkt')

stopword = nltk.corpus.stopwords.words('english')
COVID_KEYWORDS = list(map(str, APP_CONFIG.getlist('keywords', 'COVID_KEYWORDS')))

def get_tweet_keywords(message):
    """
    used to creat the tweet text data removing digit, url,
    :passing argument
      tweet: It is a string
    :param
        tweet: store thed text data after removing punctuation
        clean: remove the digits
        only_text: store the only english words
        words: break the sentence in words
        words_new: store the list of words which are not in stopwords and there length greater than 3
        final_list: return the text with clean words

    :return: the final clean text return in lower letters
    """
    try:
        tweet = message[TWEET_KEY]
        tweet_without_emoji = clean(tweet, no_emoji=True)
        tweet_without_numbers = re.sub("[0-9]", " ", tweet_without_emoji)
        tweet_without_punctuation = re.sub('[%s]' % re.escape(string.punctuation), ' ', tweet_without_numbers)
        tokenize_words = nltk.tokenize.word_tokenize(tweet_without_punctuation)
        words_list = [i for i in tokenize_words if i.lower not in stopword and len(i) > 3]

        message[TWEET_KEYWORDS] = words_list
    except Exception as e:
        LOGGER.error(f"ERROR:{e} ")

    return message



def parse_tweet_keywords(tweet_list):

    updated_list = []
    for message in tweet_list:
        msg = get_tweet_keywords(message)
        updated_list.append(msg)


    return updated_list
