import logging
from cleantext import clean
from src.common.app_config import APP_CONFIG
from src.common.variable_files import TWEET_KEY, TWEET_KEYWORDS
import re
import string
import nltk as nltk
from nltk import ngrams
nltk.download('stopwords')
nltk.download('punkt')
# from nltk.corpus import stopwords



LOGGER = logging.getLogger(__name__)

stopword = nltk.corpus.stopwords.words('english')
COVID_KEYWORDS = list(map(str, APP_CONFIG.getlist('keywords', 'COVID_KEYWORDS')))
# print("tweet_keyword_extractor")
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
        tweet_without_stopwords = (" ").join([i for i in tweet_without_punctuation.split() if i.lower() not in stopword and len(i) > 3])
        # print(tweet_without_stopwords)
        n1=1
        n2=2
        n3=3
        unigrams = ngrams(tweet_without_stopwords.split(), n1)
        bigram = ngrams(tweet_without_stopwords.split(),n2)
        trigram = ngrams(tweet_without_stopwords.split(),n3)
        word_list = []
        for item in unigrams:
            word_list.append(item)
        for item in bigram:
            word_list.append(item)
        for item in trigram:
            word_list.append(item)
        ngram_word_list = []
        for word in word_list:
            ngram_word_list .append(" ".join(list(word)))
        message[TWEET_KEYWORDS] = ngram_word_list
        return message
    except Exception as e:
        LOGGER.error(f"ERROR:{e} ")

#
# message = {"tweet":"@nathan_cllr @StephenNolan It\u2019s when where how this that weren't a bit like their failed idea of Covid passports &amp; wanting to allow men to avail of free period products.  Also their failure to address the disaster of MOT centres and wanting to give every family more free money.\nThe SDLP would lead this country to absolute ruin."}
# updated_message = get_tweet_keywords(message)
# print(updated_message)



def parse_tweet_keywords(tweet_list):

    updated_list = []
    for message in tweet_list:
        msg = get_tweet_keywords(message)
        updated_list.append(msg)
    return updated_list

