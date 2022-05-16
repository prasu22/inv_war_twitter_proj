import logging
LOGGER = logging.getLogger(__name__)
import re
import string
import nltk as nltk
from cleantext import clean
nltk.download('stopwords')
nltk.download('punkt')

stopword = nltk.corpus.stopwords.words('english')


def clean_tweet(tweet):
    """
    used to creat the tweet text data removing digit, url,
    :passing argument
      tweet: It is a string
    :param
        tweet: store thed text data after removing punctuation
        clean: remove the digits
        words: break the sentence in words
        words_new: store the list of words which are not in stopwords and there length greater than 3
        final_list: return the text with clean words

    :return: the final clean text return in lower letters
    """
    try:
        tweet = "".join([char for char in tweet if char not in string.punctuation])
        emoji_remove = clean(tweet, no_emoji=True)
        only_text = re.sub("[0-9][!@#$%^&*()_+=~;:}{|\"\'\]\[]", " ", emoji_remove)
        words = nltk.tokenize.word_tokenize(only_text)
        words_new = [i for i in words if i.lower not in stopword and len(i)>3]
        final_list = " ".join(words_new)
        return final_list.lower()
    except Exception as e:
        LOGGER.error(f"ERROR:{e} ")