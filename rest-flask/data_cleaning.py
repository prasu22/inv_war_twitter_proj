import re
import string
import nltk as nltk
nltk.download('stopwords')
nltk.download('punkt')

stopword = nltk.corpus.stopwords.words('english')


def clean_tweet(tweet):
    """
    used to creat the tweet text data removing digit, url,
    :param
        tweet: store thed text data after removing punctuation
        clean: remove the digits
        words: break the sentence in words
        words_new: store the list of words which are not in stopwords and there length greater than 3
        final_list: return the text with clean words

    :return: the final clean text return in lower letters
    """
    tweet = "".join([char for char in tweet if char not in string.punctuation])
    clean = re.sub("[0-9]", " ", tweet)
    words = nltk.tokenize.word_tokenize(clean)
    words_new = [i for i in words if i.lower not in stopword and len(i)>3]
    final_list = " ".join(words_new)
    return final_list.lower()
