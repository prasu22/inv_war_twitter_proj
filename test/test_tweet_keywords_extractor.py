import unittest

from src.common.variable_files import TWEET_KEYWORDS
from src.pub_sub.data_extract.tweet_keywords_extractor import get_tweet_keywords


class TestTweetKeywords(unittest.TestCase):

    def test_tweet_keyword_case_1(self):

        ### only unigram word
        message = {'tweet': 'take'}
        result = get_tweet_keywords(message)
        self.assertEqual(result[TWEET_KEYWORDS], ['take'])

    def test_tweet_keyword_case_2(self):

        # for uni and bi gram
        message = {'tweet': 'wear mask'}
        result = get_tweet_keywords(message)
        self.assertEqual(result[TWEET_KEYWORDS], ['wear','mask','wear mask'])

    def test_tweet_keyword_case_3(self):

        # for uni , bi , tri gram
        message = {'tweet': 'hello world good '}
        result = get_tweet_keywords(message)
        self.assertEqual(result[TWEET_KEYWORDS], ['hello','world','good','hello world','world good','hello world good'])


    def test_tweet_keyword_case_4(self):

        ## no input
        message = {'tweet': ''}
        result = get_tweet_keywords(message)
        self.assertEqual(result[TWEET_KEYWORDS], [])

    def test_tweet_keyword_case_5(self):
        ## long tweet
        message = {'tweet': 'hello world good people'}
        result = get_tweet_keywords(message)
        self.assertEqual(result[TWEET_KEYWORDS], ['hello','world','good','people','hello world','world good','good people','hello world good','world good people'])

# if __name__ == '__main__':
#     unittest.main()