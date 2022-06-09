import unittest
from src.common.variable_files import COVID_TRENDING_KEYWORD_KEY, ECONOMY_TRENDING_KEYWORD_KEY
from src.pub_sub.data_extract.trending_keywords_extractor import get_tweets_with_trending_covid_keywords, \
    get_tweets_with_trending_economy_keywords


class TestTrendingKeywords(unittest.TestCase):
    def test_trending_word_containing_single_required_covid_keyword(self):
        message = {'tweet': "in 2021 the death count of people due to corona is very high"}
        result = get_tweets_with_trending_covid_keywords(message)
        self.assertEqual(result[COVID_TRENDING_KEYWORD_KEY], ['death'])

    def test_trending_word_containing_multiple_required_covid_keyword(self):
        message = {
            'tweet': "in 2021 the death count of people due to corona is very high and many people need hospitalisation at that time"}
        result = get_tweets_with_trending_covid_keywords(message)
        self.assertEqual(result[COVID_TRENDING_KEYWORD_KEY], ['death', "hospitalisation"])


    def test_trending_word_with_no_required_covid_keyword(self):
        message = {'tweet': "many people lost there loved ones "}
        result = get_tweets_with_trending_covid_keywords(message)
        self.assertEqual(result[COVID_TRENDING_KEYWORD_KEY], [])

    def test_trending_word_containing_single_required_economy_keyword(self):
        message = {'tweet': "in covid many people lost there job which become the reason on unemployment"}
        result = get_tweets_with_trending_economy_keywords(message)
        self.assertEqual(result[ECONOMY_TRENDING_KEYWORD_KEY], ['unemployment'])

    def test_trending_word_containing_multiple_required_economy_keyword(self):
        message = {'tweet': "GDP of each country went down at that period and the stock market crash"}
        result = get_tweets_with_trending_economy_keywords(message)
        self.assertEqual(result[ECONOMY_TRENDING_KEYWORD_KEY], ["GDP", "stock", "market"])

    def test_trending_word_with_no_required_economy_keyword(self):
        message = {'tweet': ""}
        result = get_tweets_with_trending_economy_keywords(message)
        self.assertEqual(result[ECONOMY_TRENDING_KEYWORD_KEY], [])

# if __name__ == '__main__':
#     unittest.main()

