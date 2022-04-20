class StreamListener:
    def __init__(self, config):
        self.config = config

    def fetch_tweets_from_stream(self, keywords: list):
        '''
        fetches tweets from twitter stream API and returns list of tweets in below format
        {
         id:
         tweet_text: <full_text of the tweet>
         author:
         timestamp:
         country:
        }
        :return: list of dictionary
        if rate limit is reached, raises exception ratelimit reached
        '''
        pass

    def fetch_tweets_from_search_api(self, keywords: list, start_date=None, end_date=None):
        '''
        fetches tweets from twitter search API and returns list of tweets in below format
        {
         id:
         tweet_text: <full_text of the tweet>
         author:
         timestamp:
         country:
        }
        :return: list of dictionary
        if rate limit is reached, raises exception ratelimit reached
        '''
        pass

    def dump_in_database(self, tweets: list):
        '''

        :param tweets:
        :return:
        '''
        pass

    def fetch_tweets_from_archive_api(self, keywords: list, start_date=None, end_date=None):
        pass
