import unittest

from test.test_country_code import TestCountryCode
from test.test_covid_keyword import TestCovidKeywords
from test.test_donation_amount_and_currency import TestDonationDetails
from test.test_prevention_keyword import TestPreventionKeywords
from test.test_trending_keywords import TestTrendingKeywords
from test.test_encryption_and_decryption import TestEncryptionDecryptionMethod
# from test.test_tweet_keywords_extractor import TestTweetKeywords




class TestAllConsumers(unittest.TestCase):

    # call all the classes from all the test module
    ## this is for testing all files

    TestCountryCode
    TestCovidKeywords
    TestDonationDetails
    TestPreventionKeywords
    TestTrendingKeywords
    TestEncryptionDecryptionMethod
    # TestTweetKeywords


if __name__ == '__main__':
    unittest.main()
