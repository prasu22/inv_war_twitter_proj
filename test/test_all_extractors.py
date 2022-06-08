import unittest

from test.test_country_code import TestCountryCode
from test.test_covid_keyword import TestCovidKeywords
from test.test_donation_amount_and_currency import TestDonationDetails
from test.test_prevention_keyword import TestPreventionKeywords
from test.test_trending_keywords import TestTrendingKeywords
from test.test_encryption_and_decryption import TestEncryptionDecryptionMethod

<<<<<<< HEAD
from test.test_tweet_keywords_extractor import TestTweetKeywords
=======
# from test import TestCountryCode
# from test import TestCovidKeywords
# from test import TestDonationDetails
# from test import TestPreventionKeywords
# from test import TestTrendingKeywords
>>>>>>> 29e125c59795c3b81a9c7f4bf54b0b33ab6e5db0


class TestAllConsumers(unittest.TestCase):

    # call all the classes from all the test module
    TestCountryCode
    TestCovidKeywords
    TestDonationDetails
    TestPreventionKeywords
    TestTrendingKeywords
    TestEncryptionDecryptionMethod
<<<<<<< HEAD
    TestTweetKeywords
=======


>>>>>>> 29e125c59795c3b81a9c7f4bf54b0b33ab6e5db0

if __name__ == '__main__':
    unittest.main()
