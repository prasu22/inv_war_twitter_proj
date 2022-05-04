import unittest

from test.test_country_code import TestCountryCode
from test.test_covid_keyword import TestCovidKeywords
from test.test_donation_amount_and_currency import TestDonationDetails
from test.test_prevention_keyword import TestPreventionKeywords
from test.test_trending_keywords import TestTrendingKeywords


class TestAllConsumers(unittest.TestCase):

    # call all the classes from all the test module
    TestCountryCode
    TestCovidKeywords
    TestDonationDetails
    TestPreventionKeywords
    TestTrendingKeywords



if __name__ == '__main__':
    unittest.main()
