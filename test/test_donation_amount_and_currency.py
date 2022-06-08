import unittest
from src.common.variable_files import DONATION_AMOUNT_KEY, CURRENCY_NAME_KEY, DONATION_KEYWORDS_KEY
from src.pub_sub.data_extract.donation_data_extractor import get_donation_amount, get_donation_currency, \
    get_donation_keywords


class TestDonationDetails(unittest.TestCase):

    def test_donation_amount_case_1(self):
        # checking currency in INR
        message = {'tweet': 'hi your total amount is 234 INR'}
        result = get_donation_amount(message)
        self.assertEqual(result[DONATION_AMOUNT_KEY], float(234))

    def test_donation_amount_case_2(self):
        # check with space between number and letter like M, B , K
        message = {'tweet': 'hi your total amount is 234 k USD '}
        result = get_donation_amount(message)
        self.assertEqual(result[DONATION_AMOUNT_KEY], float(234))

    def test_donation_amount_case_3(self):
        # check no space between number and letter like M, B , K
        message = {'tweet': 'hi your total amount is 234.345k USD '}
        result = get_donation_amount(message)
        self.assertEqual(result[DONATION_AMOUNT_KEY], float(234.345))

    def test_donation_amount_case_4(self):
        # wrong answer
        message = {'tweet': 'hi your total amount is 234.345'}
        result = get_donation_amount(message)
        self.assertEqual(result[DONATION_AMOUNT_KEY], 0)

    def test_donation_amount_case_5(self):
        # wrong
        message = {'tweet': 'hi your total amount is USD '}
        result = get_donation_amount(message)
        self.assertEqual(result[DONATION_AMOUNT_KEY], 0)

    def test_donation_amount_case_6(self):
        # with symbol
        message = {'tweet': 'hi your total amount is $234.345'}
        result = get_donation_amount(message)
        self.assertEqual(result[DONATION_AMOUNT_KEY], float(234.345))

    def test_donation_amount_case_7(self):
        # with symbol and commas
        message = {'tweet': 'hi your total amount is ₹ 2,34,345'}
        result = get_donation_amount(message)
        self.assertEqual(result[DONATION_AMOUNT_KEY], float(234345))

    def test_currency_name_case_1(self):
        # check currency name
        message = {'tweet': 'hi your total amount is ₹ 2,34,345'}
        result = get_donation_currency(message)
        self.assertEqual(result[CURRENCY_NAME_KEY], 'INR')

    def test_currency_name_case_2(self):
        message = {'tweet': 'hi your total amount is 234 inr'}
        result = get_donation_currency(message)
        self.assertEqual(result[CURRENCY_NAME_KEY], 'INR')

    def test_currency_name_case_3(self):
        message = {'tweet': 'hi your total amount is 234 k USD '}
        result = get_donation_currency(message)
        self.assertEqual(result[CURRENCY_NAME_KEY], "USD")

    def test_currency_name_case_4(self):
        # check no space between number and letter like M, B , K
        message = {'tweet': 'hi your total amount is 234.345k USD '}
        result = get_donation_currency(message)
        self.assertEqual(result[CURRENCY_NAME_KEY], 'USD')

    def test_currency_name_case_5(self):
        # wrong answer
        message = {'tweet': 'hi your total amount is 234.345'}
        result = get_donation_currency(message)
        self.assertEqual(result[CURRENCY_NAME_KEY], 'no currency')


    def test_currency_name_case_6(self):
        message = {'tweet': 'hi your total amount is USD'}
        result = get_donation_currency(message)
        self.assertEqual(result[CURRENCY_NAME_KEY], 'no currency')

    def test_currency_name_case_7(self):
        message = {'tweet': 'hi your total amount is $234.345'}
        result = get_donation_currency(message)
        self.assertEqual(result[CURRENCY_NAME_KEY], 'USD')

    def test_donation_keyword_case_1(self):
        message = {'tweet': 'donate 2000 INR to the covid affected person.'}
        result = get_donation_keywords(message)
        self.assertEqual(result[DONATION_KEYWORDS_KEY], ['donate'])

    def test_donation_keyword_case_2(self):
        message = {'tweet': 'please raise some fund to covid people as they need money desperately.'}
        result = get_donation_keywords(message)
        self.assertEqual(result[DONATION_KEYWORDS_KEY], ['fund', 'money'])

    def test_donation_keyword_case_3(self):
        message = {'tweet': 'the world is full of lovely people . hi to all'}
        result = get_donation_keywords(message)
        self.assertEqual(result[DONATION_KEYWORDS_KEY], [])


