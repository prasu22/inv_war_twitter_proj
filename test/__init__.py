
import unittest

from src.common.variable_files import COUNTRY_CODE_KEY, COVID_KEYWORD_KEY
from src.pub_sub.data_extract.country_code_extractor import get_country_code
from src.pub_sub.data_extract.covid_keywords_extractor import get_covid_keywords
from src.common.variable_files import DONATION_AMOUNT_KEY, CURRENCY_NAME_KEY, DONATION_KEYWORDS_KEY
from src.pub_sub.data_extract.donation_data_extractor import get_donation_amount, get_donation_currency,get_donation_keywords
from src.common.variable_files import PREVENTION_KEYWORDS_KEY, WHO_KEYWORDS_KEY
from src.pub_sub.data_extract.preventive_keywords_extractor import get_prevention_keywords, get_who_keywords
from src.common.variable_files import COVID_TRENDING_KEYWORD_KEY, ECONOMY_TRENDING_KEYWORD_KEY
from src.pub_sub.data_extract.trending_keywords_extractor import get_tweets_with_trending_covid_keywords, \
    get_tweets_with_trending_economy_keywords


class TestCountryCode(unittest.TestCase):

    def test_country_code_case_1(self):

        # with correct country name
        message = {'tweet': 'hi', 'country': 'india'}
        result = get_country_code(message)
        self.assertEqual(result[COUNTRY_CODE_KEY],'IN')

    def test_country_code_case_2(self):

        # wrong country name
        message = {'tweet': 'hi', 'country': 'new york'}
        result = get_country_code(message)
        self.assertEqual(result[COUNTRY_CODE_KEY],'No Country')

    def test_country_code_case_3(self):

        # case insenstive testing

        message = {'tweet': 'hi', 'country': 'INDIa'}
        result = get_country_code(message)
        self.assertEqual(result[COUNTRY_CODE_KEY], 'IN')


# if __name__ == '__main__':
#     unittest.main()



class TestCovidKeywords(unittest.TestCase):

    def test_covid_keywords_case_1(self):

        # single match of keyword
        message = {'tweet': 'corona affects many life this year.'}
        result = get_covid_keywords(message)
        self.assertEqual(list(result[COVID_KEYWORD_KEY]),['corona'])

    def test_covid_keywords_case_2(self):

         # with multiple matching of keywords
        message = {'tweet': "coronavirus affects many lifes this year and the worst part is that people don't take covid seriously."}
        result = get_covid_keywords(message)
        self.assertEqual(result[COVID_KEYWORD_KEY],['coronavirus','covid'])

    def test_covid_keywords_case_3(self):

        # no keyword found
        message = {'tweet': 'the world is full of lovely people . hi to all'}
        result = get_covid_keywords(message)
        self.assertEqual(result[COVID_KEYWORD_KEY], [])

# if __name__ == '__main__':
#     unittest.main()

class TestDonationDetails(unittest.TestCase):

    def test_donation_amount_case_1(self):

        # checking currency in INR
        message = {'tweet': 'hi your total amount is 234 INR'}
        result = get_donation_amount(message)
        self.assertEqual(result[DONATION_AMOUNT_KEY],float(234))

    def test_donation_amount_case_2(self):

        # check with space between number and letter like M, B , K
        message = {'tweet': 'hi your total amount is 234 k USD '}
        result = get_donation_amount(message)
        self.assertEqual(result[DONATION_AMOUNT_KEY],float(234))

    def test_donation_amount_case_3(self):

        # check no space between number and letter like M, B , K
        message = {'tweet': 'hi your total amount is 234.345k USD '}
        result = get_donation_amount(message)
        self.assertEqual(result[DONATION_AMOUNT_KEY],float(234.345))

    def test_donation_amount_case_4(self):

        # wrong answer
        message = {'tweet': 'hi your total amount is 234.345'}
        result = get_donation_amount(message)
        self.assertEqual(result[DONATION_AMOUNT_KEY],0)

    def test_donation_amount_case_5(self):

        #wrong
        message = {'tweet': 'hi your total amount is USD '}
        result = get_donation_amount(message)
        self.assertEqual(result[DONATION_AMOUNT_KEY],0)

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
        self.assertEqual(result[CURRENCY_NAME_KEY],'INR')

    def test_currency_name_case_2(self):


        message = {'tweet': 'hi your total amount is 234 inr'}
        result = get_donation_currency(message)
        self.assertEqual(result[CURRENCY_NAME_KEY],'INR')

    def test_currency_name_case_3(self):


        message = {'tweet': 'hi your total amount is 234 k USD '}
        result = get_donation_currency(message)
        self.assertEqual(result[CURRENCY_NAME_KEY],"USD")

    def test_currency_name_case_4(self):

        # check no space between number and letter like M, B , K
        message = {'tweet': 'hi your total amount is 234.345k USD '}
        result = get_donation_currency(message)
        self.assertEqual(result[CURRENCY_NAME_KEY],'USD')

    def test_currency_name_case_5(self):

        # wrong answer
        message = {'tweet': 'hi your total amount is 234.345'}
        result = get_donation_currency(message)
        self.assertEqual(result[CURRENCY_NAME_KEY],'No Currency')

    def test_currency_name_case_6(self):


        message = {'tweet': 'hi your total amount is USD'}
        result = get_donation_currency(message)
        self.assertEqual(result[CURRENCY_NAME_KEY],'No Currency')

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
        self.assertEqual(result[DONATION_KEYWORDS_KEY], ['fund','money'])

    def test_donation_keyword_case_3(self):
        message = {'tweet': 'the world is full of lovely people . hi to all'}
        result = get_donation_keywords(message)
        self.assertEqual(result[DONATION_KEYWORDS_KEY], [])

# if __name__ == '__main__':
#     unittest.main()


class TestPreventionKeywords(unittest.TestCase):

    def test_prevention_keyword_case_1(self):

        message = {'tweet': 'take proper precautions to tackle the covid for that use sanitiser at appropriate time and always wear mask in public.'}
        result = get_prevention_keywords(message)
        self.assertEqual(result[PREVENTION_KEYWORDS_KEY], ['sanitiser','mask'])

    def test_prevention_keyword_case_2(self):
        message = {'tweet': 'wear mask in public.'}
        result = get_prevention_keywords(message)
        self.assertEqual(result[PREVENTION_KEYWORDS_KEY], ['mask'])

    def test_prevention_keyword_case_3(self):
        message = {'tweet': 'hello world . good to see you all.'}
        result = get_prevention_keywords(message)
        self.assertEqual(result[PREVENTION_KEYWORDS_KEY], [])


    def test_who_keyword_case_1(self):
        message = {'tweet': '@WHO suggest that you should stay home'}
        result = get_who_keywords(message)
        self.assertEqual(result[WHO_KEYWORDS_KEY], ['@WHO'])

    def test_who_keyword_case_2(self):

        message = {'tweet': '@WHO suggest that you should stay home #who'}
        result = get_who_keywords(message)
        self.assertEqual(result[WHO_KEYWORDS_KEY], ['@WHO','#who'])

    def test_who_keyword_case_3(self):

        message = {'tweet': 'hello world . how are you '}
        result = get_who_keywords(message)
        self.assertEqual(result[WHO_KEYWORDS_KEY], [])


# if __name__ == '__main__':
#     unittest.main()


class TestTrendingKeywords(unittest.TestCase):
    def test_trending_word_containing_single_required_covid_keyword(self):
        message = {'tweet':"in 2021 the death count of people due to corona is very high"}
        result = get_tweets_with_trending_covid_keywords(message)
        self.assertEqual(result[COVID_TRENDING_KEYWORD_KEY],['death'])

    def test_trending_word_containing_multiple_required_covid_keyword(self):
        message = {'tweet':"in 2021 the death count of people due to corona is very high and many people need hospitalisation at that time"}
        result = get_tweets_with_trending_covid_keywords(message)
        self.assertEqual(result[COVID_TRENDING_KEYWORD_KEY],['death',"hospitalisation"])

    def test_trending_word_with_no_required_covid_keyword(self):
        message = {'tweet': "many people lost there loved ones "}
        result = get_tweets_with_trending_covid_keywords(message)
        self.assertEqual(result[COVID_TRENDING_KEYWORD_KEY], [])

    def test_trending_word_containing_single_required_economy_keyword(self):
        message = {'tweet':"in covid many people lost there job which become the reason on unemployment"}
        result = get_tweets_with_trending_economy_keywords(message)
        self.assertEqual(result[ECONOMY_TRENDING_KEYWORD_KEY],['unemployment'])

    def test_trending_word_containing_multiple_required_economy_keyword(self):
        message = {'tweet':"GDP of each country went down at that period and the stock market crash"}
        result = get_tweets_with_trending_economy_keywords(message)
        self.assertEqual(result[ECONOMY_TRENDING_KEYWORD_KEY],["GDP","stock","market"])

    def test_trending_word_with_no_required_economy_keyword(self):
        message = {'tweet': ""}
        result = get_tweets_with_trending_economy_keywords(message)
        self.assertEqual(result[ECONOMY_TRENDING_KEYWORD_KEY], [])

# if __name__ == '__main__':
#     unittest.main()


