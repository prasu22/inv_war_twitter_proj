import unittest

from src.common.variable_files import COUNTRY_CODE_KEY
from src.pub_sub.data_extract.country_code_extractor import get_country_code


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


if __name__ == '__main__':
    unittest.main()
