import unittest
<<<<<<< HEAD
=======
import os


>>>>>>> 29e125c59795c3b81a9c7f4bf54b0b33ab6e5db0
from src.common.variable_files import COUNTRY_CODE_KEY
from src.pub_sub.data_extract.country_code_extractor import get_country_code


class TestCountryCode(unittest.TestCase):

    def test_country_code_case_1(self):
<<<<<<< HEAD
        # with correct country name
        message = {'tweet': 'hi', 'country': 'india'}
        result = get_country_code(message)
        self.assertEqual(result[COUNTRY_CODE_KEY], 'IN')

    def test_country_code_case_2(self):
        # wrong country name
        message = {'tweet': 'hi', 'country': 'new york'}
        result = get_country_code(message)
        self.assertEqual(result[COUNTRY_CODE_KEY], 'no country code')


    def test_country_code_case_3(self):
=======

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

>>>>>>> 29e125c59795c3b81a9c7f4bf54b0b33ab6e5db0
        # case insenstive testing

        message = {'tweet': 'hi', 'country': 'INDIa'}
        result = get_country_code(message)
        self.assertEqual(result[COUNTRY_CODE_KEY], 'IN')


<<<<<<< HEAD
# if __name__ == '__main__':
#     unittest.main()
=======
if __name__ == '__main__':
    unittest.main()
>>>>>>> 29e125c59795c3b81a9c7f4bf54b0b33ab6e5db0
