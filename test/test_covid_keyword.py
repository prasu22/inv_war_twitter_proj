<<<<<<< HEAD
import unittest
=======
# # country_code_extractor
# # include all the test_case in one file

import unittest

>>>>>>> 29e125c59795c3b81a9c7f4bf54b0b33ab6e5db0
from src.common.variable_files import COVID_KEYWORD_KEY
from src.pub_sub.data_extract.covid_keywords_extractor import get_covid_keywords


class TestCovidKeywords(unittest.TestCase):

    def test_covid_keywords_case_1(self):
<<<<<<< HEAD
        # single match of keyword
        message = {'tweet': 'corona affects many life this year.'}
        result = get_covid_keywords(message)
        self.assertEqual(list(result[COVID_KEYWORD_KEY]), ['corona'])

    def test_covid_keywords_case_2(self):
        # with multiple matching of keywords
        message = {
            'tweet': "coronavirus affects many lifes this year and the worst part is that people don't take covid seriously."}
        result = get_covid_keywords(message)
        self.assertEqual(result[COVID_KEYWORD_KEY], ['coronavirus', 'covid'])

    def test_covid_keywords_case_3(self):
=======

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

>>>>>>> 29e125c59795c3b81a9c7f4bf54b0b33ab6e5db0
        # no keyword found
        message = {'tweet': 'the world is full of lovely people . hi to all'}
        result = get_covid_keywords(message)
        self.assertEqual(result[COVID_KEYWORD_KEY], [])


<<<<<<< HEAD
=======


# if __name__ == '__main__':
#     unittest.main()
>>>>>>> 29e125c59795c3b81a9c7f4bf54b0b33ab6e5db0
