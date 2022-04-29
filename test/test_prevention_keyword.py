import unittest

from common_variables.variable_files import PREVENTION_KEYWORDS_KEY, WHO_KEYWORDS_KEY
from pub_sub.data_extract.extract_prevention_keywords import get_prevention_keywords, get_who_keywords


class MyTestCase(unittest.TestCase):

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


if __name__ == '__main__':
    unittest.main()
