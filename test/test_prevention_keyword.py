import unittest
<<<<<<< HEAD
=======

>>>>>>> 29e125c59795c3b81a9c7f4bf54b0b33ab6e5db0
from src.common.variable_files import PREVENTION_KEYWORDS_KEY, WHO_KEYWORDS_KEY
from src.pub_sub.data_extract.preventive_keywords_extractor import get_prevention_keywords, get_who_keywords


class TestPreventionKeywords(unittest.TestCase):

    def test_prevention_keyword_case_1(self):
<<<<<<< HEAD
        message = {
            'tweet': 'take proper precautions to tackle the covid for that use sanitiser at appropriate time and always wear mask in public.'}
        result = get_prevention_keywords(message)
        self.assertEqual(result[PREVENTION_KEYWORDS_KEY], ['sanitiser', 'mask'])
=======

        message = {'tweet': 'take proper precautions to tackle the covid for that use sanitiser at appropriate time and always wear mask in public.'}
        result = get_prevention_keywords(message)
        self.assertEqual(result[PREVENTION_KEYWORDS_KEY], ['sanitiser','mask'])
>>>>>>> 29e125c59795c3b81a9c7f4bf54b0b33ab6e5db0

    def test_prevention_keyword_case_2(self):
        message = {'tweet': 'wear mask in public.'}
        result = get_prevention_keywords(message)
        self.assertEqual(result[PREVENTION_KEYWORDS_KEY], ['mask'])

    def test_prevention_keyword_case_3(self):
        message = {'tweet': 'hello world . good to see you all.'}
        result = get_prevention_keywords(message)
        self.assertEqual(result[PREVENTION_KEYWORDS_KEY], [])

<<<<<<< HEAD
=======

>>>>>>> 29e125c59795c3b81a9c7f4bf54b0b33ab6e5db0
    def test_who_keyword_case_1(self):
        message = {'tweet': '@WHO suggest that you should stay home'}
        result = get_who_keywords(message)
        self.assertEqual(result[WHO_KEYWORDS_KEY], ['@WHO'])

    def test_who_keyword_case_2(self):
<<<<<<< HEAD
        message = {'tweet': '@WHO suggest that you should stay home #who'}
        result = get_who_keywords(message)
        self.assertEqual(result[WHO_KEYWORDS_KEY], ['@WHO', '#who'])

    def test_who_keyword_case_3(self):
=======

        message = {'tweet': '@WHO suggest that you should stay home #who'}
        result = get_who_keywords(message)
        self.assertEqual(result[WHO_KEYWORDS_KEY], ['@WHO','#who'])

    def test_who_keyword_case_3(self):

>>>>>>> 29e125c59795c3b81a9c7f4bf54b0b33ab6e5db0
        message = {'tweet': 'hello world . how are you '}
        result = get_who_keywords(message)
        self.assertEqual(result[WHO_KEYWORDS_KEY], [])

<<<<<<< HEAD
=======

>>>>>>> 29e125c59795c3b81a9c7f4bf54b0b33ab6e5db0
# if __name__ == '__main__':
#     unittest.main()
