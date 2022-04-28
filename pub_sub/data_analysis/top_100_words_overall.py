from pub_sub.data_extraction.extract_all_words import get_all_the_words
from pub_sub.data_extraction.extract_country_code import get_country_code
from pub_sub.data_extraction.extract_tweets_by_keywords import get_tweets_with_keyword



#  query 3 and 4

def analysis_top_100_words(message,db):
    """
        store the data in collection after manupulation in mongodb collection  top_100_words
        :collection schema
         {
           _id: objectid
           country:string
           count: int
           word: string
        }
        :passing argument
        message : dictionary storing information of tweet
        :param
        country = store the country name
        list_of_words = store the string of words with separated by _
    """
    # print("query 3 called")
    keyword = ['coronavirus','covid','corona']
    result = get_tweets_with_keyword(message,keyword)
    list_of_words = get_all_the_words(message)
    country_data= get_country_code(message)

    if result and country_data:
        country_code = country_data['country_code']
        country = country_data['country']
        if len(list_of_words) > 0:
            for words in list_of_words.split(" "):
                if db['a_top_100_words'].count_documents({"word": words.title(), "country": country}) == 0:
                    db['a_top_100_words'].insert_one({'word': words.title(), "country": country, 'country_code':country_code,'count': 1})
                else:
                    db['a_top_100_words'].update_one({'word': words.title(), "country": country},
                                                   {'$inc': {'count': 1}})
                # print(words)
            return True
    else:
        return False
# preprocess_top_100_words({'tweet':'coronavirus and is my sajshjjd asdas de','country':'united kingdom','created_at':'2022-04-27 06:54:04','id':'123'})
