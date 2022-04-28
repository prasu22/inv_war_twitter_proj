from pub_sub.data_analytics.data_cleaning import clean_tweet




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
    result = message['covid_keywords']
    list_of_words = clean_tweet(message['tweet'])
    country_name = message['country']
    country_code = message['country_code']
    if len(result) > 0:
        for words in list_of_words.split(" "):
            if db['a_top_100_words'].count_documents({"word": words.title(), "country": country_name}) == 0:
                db['a_top_100_words'].insert_one({'word': words.title(), "country": country_name, 'country_code':country_code,'count': 1})
            else:
                db['a_top_100_words'].update_one({'word': words.title(), "country": country_name},
                                               {'$inc': {'count': 1}})
                # print(words)
        print("i am analysis_top_100_words")
# preprocess_top_100_words({'tweet':'coronavirus and is my sajshjjd asdas de','country':'united kingdom','created_at':'2022-04-27 06:54:04','id':'123'})
