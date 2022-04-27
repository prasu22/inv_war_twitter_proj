from pub_sub.data_extraction.extract_country_code import get_country_code
from pub_sub.data_extraction.extract_keyword_from_tweets import get_keywords
from pub_sub.data_extraction.extract_tweets_by_keywords import get_tweets_with_keyword

prevention_list = ['mask','sanitiser','stay home','social distancing','wash hands']
list_of_who = ['#who','@who']

def top_10_preventions(message,db):

    prevention_data = get_tweets_with_keyword(message,prevention_list)
    who_word = get_tweets_with_keyword(message,list_of_who)
    country_data = get_country_code(message)

    list_of_words = get_keywords(message,prevention_list)

    if prevention_data and who_word and country_data:
        country_code = country_data['country_code']
        country = country_data['country']
        for words in list_of_words:
            word = words.title()
            if db['top_10_prevention_country_code'].count_documents({"word":word,"country_code":country_code})==0:
                db['top_10_prevention_country_code'].insert_one({'word':word,"country_code":country_code,'country':country,'count':1})
            else:
                db['top_10_prevention_country_code'].update_one({'word': word, "country_code": country_code},{'$inc':{'count':1}})
        print("inserted_success fully")



# top_10_preventions({'tweet':'coronavirus and is my sajshjjd asdas mask de sanitiser , #who wash a hands','country':'united kingdom','created_at':'2022-04-27 06:54:04','id':'123'})