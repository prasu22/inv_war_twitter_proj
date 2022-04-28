

def analysis_top_10_preventions(message,db):

    prevention_data = message['prevention_keywords']
    who_word = message['WHO_keywords']
    country_name = message['country']
    country_code = message['country_code']

    if len(prevention_data) > 0 and len(who_word) > 0:
        for words in prevention_data:
            word = words.title()
            if db['a_top_10_prevention_country_code'].count_documents({"word":word,"country_code":country_code})==0:
                db['a_top_10_prevention_country_code'].insert_one({'word':word,"country_code":country_code,'country':country_name,'count':1})
            else:
                db['a_top_10_prevention_country_code'].update_one({'word': word, "country_code": country_code},{'$inc':{'count':1}})
        print("i am in analysis_top_10_preventions")



# top_10_preventions({'tweet':'coronavirus and is my sajshjjd asdas mask de sanitiser , #who wash a hands','country':'united kingdom','created_at':'2022-04-27 06:54:04','id':'123'})