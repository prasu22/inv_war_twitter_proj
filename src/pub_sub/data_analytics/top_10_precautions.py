import logging
LOGGER = logging.getLogger(__name__)
from src.common.variable_files import COLL_OF_TOP_10_PREVENTIVE_WORDS
from src.common.variable_files import COUNTRY_NAME_KEY ,COUNTRY_CODE_KEY ,PREVENTION_KEYWORDS_KEY ,WHO_KEYWORDS_KEY ,WORD_KEY,COUNT_KEY



def analysis_top_10_preventions(message, output_dictionary,db):
    """
               store the data in collection after manupulation in mongodb collection top_10_preventive_words
               :collection schema
                 {
                   _id: objectid
                   country:string
                   country_code: string
                   count: int
                   word: string
                }
               :passing argument
               message : dictionary storing information of tweet
               db : store the database
               :param
               prevention_data = store the list of prevention keyword present in tweet
               who_word = store the list of WHO keyword present in tweet
               country_name = store the country name present in message
               country_code = store the country code
        """
    try:
        prevention_data = message[PREVENTION_KEYWORDS_KEY]
        who_word = message[WHO_KEYWORDS_KEY]
        country_name = message[COUNTRY_NAME_KEY]
        country_code = message[COUNTRY_CODE_KEY]

        if len(prevention_data) > 0 and len(who_word) > 0:
            for words in prevention_data:
                word = words.title()
                if db[COLL_OF_TOP_10_PREVENTIVE_WORDS].count_documents({WORD_KEY: word, COUNTRY_CODE_KEY: country_code}) == 0:
                    db[COLL_OF_TOP_10_PREVENTIVE_WORDS].insert_one(
                        {WORD_KEY: word, COUNTRY_CODE_KEY: country_code, COUNTRY_NAME_KEY: country_name, COUNT_KEY: 1})

                    country_code_key = country_code
                    if country_code_key not in output_dictionary:
                        output_dictionary[country_code_key] = [{word:1}]
                    else:
                        list_of_words = output_dictionary[country_code_key]
                        flag = 0
                        for frequency in list_of_words:
                            if word in frequency.keys():
                                frequency[word] += 1
                                flag = 1
                                break
                        if flag == 0:
                            output_dictionary[country_code_key].append({word:1})

                else:
                    db[COLL_OF_TOP_10_PREVENTIVE_WORDS].update_one({WORD_KEY: word, COUNTRY_CODE_KEY: country_code},
                                                                   {'$inc': {COUNT_KEY: 1}})

                    country_code_key = country_code
                    if country_code_key not in output_dictionary:
                        output_dictionary[country_code_key] = [{word: 1}]
                    else:
                        list_of_words = output_dictionary[country_code_key]
                        flag = 0
                        for frequency in list_of_words:
                            if word in frequency.keys():
                                frequency[word] += 1
                                flag = 1
                                break
                        if flag == 0:
                            output_dictionary[country_code_key].append({word: 1})

        else:
            LOGGER.info(f"MESSAGE:Data is not found! ")
    except Exception as e:
        LOGGER.error(f"ERROR:{e} ")

def updated_list_top_10_precautions(tweet_list,db):

    output_dictionary = {}
    for message in tweet_list:
        analysis_top_10_preventions(message,output_dictionary,db)
    return output_dictionary