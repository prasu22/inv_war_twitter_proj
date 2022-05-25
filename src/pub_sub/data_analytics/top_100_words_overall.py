import logging
LOGGER = logging.getLogger(__name__)
from src.common.variable_files import COLL_OF_WORDS_FROM_TWEETS, TWEET_KEYWORDS, IS_COVID_TWEET
from src.common.variable_files import COUNTRY_NAME_KEY ,COUNTRY_CODE_KEY  ,WORD_KEY,COUNT_KEY

COLL_OF_WORDS_FROM_TWEETS = 'get_words_top'

def analysis_top_100_words(message,output_dictionary,db):
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
        db: store the database
        :param
        result = store the list of covid keywords present in the message
        country_name = store the country name present in message
        list_of_words = store the string of words clean from the
        country_code =  store the country code
    """
    try:
        result = message[IS_COVID_TWEET]
        list_of_words = message[TWEET_KEYWORDS]
        country_name = message[COUNTRY_NAME_KEY]
        country_code = message[COUNTRY_CODE_KEY]
        print("list_of_words",list_of_words)
        if result:
            for words in list_of_words:
                if db[COLL_OF_WORDS_FROM_TWEETS].count_documents({WORD_KEY: words.title(), COUNTRY_CODE_KEY: country_code}) == 0:
                    db[COLL_OF_WORDS_FROM_TWEETS].insert_one(
                        {WORD_KEY: words.title(), COUNTRY_NAME_KEY: country_name, COUNTRY_CODE_KEY: country_code, COUNT_KEY: 1})

                    ### for validation
                    country_code_key = country_code
                    if country_code_key not in output_dictionary:
                        output_dictionary[country_code_key] = [{words: 1}]
                    else:
                        list_of_words = output_dictionary[country_code_key]
                        flag = 0
                        for frequency in list_of_words:
                            if words in frequency.keys():
                                frequency[words] += 1
                                flag = 1
                                break
                        if flag == 0:
                            output_dictionary[country_code_key].append({words: 1})

                else:
                    db[COLL_OF_WORDS_FROM_TWEETS].update_one({WORD_KEY: words.title(), COUNTRY_CODE_KEY: country_code},
                                                             {'$inc': {COUNT_KEY: 1}})

                    ## for validation
                    country_code_key = country_code
                    if country_code_key not in output_dictionary:
                        output_dictionary[country_code_key] = [{words: 1}]
                    else:
                        list_of_words = output_dictionary[country_code_key]
                        flag = 0
                        for frequency in list_of_words:
                            if words in frequency.keys():
                                frequency[words] += 1
                                flag = 1
                                break
                        if flag == 0:
                            output_dictionary[country_code_key].append({words: 1})


        else:
            LOGGER.info(f"MESSAGE:Data is not found! ")
    except Exception as e:
        LOGGER.error(f"ERROR:{e} ")



def updated_list_top_words(tweet_list,db):

    output_dictionary = {}
    for message in tweet_list:
        analysis_top_100_words(message,output_dictionary,db)
    return output_dictionary