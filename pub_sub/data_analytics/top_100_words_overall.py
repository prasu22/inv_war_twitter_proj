from common_variables.variable_files import COLL_OF_WORDS_FROM_TWEETS
from pub_sub.data_analytics.data_cleaning import clean_tweet
from common_variables.variable_files import COUNTRY_NAME_KEY ,COUNTRY_CODE_KEY ,TWEET_KEY ,COVID_KEYWORD_KEY ,WORD_KEY,COUNT_KEY


def analysis_top_100_words(message, db):
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

    result = message[COVID_KEYWORD_KEY]
    list_of_words = clean_tweet(message[TWEET_KEY])
    country_name = message[COUNTRY_NAME_KEY]
    country_code = message[COUNTRY_CODE_KEY]
    if len(result) > 0:
        for words in list_of_words.split(" "):
            if db[COLL_OF_WORDS_FROM_TWEETS].count_documents({WORD_KEY: words.title(), COUNTRY_CODE_KEY: country_name}) == 0:
                db[COLL_OF_WORDS_FROM_TWEETS].insert_one(
                    {WORD_KEY: words.title(), COUNTRY_NAME_KEY: country_name, COUNTRY_CODE_KEY: country_code, COUNT_KEY: 1})
            else:
                db[COLL_OF_WORDS_FROM_TWEETS].update_one({WORD_KEY: words.title(), COUNTRY_NAME_KEY: country_name},
                                                         {'$inc': {COUNT_KEY: 1}})
    else:
        print("data is not found")
