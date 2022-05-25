import logging
LOGGER = logging.getLogger(__name__)
from datetime import datetime
from src.common.variable_files import COLL_OF_TWEET_PER_COUNTRY_ON_DAILY_BASIS
from src.common.variable_files import COUNTRY_NAME_KEY, COUNTRY_CODE_KEY, CREATED_AT_KEY, COUNT_KEY


COLL_OF_TWEET_PER_COUNTRY_ON_DAILY_BASIS ='tweet_daily_basis'


def analysis_total_tweet_per_country(message,output_dictionary,db):
    """
        store the data in collection after manupulation in mongodb collection overall_tweet_per_country_on_daily_basis
        :collection schema
         {
           _id: objectid
           country:string
           country_code: string
           count: int
           date: string
        }
        :passing argument
        message : dictionary storing information of tweet
        db: store the database
        :param
        new_dt = data_extract only date and time in string format
        created_at = convert string date object format
        country_name = store the country name
        country_code = store the country code

    """
    try:
        new_dt = str(message[CREATED_AT_KEY])[:19]
        created_at = datetime.strptime(new_dt, '%Y-%m-%d %H:%M:%S').date()
        country_name = message[COUNTRY_NAME_KEY]
        country_code = message[COUNTRY_CODE_KEY]

        if db[COLL_OF_TWEET_PER_COUNTRY_ON_DAILY_BASIS].count_documents(
                {COUNTRY_NAME_KEY: country_name, "created_at": str(created_at)}) == 0:
            db[COLL_OF_TWEET_PER_COUNTRY_ON_DAILY_BASIS].insert_one(
                {COUNT_KEY: 1, COUNTRY_NAME_KEY: country_name, COUNTRY_CODE_KEY: country_code, 'created_at': str(created_at)})

            country_code_date = country_code + ' ' + str(created_at)
            if country_code_date not in output_dictionary:
                output_dictionary[country_code_date] = 1
            else:
                output_dictionary[country_code_date] += 1

        else:
            db[COLL_OF_TWEET_PER_COUNTRY_ON_DAILY_BASIS].update_one(
                {COUNTRY_NAME_KEY: country_name, "created_at": str(created_at)},
                {'$inc': {COUNT_KEY: 1}})

            country_code_date = country_code + ' ' + str(created_at)

            if country_code_date not in output_dictionary:
                output_dictionary[country_code_date] = 1
            else:
                output_dictionary[country_code_date] += 1

    except Exception as e:
        LOGGER.error(f"ERROR:{e} ")


def updated_list_daily_tweets(tweet_list,db):
    output_dictionary = {}
    for message in tweet_list:
        analysis_total_tweet_per_country(message,output_dictionary,db)
    return output_dictionary



