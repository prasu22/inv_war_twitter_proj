import logging

# from src.mongodb.mongo_data_connector import mongodb_connection

LOGGER = logging.getLogger(__name__)
from datetime import datetime
from src.common.variable_files import COLL_OF_TOTAL_TWEET_PER_COUNTRY, CREATED_AT_KEY, COVID_KEYWORD_KEY, \
    COUNTRY_NAME_KEY, COUNTRY_CODE_KEY, MONTH_KEY, COUNT_KEY, YEAR_KEY



COLL_OF_TOTAL_TWEET_PER_COUNTRY = 'total_tweets_country'


def overall_tweets_country_wise(message,output_dictionary,db):

    """
        store the data in collection after manupulation in mongodb collection overall_tweet_per_country
        :collection schema
         {
           _id: objectid
           country:string
           country_code: string
           count: int
           month: int
        }
        :passing argument
        message : dictionary storing information of tweet
        :param
        new_dt = data_extract only date and time in string format
        created_at = convert string date object format
        month = data_extract the month number and store in this variable
        result = store the list of covid keywords
        country_name =  store the country name
        country_code = store the country code
        
        
    """


    try:

        new_dt = str(message[CREATED_AT_KEY])[:19]
        created_at = datetime.strptime(new_dt, '%Y-%m-%d %H:%M:%S').date()
        month = created_at.month
        year = created_at.year
        result = len(message[COVID_KEYWORD_KEY])
        country_name = message[COUNTRY_NAME_KEY]
        country_code = message[COUNTRY_CODE_KEY]

        if result > 0:

            if db[COLL_OF_TOTAL_TWEET_PER_COUNTRY].count_documents({COUNTRY_NAME_KEY: country_name, MONTH_KEY: month,YEAR_KEY: year}) == 0:
                db[COLL_OF_TOTAL_TWEET_PER_COUNTRY].insert_one(
                    {COUNT_KEY: 1, COUNTRY_NAME_KEY: country_name, COUNTRY_CODE_KEY: country_code, MONTH_KEY: month,YEAR_KEY: year})

                country_code_month_year = country_code + ' ' + str(month) + ' ' + str(year)
                if country_code_month_year not in output_dictionary:
                    output_dictionary[country_code_month_year] = 1
                else:
                    output_dictionary[country_code_month_year] += 1

            else:
                db[COLL_OF_TOTAL_TWEET_PER_COUNTRY].update_one({COUNTRY_NAME_KEY: country_name, MONTH_KEY: month,YEAR_KEY: year},
                                                               {'$inc': {COUNT_KEY: 1}})

                country_code_month_year = country_code + ' ' + str(month) + ' ' + str(year)
                if country_code_month_year not in output_dictionary:
                    output_dictionary[country_code_month_year] = 1
                else:
                    output_dictionary[country_code_month_year] += 1

        else:
            LOGGER.info(f"MESSAGE:Data is not found! ")
    except Exception as e:
        LOGGER.error(f"ERROR:{e} ")


def updated_list_total_tweets(tweet_list,db):
    output_dictionary = {}
    for message in tweet_list:
        overall_tweets_country_wise(message,output_dictionary,db)
    return output_dictionary



