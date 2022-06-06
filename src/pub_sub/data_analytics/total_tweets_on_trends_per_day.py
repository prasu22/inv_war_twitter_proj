
import logging

LOGGER = logging.getLogger(__name__)
from datetime import datetime
from src.common.variable_files import COLL_OF_RANKING_COUNTRY_BASED_ON_TWEET_WITH_ECONOMY_KEYS, \
    COLL_OF_RANKING_COUNTRY_BASED_ON_TWEET_WITH_COVID_KEYS, ECONOMY_VALUE
from src.common.variable_files import COUNTRY_NAME_KEY, COUNTRY_CODE_KEY, CREATED_AT_KEY, \
    COVID_TRENDING_KEYWORD_KEY, ECONOMY_TRENDING_KEYWORD_KEY, TREND_KEY, COVID_VALUE, COUNT_KEY



def analysis_overall_tweets_based_on_trends_per_day(message, output_dictionary, db):
    """
           store the data in collection after based on  trend  in different collection like if trend is covid than data insert in impact_analysis_on_covid_keys collection or if trend is economy then data insert in impact_analysis_on_economy_keys to give the rank based on impact
           :collection schema
         {
           _id: objectid
           country:string
           country_code:string
           count: int
           trend: string
        }
           :passing argument
           message : dictionary storing information of tweet
           db: store the database
           :param
           country_name = store the country name
           covid_trend_data = store the list of keyword related to covid present in tweet message
           economy_trend_data = store the list of the keyword related to economy present in tweet message
           country_code = store the list of the country code
           new_dt = store the date in string format
           created_at = store the date in date object

    """
    try:
        covid_trend_data = message[COVID_TRENDING_KEYWORD_KEY]
        economy_trend_data = message[ECONOMY_TRENDING_KEYWORD_KEY]
        country_name = message[COUNTRY_NAME_KEY]
        country_code = message[COUNTRY_CODE_KEY]
        new_dt = str(message[CREATED_AT_KEY])[:19]
        created_at = datetime.strptime(new_dt, '%Y-%m-%d %H:%M:%S').date()
        if len(covid_trend_data) > 0:
            if db[COLL_OF_RANKING_COUNTRY_BASED_ON_TWEET_WITH_COVID_KEYS].count_documents(
                    {COUNTRY_NAME_KEY: country_name, "created_at": str(created_at)}) == 0:
                db[COLL_OF_RANKING_COUNTRY_BASED_ON_TWEET_WITH_COVID_KEYS].insert_one(
                    {COUNTRY_NAME_KEY: country_name, COUNTRY_CODE_KEY: country_code, COUNT_KEY: 1,
                     TREND_KEY: COVID_VALUE,
                     "created_at": str(created_at)})
                #################################################
                trend_key = 'covid'
                output_key = str(created_at) + ' ' + str(country_code)
                if trend_key not in output_dictionary:
                    output_dictionary[trend_key] = [{output_key: 1}]
                else:
                    list_of_words = output_dictionary[trend_key]
                    flag = 0
                    for frequency in list_of_words:
                        if output_key in frequency.keys():
                            frequency[output_key] += 1
                            flag = 1
                            break
                    if flag == 0:
                        output_dictionary[trend_key].append({output_key: 1})

            else:
                db[COLL_OF_RANKING_COUNTRY_BASED_ON_TWEET_WITH_COVID_KEYS].update_one(
                    {COUNTRY_NAME_KEY: country_name, "created_at": str(created_at)},
                    {"$inc": {COUNT_KEY: 1}})

                trend_key = 'covid'
                output_key = str(created_at) + ' ' + str(country_code)
                if trend_key not in output_dictionary:
                    output_dictionary[trend_key] = [{output_key: 1}]
                else:
                    list_of_words = output_dictionary[trend_key]
                    flag = 0
                    for frequency in list_of_words:
                        if output_key in frequency.keys():
                            frequency[output_key] += 1
                            flag = 1
                            break
                    if flag == 0:
                        output_dictionary[trend_key].append({output_key: 1})

        else:
            LOGGER.info(f"MESSAGE:Data is not found! ")

        if len(economy_trend_data) > 0:
            # 7th query
            if db[COLL_OF_RANKING_COUNTRY_BASED_ON_TWEET_WITH_ECONOMY_KEYS].count_documents(
                    {COUNTRY_NAME_KEY: country_name, "created_at": str(created_at)}) == 0:
                db[COLL_OF_RANKING_COUNTRY_BASED_ON_TWEET_WITH_ECONOMY_KEYS].insert_one(
                    {COUNTRY_NAME_KEY: country_name, COUNTRY_CODE_KEY: country_code, COUNT_KEY: 1,
                     TREND_KEY: ECONOMY_VALUE, "created_at": str(created_at)})

                trend_key = 'economy'
                output_key = str(created_at) + ' ' + str(country_code)
                if trend_key not in output_dictionary:
                    output_dictionary[trend_key] = [{output_key: 1}]
                else:
                    list_of_words = output_dictionary[trend_key]
                    flag = 0
                    for frequency in list_of_words:
                        if output_key in frequency.keys():
                            frequency[output_key] += 1
                            flag = 1
                            break
                    if flag == 0:
                        output_dictionary[trend_key].append({output_key: 1})

            else:
                db[COLL_OF_RANKING_COUNTRY_BASED_ON_TWEET_WITH_ECONOMY_KEYS].update_one(
                    {COUNTRY_NAME_KEY: country_name, "created_at": str(created_at)}, {"$inc": {COUNT_KEY: 1}})

                trend_key = 'economy'
                output_key = str(created_at) + ' ' + str(country_code)
                if trend_key not in output_dictionary:
                    output_dictionary[trend_key] = [{output_key: 1}]
                else:
                    list_of_words = output_dictionary[trend_key]
                    flag = 0
                    for frequency in list_of_words:
                        if output_key in frequency.keys():
                            frequency[output_key] += 1
                            flag = 1
                            break
                    if flag == 0:
                        output_dictionary[trend_key].append({output_key: 1})
        else:
            LOGGER.info(f"MESSAGE:Data is not found! ")

    except Exception as e:
        LOGGER.error(f"ERROR:{e} ")


def updated_trend_list_total_tweets(tweet_list, db):
    output_dictionary = {}
    for message in tweet_list:
        analysis_overall_tweets_based_on_trends_per_day(message, output_dictionary, db)
    return output_dictionary
