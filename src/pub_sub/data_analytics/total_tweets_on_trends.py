import logging
LOGGER = logging.getLogger(__name__)
from src.common.variable_files import COLL_OF_IMPACT_ANALYSIS_ON_COVID_KEYS, \
   COLL_OF_IMPACT_ANALYSIS_ON_ECONOMY_KEYS

from src.common.variable_files import COUNTRY_NAME_KEY ,COUNTRY_CODE_KEY ,COVID_TRENDING_KEYWORD_KEY ,ECONOMY_TRENDING_KEYWORD_KEY,TREND_KEY,ECONOMY_VALUE,COVID_VALUE,COUNT_KEY



def analysis_overall_tweets_based_on_trends(message,covid_output_dictionary,economy_output_dictionary,db):
   """
          store the data in collection after based on  trend  in different collection like if trend is covid than data insert in impact_analysis_on_covid_keys collection or if trend is economy then data insert in impact_analysis_on_economy_keys
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

       if len(covid_trend_data) > 0:
           if db[COLL_OF_IMPACT_ANALYSIS_ON_COVID_KEYS].count_documents({COUNTRY_NAME_KEY: country_name, TREND_KEY: COVID_VALUE}) == 0:
               db[COLL_OF_IMPACT_ANALYSIS_ON_COVID_KEYS].insert_one(
                   {COUNTRY_NAME_KEY: country_name, COUNTRY_CODE_KEY: country_code, COUNT_KEY: 1, TREND_KEY: COVID_VALUE})
               covid_country_code = country_code
               if covid_country_code not in covid_output_dictionary:
                   covid_output_dictionary[covid_country_code] = 1
               else:
                   covid_output_dictionary[covid_country_code] += 1
           else:
               db[COLL_OF_IMPACT_ANALYSIS_ON_COVID_KEYS].update_one({COUNTRY_NAME_KEY: country_name, TREND_KEY: COVID_VALUE},
                                                                    {"$inc": {COUNT_KEY: 1}})
               covid_country_code = country_code
               if covid_country_code not in covid_output_dictionary:
                   covid_output_dictionary[covid_country_code] = 1
               else:
                   covid_output_dictionary[covid_country_code] += 1
       else:
           LOGGER.info(f"MESSAGE:Data is not found! ")

       if len(economy_trend_data) > 0:
           # 8th query
           if db[COLL_OF_IMPACT_ANALYSIS_ON_ECONOMY_KEYS].count_documents(
                   {COUNTRY_NAME_KEY: country_name, TREND_KEY: ECONOMY_VALUE}) == 0:
               db[COLL_OF_IMPACT_ANALYSIS_ON_ECONOMY_KEYS].insert_one(
                   {COUNTRY_NAME_KEY: country_name, COUNTRY_CODE_KEY: country_code, COUNT_KEY: 1, TREND_KEY: ECONOMY_VALUE})
               economy_country_code = country_code
               if economy_country_code not in economy_output_dictionary:
                   economy_output_dictionary[economy_country_code] = 1
               else:
                   economy_output_dictionary[economy_country_code] += 1
           else:
               db[COLL_OF_IMPACT_ANALYSIS_ON_ECONOMY_KEYS].update_one({COUNTRY_NAME_KEY: country_name, TREND_KEY: ECONOMY_VALUE},
                                                                      {"$inc": {COUNT_KEY: 1}})
               economy_country_code = country_code
               if economy_country_code not in economy_output_dictionary:
                   economy_output_dictionary[economy_country_code] = 1
               else:
                   economy_output_dictionary[economy_country_code] += 1

       else:
           LOGGER.info(f"MESSAGE:Data  not found! ")

   except Exception as e:
       LOGGER.error(f"ERROR:{e} ")


def updated_covid_list_total_tweets(tweet_list, db):
    covid_output_dictionary ={}
    economy_output_dictionary ={}
    for message in tweet_list:
       analysis_overall_tweets_based_on_trends(message, covid_output_dictionary, economy_output_dictionary, db)

    return [covid_output_dictionary,economy_output_dictionary]

