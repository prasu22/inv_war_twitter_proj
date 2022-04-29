from datetime import datetime

from comman_variables.variable_files import COLL_OF_RANKING_COUNTRY_BASED_ON_TWEET_WITH_ECONOMY_KEYS, \
    COLL_OF_RANKING_COUNTRY_BASED_ON_TWEET_WITH_COVID_KEYS

from comman_variables.variable_files import COUNTRY_NAME_KEY ,COUNTRY_CODE_KEY ,TWEET_KEY ,CREATED_AT_KEY,ID_KEY,DONATION_AMOUNT_KEY,CURRENCY_NAME_KEY,DONATION_KEYWORDS_KEY,PREVENTION_KEYWORDS_KEY ,WHO_KEYWORDS_KEY ,COVID_TRENDING_KEYWORD_KEY ,ECONOMY_TRENDING_KEYWORD_KEY,COVID_KEYWORD_KEY ,MONTH_KEY,WORD_KEY,TREND_KEY,ECONOMY_VALUE,COVID_VALUE,COUNT_KEY
def analysis_overall_tweets_based_on_trends_per_day(message, db):
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

    covid_trend_data = message[COVID_TRENDING_KEYWORD_KEY]
    economy_trend_data = message[ECONOMY_TRENDING_KEYWORD_KEY]
    country_name = message[COUNTRY_NAME_KEY]
    country_code = message[COUNTRY_CODE_KEY]
    new_dt = str(message[CREATED_AT_KEY])[:19]
    created_at = datetime.strptime(new_dt, '%Y-%m-%d %H:%M:%S').date()
    if len(covid_trend_data) > 0:
        if db[COLL_OF_RANKING_COUNTRY_BASED_ON_TWEET_WITH_COVID_KEYS].count_documents(
                {COUNTRY_NAME_KEY: country_name, "date": str(created_at)}) == 0:
            db[COLL_OF_RANKING_COUNTRY_BASED_ON_TWEET_WITH_COVID_KEYS].insert_one(
                {COUNTRY_NAME_KEY: country_name, COUNTRY_CODE_KEY: country_code, COUNT_KEY: 1, TREND_KEY: COVID_VALUE,
                 "date": str(created_at)})
        else:
            db[COLL_OF_RANKING_COUNTRY_BASED_ON_TWEET_WITH_COVID_KEYS].update_one(
                {COUNTRY_NAME_KEY: country_name, "date": str(created_at)},
                {"$inc": {COUNT_KEY: 1}})
    else:
        print("data is not found")


    if len(economy_trend_data) > 0:
        # 7th query
        if db[COLL_OF_RANKING_COUNTRY_BASED_ON_TWEET_WITH_ECONOMY_KEYS].count_documents(
                {COUNTRY_NAME_KEY: country_name, "date": str(created_at)}) == 0:
            db[COLL_OF_RANKING_COUNTRY_BASED_ON_TWEET_WITH_ECONOMY_KEYS].insert_one(
                {COUNTRY_NAME_KEY: country_name, COUNTRY_CODE_KEY: country_code, COUNT_KEY: 1, TREND_KEY: COVID_VALUE,
                 "date": str(created_at)})
        else:
            db[COLL_OF_RANKING_COUNTRY_BASED_ON_TWEET_WITH_ECONOMY_KEYS].update_one(
                {COUNTRY_NAME_KEY: country_name, "date": str(created_at)}, {"$inc": {COUNT_KEY: 1}})
    else:
        print("data is not found")

