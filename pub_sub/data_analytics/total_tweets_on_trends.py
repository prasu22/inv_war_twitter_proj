#for query 7th and 8th query
import time
from datetime import datetime





def analysis_overall_tweets_based_on_trends(message,db):
    """
           store the data in collection after based on  trend  in different collection like if trend is covid than data insert in impact_analysis_on_covid_keys collection or if trend is economy then data insert in impact_analysis_on_economy_keys
           :collection schema
         {
           _id: objectid
           country:string
           count: int
           trend: string
        }
           :passing argument
           message : dictionary storing information of tweet
           :param
           country = store the country name
        """

    covid_trend_data = message['covid_trending_keywords']
    economy_trend_data = message['economy_trending_keywords']
    country_name = message['country']
    country_code = message['country_code']
    new_dt = str(message['created_at'])[:19]
    created_at = datetime.strptime(new_dt, '%Y-%m-%d %H:%M:%S').date()
    print("query 8 ", created_at, covid_trend_data, economy_trend_data)
    if len(covid_trend_data) > 0 :

        # for 8th query
        if db['a_impact_analysis_on_covid_keys'].count_documents({"country":country_name,"trend":"covid"})==0:
            db['a_impact_analysis_on_covid_keys'].insert_one({"country":country_name,'country_code':country_code,'count':1,"trend":"covid"})
        else:
            db['a_impact_analysis_on_covid_keys'].update_one({"country":country_name,"trend":"covid"},{"$inc":{'count': 1}})

        print('i am analysis_overall_tweets_based_on_trends covid')

    if len(economy_trend_data) > 0 :
        #8th query
        if db['a_impact_analysis_on_economy_keys'].count_documents({"country": country_name, "trend": "economy"}) == 0:
            db['a_impact_analysis_on_economy_keys'].insert_one({"country":country_name,'country_code':country_code, 'count': 1, "trend": "economy"})
        else:
            db['a_impact_analysis_on_economy_keys'].update_one({"country":country_name, "trend": "economy"},
                                             {"$inc": {'count': 1}})

        print('i am analysis_overall_tweets_based_on_trends economy')


# overall_tweets_based_on_trends({'tweet':'coronavirus and is my death gdp market stock','country':'united kingdom','created_at':'2022-04-27 06:54:04','id':'123'})
