from pub_sub.data_extraction.extract_country_code import get_country_code
from pub_sub.data_extraction.extract_tweets_by_keywords import get_tweets_with_keyword

covid_keys = ['death', 'hospitalisation', 'medicine' ]
economy_keys = ['GDP', 'unemployment', 'employment', 'layoffs', 'market', 'stock', 'index']




def overall_tweets_based_on_trends(message,db):
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

    covid_trend_data = get_tweets_with_keyword(message, covid_keys)
    economy_trend_data = get_tweets_with_keyword(message, economy_keys)
    country_data = get_country_code(message)

    if covid_trend_data and country_data:
        country_code = country_data['country_code']
        country = country_data['country']
        if db['impact_analysis_on_covid_keys'].count_documents({"country":country,"trend":"covid"})==0:
            db['impact_analysis_on_covid_keys'].insert_one({"country":country,'country_code':country_code,'count':1,"trend":"covid"})
        else:
            db['impact_analysis_on_covid_keys'].update_one({"country":country,"trend":"covid"},{"$inc":{'count': 1}})



    if economy_trend_data and country_data:
        country_code = country_data['country_code']
        country = country_data['country']
        if db['impact_analysis_on_economy_keys'].count_documents({"country": country, "trend": "economy"}) == 0:
            db['impact_analysis_on_economy_keys'].insert_one({"country": country,'country_code':country_code, 'count': 1, "trend": "economy"})
        else:
            db['impact_analysis_on_economy_keys'].update_one({"country": country, "trend": "economy"},
                                             {"$inc": {'count': 1}})



# overall_tweets_based_on_trends({'tweet':'coronavirus and is my death gdp market stock','country':'united kingdom','created_at':'2022-04-27 06:54:04','id':'123'})
