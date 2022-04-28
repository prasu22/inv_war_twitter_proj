from datetime import datetime



def analysis_total_tweet_per_country(message,db):
    """
        store the data in collection after manupulation in mongodb collection overall_tweet_per_country_on_daily_basis
        :collection schema
         {
           _id: objectid
           country:string
           count: int
           date: string
        }
        :passing argument
        message : dictionary storing information of tweet
        :param
        new_dt = data_extract only date and time in string format
        created_at = convert string date object format
        country = store the country name
        daily_basis_tweet = store the dictionary
    """
    new_dt = str(message['created_at'])[:19]
    created_at = datetime.strptime(new_dt, '%Y-%m-%d %H:%M:%S').date()
    print(created_at)
    country = message['country']
    country_code = message['country_code']

    # print(country_data)
    # print("hello i am from daily basis",country,country_code)
    # print('something to insert in data',daily_basis_tweet.keys(),daily_basis_tweet.values())
    if db['a_overall_tweet_per_country_on_daily_basis'].count_documents({"country": country, "date": str(created_at)}) == 0:
        db['a_overall_tweet_per_country_on_daily_basis'].insert_one({'count': 1, 'country': country,'country_code':country_code, 'date': str(created_at)})
    else:
        db['a_overall_tweet_per_country_on_daily_basis'].update_one({"country":country,"date":str(created_at)},{'$inc':{'count':1}})
    print(" i am in analysis_total_tweet_per_country")

# preprocess_total_tweet_per_country({'tweet':'coronavi','country':'united kingdom','created_at':'2022-04-27 06:54:04','id':'123'})