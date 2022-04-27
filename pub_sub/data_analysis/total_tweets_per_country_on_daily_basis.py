from datetime import datetime

from pub_sub.data_extraction.extract_country_code import get_country_code

def preprocess_total_tweet_per_country(message,db):
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
        new_dt = extract only date and time in string format
        created_at = convert string date object format
        country = store the country name
        daily_basis_tweet = store the dictionary
    """
    new_dt = str(message['created_at'])[:19]
    created_at = datetime.strptime(new_dt, '%Y-%m-%d %H:%M:%S')
    print(created_at)
    country_data = get_country_code(message)

    # print(country_data)
    if country_data:
        country_code = country_data['country_code']
        country = country_data['country']
        daily_basis_tweet = {'country': country,'count': 1,'date':created_at}
        # print("daily basis",data.keys(),data.values())
        if daily_basis_tweet is not None:
            # print('something to insert in data',daily_basis_tweet.keys(),daily_basis_tweet.values())
            if db['overall_tweet_per_country_on_daily_basis'].count_documents({"country": daily_basis_tweet['country'], "date": str(daily_basis_tweet['date'])}) == 0:
                db['overall_tweet_per_country_on_daily_basis'].insert_one({'count': daily_basis_tweet['count'], 'country': daily_basis_tweet['country'],'country_code':country_code, 'date': str(daily_basis_tweet['date'])})
            else:
                db['overall_tweet_per_country_on_daily_basis'].update_one({"country":daily_basis_tweet['country'],"date":str(daily_basis_tweet['date'])},{'$inc':{'count':1}})


# preprocess_total_tweet_per_country({'tweet':'coronavi','country':'united kingdom','created_at':'2022-04-27 06:54:04','id':'123'})