# get overall tweets per country last n months

from datetime import datetime


keyword = ['covid', 'virus', 'coronavirus']


# db needs to be taken care of

def overall_tweets_country_wise(message, db):

    new_dt = str(message['created_at'])[:19]
    created_at = datetime.strptime(new_dt, '%Y-%m-%d %H:%M:%S')
    print(created_at)
    # created_at = datetime.strptime(message['created_at'], '%Y-%m-%d %H:%M:%S')
    month = created_at.month
    print("date", created_at.month)
    result = len(message['covid_keywords'])
    country = message['country']
    country_code = message['country_code']

    # print(result, country_data)
    if result > 0:
        if db['a_overall_tweet_per_country'].count_documents({"country": country, "month": month}) == 0:
            # print("adsf",db)
            db['a_overall_tweet_per_country'].insert_one({'count': 1, 'country':country, 'country_code': country_code,'month': month})
            # print("happys",db['overall_tweet_per_country'].find())
        else:
            db['a_overall_tweet_per_country'].update_one({"country": country, "month": month}, {'$inc': {'count': 1}})
        print("i am in overall_tweets_country_wise")
# overall_tweets_country_wise({'tweet':'coronavirus','country':'india','created_at':'2022-04-27 06:54:04','id':'123'})


