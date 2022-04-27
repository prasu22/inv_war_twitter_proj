# get overall tweets per country last n months



from datetime import datetime

from pub_sub.data_extraction.extract_country_code import get_country_code
from pub_sub.data_extraction.extract_tweets_by_keywords import get_tweets_with_keyword

keyword = ['covid', 'virus', 'coronavirus']


# db needs to be taken care of

def overall_tweets_country_wise(message, db):

    new_dt = str(message['created_at'])[:19]
    created_at = datetime.strptime(new_dt, '%Y-%m-%d %H:%M:%S')
    print(created_at)
    # created_at = datetime.strptime(message['created_at'], '%Y-%m-%d %H:%M:%S')
    month = created_at.month
    print("date", created_at.month)
    result = get_tweets_with_keyword(message,keyword)
    country_data = get_country_code(message)

    # print(result, country_data)
    if result and country_data:

        country_code = country_data['country_code']
        country = country_data['country']
        values = {'country': country, 'month': month, 'count': 1}
        print("last n month", values.keys(), values.values())
        if values is not None:
            if db['a_overall_tweet_per_country'].count_documents(
                    {"country": values['country'], "month": values['month']}) == 0:
                # print("adsf",db)
                db['a_overall_tweet_per_country'].insert_one(
                    {'count': values['count'], 'country': values['country'], 'country_code': country_code,'month': values['month']})
                # print("happys",db['overall_tweet_per_country'].find())
            else:
                db['a_overall_tweet_per_country'].update_one(
                    {"country": values['country'], "month": values['month']}, {'$inc': {'count': 1}})



# overall_tweets_country_wise({'tweet':'coronavirus','country':'india','created_at':'2022-04-27 06:54:04','id':'123'})


