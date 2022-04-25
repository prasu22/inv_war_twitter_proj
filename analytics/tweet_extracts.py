import time
from datetime import datetime

def overall_tweet_per_country_in_last_n_month(country,date,coll):
    try:
        month = datetime.strptime(date,'%Y-%m-%d').month
        data = list(coll.aggregate([{'$match':{'country':{"$regex":country,"$options":'i'},"month":{"$gte":month}}},{'$project':{"_id":1,"count":1,"country":1,"month":1}},{"$group":{"_id":{"country":country},"total_tweet":{"$sum":"$count"}}}]))
        return data
    except Exception as e:
        print("error ",e)

def total_tweet_per_country_on_daily_basis(country,date,coll):
    try:
        data = list(coll.aggregate([{'$match':{"country":{"$regex":country,"$options":"i"},"date":date}}]))
        return data
    except Exception as e:
        print("error ",e)

def top_100_words_tweeted_in_world(coll):
    try:
        word_count_list = list(coll.aggregate([{'$project':{"count":1,"word":1}},{"$group":{"_id":"$word","count":{"$sum":"$count"}}},{"$sort":{"count":-1}},{"$limit":100}]))
        word_dict = {}
        for word in word_count_list:
            word_dict[word['_id']]=word['count']
        return word_dict
    except Exception as e:
        print("error",e)