from datetime import datetime
from mongodb.mongo_data_connector import mongodb_connection

conn = mongodb_connection()
db = conn['tweet_new_db']
coll_overall_tweet = db['a_overall_tweet_per_country']
coll_total_tweet_on_daily_basis = db['a_overall_tweet_per_country_on_daily_basis']
coll_top_100_words = db['a_top_100_words']

def overall_tweet_per_country_in_last_n_month(country,date):
    """
       find the all the tweet on country basis in last n months
       :param:
       month = store the number of month
       data =  store the list of the returned result
       :return
       return the list of dictionary the data fetch from mongodb collection
    """
    try:
        month = datetime.strptime(date,'%Y-%m-%d').month
        data = list(coll_overall_tweet.aggregate([{'$match':{'country':{"$regex":country,"$options":'i'},"month":{"$gte":month}}},{'$project':{"_id":1,"count":1,"country":1,"month":1}},{"$group":{"_id":{"country":country},"total_tweet":{"$sum":"$count"}}}]))
        return data
    except Exception as e:
        print("error ",e)

def total_tweet_per_country_on_daily_basis(country,date):
    """
     find all the tweet per country on daily basis
     :passing argument
      country: particular country from which we need tweet
      date: date of which we need tweets
      coll: collection name from which we fetch data
     :params
     data =  store the list of the returned result
     :return
     return the list of dictionary the data fetch from mongodb collection
    """
    try:
        data = list(coll_total_tweet_on_daily_basis.aggregate([{'$match':{"country":{"$regex":country,"$options":"i"},"date":date}}]))
        return data
    except Exception as e:
        print("error ",e)

def top_100_words_tweeted_in_world():
    """
     find all the top 100 words occured frequently in tweets worldwide
     :passing argument
     coll: collection name from which we fetch data
     :params
     word_count_list = it is list of dictionary containing 100 most frequent word and there corresponding frequency
     word_dict =  It is dictionary storing word and there frequency
     :returns
      return the top 100 most frequent word with there frequency in dictionary format

    """
    try:
        word_count_list = list(coll_top_100_words.aggregate([{'$project':{"count":1,"word":1}},{"$group":{"_id":"$word","count":{"$sum":"$count"}}},{"$sort":{"count":-1}},{"$limit":100}]))
        word_dict = {}
        for word in word_count_list:
            word_dict[word['_id']]=word['count']
        return word_dict
    except Exception as e:
        print("error",e)