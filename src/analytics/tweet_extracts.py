import logging

from src.common.variable_files import DATABASE_TWEET_NEW_DB, COLL_OF_TOTAL_TWEET_PER_COUNTRY, \
    COLL_OF_TWEET_PER_COUNTRY_ON_DAILY_BASIS, COLL_TOP_100_WORDS, COLL_OF_TOP_10_PREVENTIVE_WORDS, \
    COLL_OF_DONATION_PER_COUNTRY

LOGGER = logging.getLogger(__name__)

from datetime import datetime
from src.mongodb.mongo_data_connector import mongodb_connection

conn = mongodb_connection()
db = conn[DATABASE_TWEET_NEW_DB]
coll_overall_tweet = db[COLL_OF_TOTAL_TWEET_PER_COUNTRY]
coll_total_tweet_on_daily_basis = db[COLL_OF_TWEET_PER_COUNTRY_ON_DAILY_BASIS]
coll_top_100_words = db[COLL_TOP_100_WORDS]
coll_top_10_preventions_country_code = db[COLL_OF_TOP_10_PREVENTIVE_WORDS]
coll_total_donations = db[COLL_OF_DONATION_PER_COUNTRY]


def overall_tweet_per_country_in_last_n_month(country_code, from_date, to_date):
    """
      find the all the tweet on country basis in last n months
      :param:
      month = store the number of month
      data =  store the list of the returned result
      :return
      return the list of dictionary the data fetch from mongodb collection
   """
    try:
        start_month = datetime.strptime(from_date, '%Y-%m').month
        start_year = datetime.strptime(from_date, "%Y-%m").year
        end_month = datetime.strptime(to_date, "%Y-%m").month
        end_year = datetime.strptime(to_date, "%Y-%m").year
        # print(start_month,start_year,end_month,end_year)
        count = 0
        for year in range(start_year, end_year + 1):
            if start_year == end_year:
                for month in range(start_month, end_month + 1):
                    data = list(coll_overall_tweet.aggregate([{'$match': {
                        'country_code': {"$regex": country_code, "$options": 'i'}, "year": year, "month": month}}, {
                                                                  '$project': {"_id": 1, "count": 1, "country": 1,
                                                                               "month": 1, 'year': 1}}, {
                                                                  "$group": {"_id": {"country_code": country_code},
                                                                             "total_tweet": {"$sum": "$count"}}}]))
                    if len(data) > 0 and data[0].get('total_tweet'):
                        count += data[0]['total_tweet']
            elif year == start_year and year != end_year:
                for month in range(start_month, 13):
                    data = list(coll_overall_tweet.aggregate([{'$match': {
                        'country_code': {"$regex": country_code, "$options": 'i'}, "year": year, "month": month}}, {
                                                                  '$project': {"_id": 1, "count": 1, "country": 1,
                                                                               "month": 1, 'year': 1}}, {
                                                                  "$group": {"_id": {"country_code": country_code},
                                                                             "total_tweet": {"$sum": "$count"}}}]))
                    if len(data) > 0 and data[0].get('total_tweet'):
                        count += data[0]['total_tweet']
            elif year != end_year:
                for month in range(1, 13):
                    data = list(coll_overall_tweet.aggregate([{'$match': {
                        'country_code': {"$regex": country_code, "$options": 'i'}, "year": year, "month": month}}, {
                        '$project': {"_id": 1, "count": 1, "country": 1,
                                     "month": 1, 'year': 1}}, {
                        "$group": {"_id": {"country_code": country_code},
                                   "total_tweet": {"$sum": "$count"}}}]))
                    if len(data) > 0 and data[0].get('total_tweet'):
                        count += data[0]['total_tweet']
            else:
                for month in range(1, end_month):
                    data = list(coll_overall_tweet.aggregate([{'$match': {
                        'country_code': {"$regex": country_code, "$options": 'i'}, "year": year, "month": month}}, {
                        '$project': {"_id": 1, "count": 1, "country": 1,
                                     "month": 1, 'year': 1}}, {
                        "$group": {"_id": {"country_code": country_code},
                                   "total_tweet": {"$sum": "$count"}}}]))
                    if len(data) > 0 and data[0].get('total_tweet'):
                        count += data[0]['total_tweet']

        return count
    except Exception as e:
        LOGGER.error(f"ERROR:{e}")


def total_tweet_per_country_on_daily_basis(country_code, date):
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
        data = list(coll_total_tweet_on_daily_basis.aggregate(
            [{'$match': {"country_code": {"$regex": country_code, "$options": "i"}, "date": date}}]))
        return data
    except Exception as e:
        LOGGER.error(f"ERROR:{e}")


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
        word_count_list = list(coll_top_100_words.aggregate([
            {'$project': {"count": 1, "word": 1}},
            {"$group": {"_id": "$word", "count": {"$sum": "$count"}}},
            {"$sort": {"count": -1}}, {"$limit": 100}]))
        word_dict = {}
        for word in word_count_list:
            word_dict[word['_id']] = word['count']
        return word_dict
    except Exception as e:
        LOGGER.error(f"ERROR:{e}")


def top_100_word_occuring_with_country(country_code):
    result = []
    try:
        for row in coll_top_100_words.aggregate(
                [{'$match': {'country_code': country_code}}, {'$project': {'word': 1, 'count': 1, '_id': 0}},
                 {'$sort': {'count': -1}}, {'$limit': 100}]):
            word_count = (row['word'] + ' :' + str(row['count']))
            result.append(word_count)
        return result
    except Exception as e:
        LOGGER.error(f"ERROR:{e}")


#
# # ====================================================================================================================================================
# # ========================================================================================================================================================
# # query 5
#
def top_10_prevention(country_code):
    try:
        answer = []
        for row in coll_top_10_preventions_country_code.aggregate([
            {'$match': {'country_code': country_code}},
            {'$sort': {'count': -1}},
            {'$project': {'_id': 0, 'country_code': 1, 'word': 1, 'count': 1}},
            {'$limit': 10}
        ]):
            word_count = row['word'] + ' : ' + str(row['count'])
            answer.append(word_count)

        if len(answer):
            return answer
        else:
            return "NO RECORD FOUND"

    except Exception as e:
        LOGGER.error(f"ERROR:{e}")
        return "404"


#########################################################
def top_10_prevention_world_wide():
    answer = []
    try:
        for row in coll_top_10_preventions_country_code.aggregate([
            {'$group': {'_id': '$word', 'count': {'$sum': '$count'}}},
            {'$sort': {'count': -1}},
            {'$limit': 10}
        ]):
            word_count = row['_id'] + ' : ' + str(row['count'])
            answer.append(word_count)
        return answer
    except Exception as e:
        LOGGER.error(f"ERROR:{e}")
        return "error occured"


############################################################

def total_no_of_donations(country_code):
    try:
        answer = list(coll_total_donations.aggregate([{'$match': {'country_code': country_code}}, {
            '$project': {'country_code': 1, 'count': 1, '_id': 0, 'country': 1}}]))
        if answer:
            return answer
        else:
            return "NO RESULT FOUND"
    except Exception as e:
        LOGGER.error(f"ERROR:{e}")
        return "error occured"
