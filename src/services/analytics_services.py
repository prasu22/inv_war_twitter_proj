import logging
from src.common.variable_files import DATABASE_TWEET_NEW_DB, COLL_OF_TOTAL_TWEET_PER_COUNTRY, \
    COLL_OF_TWEET_PER_COUNTRY_ON_DAILY_BASIS, COLL_TOP_100_WORDS, COLL_OF_TOP_10_PREVENTIVE_WORDS, \
    COLL_OF_DONATION_PER_COUNTRY, COLL_OF_IMPACT_ANALYSIS_ON_COVID_KEYS, COLL_OF_IMPACT_ANALYSIS_ON_ECONOMY_KEYS, \
    COLL_OF_RANKING_COUNTRY_BASED_ON_TWEET_WITH_COVID_KEYS, COLL_OF_RANKING_COUNTRY_BASED_ON_TWEET_WITH_ECONOMY_KEYS, \
    COUNT_KEY, COUNTRY_CODE_KEY
import pandas as pd

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

coll_impact_covid_keys = db[COLL_OF_IMPACT_ANALYSIS_ON_COVID_KEYS]
coll_impact_economy_keys = db[COLL_OF_IMPACT_ANALYSIS_ON_ECONOMY_KEYS]
coll_ranking_impact_covid_keys = db[COLL_OF_RANKING_COUNTRY_BASED_ON_TWEET_WITH_COVID_KEYS]
coll_ranking_impact_economy_keys=db[COLL_OF_RANKING_COUNTRY_BASED_ON_TWEET_WITH_ECONOMY_KEYS]


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
        start_month = datetime.strptime(from_date,'%Y-%m').month
        start_year = datetime.strptime(from_date,"%Y-%m").year
        end_month = datetime.strptime(to_date,"%Y-%m").month
        end_year = datetime.strptime(to_date,"%Y-%m").year
        # print(start_month,start_year,end_month,end_year)
        count = 0
        for year in range(start_year,end_year+1):
            if start_year == end_year:
                for month in range(start_month,end_month+1):
                    data = list(coll_overall_tweet.aggregate([{'$match':{'country_code':{"$regex":country_code,"$options":'i'},"year":year,"month":month}},{'$project':{"_id":1,"count":1,"country":1,"month":1,'year':1}},{"$group":{"_id":{"country_code":country_code},"total_tweet":{"$sum":"$count"}}}]))
                    if len(data)>0 and data[0].get('total_tweet'):
                        count += data[0]['total_tweet']
            elif year == start_year and year != end_year:
                for month in range(start_month,13):
                    data = list(coll_overall_tweet.aggregate([{'$match':{'country_code':{"$regex":country_code,"$options":'i'},"year":year,"month":month}},{'$project':{"_id":1,"count":1,"country":1,"month":1,'year':1}},{"$group":{"_id":{"country_code":country_code},"total_tweet":{"$sum":"$count"}}}]))
                    if len(data) > 0 and data[0].get('total_tweet'):
                        count += data[0]['total_tweet']
            elif year != end_year:
                for month in range(1,13):
                    data = list(coll_overall_tweet.aggregate([{'$match': {
                        'country_code': {"$regex": country_code, "$options": 'i'}, "year": year, "month": month}}, {
                                                                  '$project': {"_id": 1, "count": 1, "country": 1,
                                                                               "month": 1, 'year': 1}}, {
                                                                  "$group": {"_id": {"country_code": country_code},
                                                                             "total_tweet": {"$sum": "$count"}}}]))
                    if len(data) > 0 and data[0].get('total_tweet'):
                        count += data[0]['total_tweet']
            else:
                for month in range(1,end_month+1):
                    data = list(coll_overall_tweet.aggregate([{'$match': {
                        'country_code': {"$regex": country_code, "$options": 'i'}, "year": year, "month": month}}, {
                                                                  '$project': {"_id": 1, "count": 1, "country": 1,
                                                                               "month": 1, 'year': 1}}, {
                                                                  "$group": {"_id": {"country_code": country_code},
                                                                             "total_tweet": {"$sum": "$count"}}}]))
                    if len(data) > 0 and data[0].get('total_tweet'):
                        count += data[0]['total_tweet']

        if count > 0:
            return {"total_tweet_last_n_month": count}
        else:
            return {"total_tweet_last_n_month":"No Record Found"}
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
        data = list(coll_total_tweet_on_daily_basis.aggregate([{'$match':{"country_code":{"$regex":country_code,"$options":"i"},"created_at":date}},{"$project":{"_id":0,"count":1,"country":1,"country_code":1}}]))
        if data:
            return {"tweet_per_country_on_daily_basis": data[0]["count"]}
        else:
            return {"tweet_per_country_on_daily_basis":"No Record Found"}
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
            word_dict[word['_id']]=word['count']

        if word_dict:
            return word_dict
        else:
            return {"words": "record not found"}
    except Exception as e:
        LOGGER.error(f"ERROR:{e}")


def top_100_word_occuring_with_country(country_code):
    result = {}
    try:
        for row in coll_top_100_words.aggregate([{'$match':{'country_code': country_code}},{'$project':{'word':1,'count':1,'_id':0}},{'$sort':{'count':-1}},{'$limit':100}]):
            result[row['word']] = row['count']
        if result:
            return result
        else:
            return {"words": "record not found"}
    except Exception as e:
        LOGGER.error(f"ERROR:{e}")


def top_10_prevention(country_code):
    try:
        answer = {}
        for row in coll_top_10_preventions_country_code.aggregate([
            {'$match': {'country_code': country_code}},
            {'$sort': {'count': -1}},
            {'$project': {'_id': 0, 'country_code': 1, 'word': 1, 'count': 1}},
            {'$limit': 10}
        ]):

            answer[row['word']] = row['count']

        if answer:
            return answer
        else:
            return {"words":"No record found"}
    except Exception as e:
        LOGGER.error(f"ERROR:{e}")
        return "404"


#########################################################
def top_10_prevention_world_wide():
    answer ={}
    try:
        for row in coll_top_10_preventions_country_code.aggregate([
            {'$group': {'_id': '$word', 'count': {'$sum': '$count'}}},
            {'$sort': {'count': -1}},
            {'$limit': 10}
        ]):
            answer[row['_id']]= row['count']
        if answer:
            return answer
        else:
            return {"words":"No record found"}
    except Exception as e:
        LOGGER.error(f"ERROR:{e}")
        return "error occured"


############################################################

def total_no_of_donations(country_code):
    try:
        answer = list(coll_total_donations.aggregate([{'$match': {'country_code':country_code}},{'$project':{'country_code':1,'count':1,'_id':0,"donation_amount":1,"currency_name":1,"country":1}}]))
        if answer:
            return answer
        else:
            return [{"country_code":country_code,"donation_amount":0,"currency_name":"no currency"}]
    except Exception as e:
        LOGGER.error(f"ERROR:{e}")
        return "error occured"

#shubham query
def impact_analysis_on_covid_keys_month(country_code):
    try:

        start_date = datetime.now().strftime("%Y-%m-%d")
        date_format = "%Y-%m-%d"
        dtObj = datetime.strptime(start_date, date_format)
        past_date = dtObj - pd.DateOffset(months=2)
        end_date = past_date.strftime("%Y-%m-%d")
        answer = list(coll_ranking_impact_covid_keys.aggregate([
            {"$match": {"created_at": {"$gte": end_date, "$lte": start_date}}},
            {"$project": {'weekdata': {"$isoWeek": {"$dateFromString": {"dateString": "$created_at"}}},
                          'year': {"$year": {"$dateFromString": {"dateString": "$created_at"}}},
                          'month': {"$month": {"$dateFromString": {"dateString": "$created_at"}}}, 'country_code': 1,
                          "count": 1}},
            {"$group": {"_id": {"country_code": "$country_code", "week": "$weekdata", "year": "$year"},
                        'month': {"$first": "$month"}, "count": {"$sum": "$count"}}},
            {"$sort": {"_id.week": 1, "_id.country_code": 1}}
        ]))
        # print(answer)
        answer2 = []
        if answer:
            for data in answer:
                value = {}
                country_code = data['_id']['country_code']
                week = data['_id']['week']
                month = data['month']
                year = data['_id']['year']
                count = data['count']
                value = {COUNTRY_CODE_KEY: country_code, "year": year, "week": week, "month": month, COUNT_KEY: count}
                answer2.append(value)
            return answer2
        else:
            return [{"country_code": country_code, COUNT_KEY: "No Record Found", "year": "none", "month": "none",
                     "week": "none"}]
    except Exception as e:
        LOGGER.error(f"ERROR:{e}")
        return "error occurred"

def impact_analysis_on_economy_keys_month(country_code):
    try:
        start_date = datetime.now().strftime("%Y-%m-%d")
        date_format = "%Y-%m-%d"
        dtObj = datetime.strptime(start_date, date_format)
        past_date = dtObj - pd.DateOffset(months=2)
        end_date = past_date.strftime("%Y-%m-%d")
        # start_month = datetime.strptime(start_date, '%Y-%m-%d').month
        # start_year = datetime.strptime(start_date, "%Y-%m-%d").year
        answer2 = list(coll_ranking_impact_economy_keys.aggregate([
            {"$match":{"created_at":{"$gte":end_date,"$lte":start_date}}},
            {"$project":{'weekdata':{"$isoWeek":{"$dateFromString":{"dateString":"$created_at"}}},'year':{"$year":{"$dateFromString":{"dateString":"$created_at"}}},'month':{"$month":{"$dateFromString":{"dateString":"$created_at"}}},'country_code':1,"count":1}},
                                                                   {"$group":{"_id":{"country_code":"$country_code","week":"$weekdata","year":"$year"},'month':{"$first":"$month"},"count":{"$sum":"$count"}}},
            {"$sort":{"_id.week":1,"_id.country_code":1}}
            ]))
        answer = []
        if answer2:
            for data in answer2:
                value = {}
                country_code=data['_id']['country_code']
                week = data['_id']['week']
                month = data['month']
                year = data['_id']['year']
                count = data['count']
                value = {COUNTRY_CODE_KEY:country_code,"year":year,"week":week,"month":month,COUNT_KEY:count}
                answer.append(value)
            return answer
        else:
            return [{"country_code":country_code,COUNT_KEY:"No Record Found","year":"none","month":"none","week":"none"}]
    except Exception as e:
        LOGGER.error(f"ERROR:{e}")
        return "error occurred"


def impact_analysis_on_covid_keys(country_code):
    try:
        answer = list(coll_impact_covid_keys.aggregate([{'$match': {'country_code': country_code}}, {
            '$project': {'country_code': 1, 'count': 1, '_id': 0, 'country': 1}}]))
        if answer:
            print(answer)

            return {COUNTRY_CODE_KEY:answer[0]["country_code"],COUNT_KEY:answer[0]["count"]}
        else:
            return {"country_code":country_code,COUNT_KEY:"No Record Found"}
    except Exception as e:
        LOGGER.error(f"ERROR:{e}")
        return "error occured"


def impact_analysis_on_economy_keys(country_code):
    try:
        answer = list(coll_impact_economy_keys.aggregate([{'$match': {'country_code': country_code}}, {
            '$project': {'country_code': 1, 'count': 1, '_id': 0, 'country': 1}}]))
        if answer:
            return {COUNTRY_CODE_KEY:answer[0]["country_code"],COUNT_KEY:answer[0]["count"]}
        else:
            return {"country_code":country_code,COUNT_KEY:"No Record Found"}
    except Exception as e:
        LOGGER.error(f"ERROR:{e}")
        return "error occured"

## ============================================================================================================================



