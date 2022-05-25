import sys

from src.common.variable_files import COLL_OF_RAW_DATA, COLL_OF_TWEET_PER_COUNTRY_ON_DAILY_BASIS, DATABASE_TWEET_NEW_DB
from src.mongodb.mongo_data_connector import mongodb_connection


try:
    conn = mongodb_connection()
    db = conn[DATABASE_TWEET_NEW_DB]
    print('connection pass')
except Exception as e:
    print('error',e)
    sys.exit()


start_time = "2022-04-29"

count1 = list(db[COLL_OF_RAW_DATA].aggregate([
   #  filter by ids
   {"$project":{'_id':1,"is_covid_tweet":1,"country_code":1,"date": {"$dateToString": {"format": '%Y-%m-%d',"date": {"$dateFromString": {"dateString": '$created_at'}}}}}},
   {"$match":{'is_covid_tweet':{'$eq':True},"date": {'$eq': start_time},"country_code":"IN"}},
   {"$group":{"_id":"$country_code","count":{"$sum":1}}}]))
if count1:
   print("raw",count1)
else:
   print("raw",0)

count2 = list(db[COLL_OF_TWEET_PER_COUNTRY_ON_DAILY_BASIS].aggregate([{'$match':{"country_code":"IN","created_at":start_time}}]))
if count2:
   print("raw",count2[0]['count'])
else:
   print("raw",0)
