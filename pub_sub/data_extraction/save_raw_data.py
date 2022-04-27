# save all the tweets

from mongodb.mongo_data_connector import mongodb_connection

connection = mongodb_connection()

db = connection['tweet_db']

tweet_raw_data = db['tweet_raw_datas']

def get_tweets(message):

    try:
        tweet_raw_data.insert_one({'_id':message['id'],'country':message['country'],'tweet':message['tweet'],'created_at':message['created_at']})

    except Exception as e:
        print("Some error occured : ", e)


# get_tweets({'id':'1233','country':'INDIA','tweet':'hello world','created_at':'2202-32-12'})


