from pymongo import MongoClient
from pymongo.errors import CollectionInvalid
import urllib

def mongodb_connection():
    print("Line 6 was here")
    username = urllib.parse.quote_plus('sauravverma')
    password = urllib.parse.quote_plus('Mongodb@123')
    print("Line 8 was there")
    conn = MongoClient("mongodb+srv://"+username+":"+password+"@cluster0.2tuhc.mongodb.net/tweet_db")
    print("Hello World")
    print(conn)
    print("dB name")
    db_name = "tweet_db"
    db = conn[db_name]
    print(db)
    coll = db["tweet_data"]
    x = (list(coll.find()))
    print(x)
    # if "tweet_data" not in db.list_collection_names():
    #     return create_database(db,"tweet_data")
    # # print(db)
    # return db["tweet_data"]


    # return conn

