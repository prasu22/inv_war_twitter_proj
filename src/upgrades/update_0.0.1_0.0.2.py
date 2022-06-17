from src.common.variable_files import COLL_TOP_100_WORDS
from src.mongodb.mongo_data_connector import mongodb_connection

DATABASE_NAME = "tweet_new_db"
con = mongodb_connection()
db_name = DATABASE_NAME
db = con[db_name]

# collection name
COLLECTION_NAME = "tweet_extract_data"
top_100_words = db[COLL_TOP_100_WORDS]
print(top_100_words)
def update_currency_value(collection_name):
    db[collection_name].update_many({'currency_name': 'NO_Currency'}, {'$set': {'currency_name': 'no currency'}})


def update_country_values(collection_name):
    db[collection_name].update_many({'country': 'Not Found'},
                                    {'$set': {'country': 'no country', 'country_code': 'no country code'}})


def update_collection_name(collection_name):
    db[collection_name].rename("save_raw_data")

import nltk as nltk
nltk.download('stopwords')
nltk.download('punkt')



# stopword = nltk.corpus.stopwords.words('english')
# list_of_words = ['https',"http","Bollywoodbubble Https Twpvkc","Rsyp","Yzobvxm","Https Rsyp"]
# print(stopword)
# top_100_words.drop()
print(len(list(top_100_words.find())))
# for word in top_100_words.delete_many({ "word": { "$regex": word, "$options": 'i' } }):
#     print(top_100_words.delete_many({ "word": { "$regex": word, "$options": 'i' } }))
# import enchant
# d = enchant.Dict("en_US")
# d.check()

# update_currency_value(COLLECTION_NAME)
# update_country_values(COLLECTION_NAME)

# update_collection_name(COLLECTION_NAME)
# import nltk as nltk
# nltk.download('stopwords')
# nltk.download('punkt')
# stopword = nltk.corpus.stopwords.words('english')
# print(stopword)