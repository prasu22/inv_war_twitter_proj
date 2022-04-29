
def insert_preprocessed_data(message,db):
    try:
        tweet_raw_data = db['tweet_extract_data']
        tweet_raw_data.insert_one(message)
    except Exception as e:
        print("Some error occured : ", e)




