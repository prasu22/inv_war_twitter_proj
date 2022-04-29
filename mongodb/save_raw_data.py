# save all the tweets



def get_tweets(message,db):
    try:
        tweet_raw_data = db['tweet_extract_data']
        # print(message)
        tweet_raw_data.insert_one(message)
    except Exception as e:
        print("Some error occured : ", e)


# get_tweets({'id':'1233','country':'INDIA','tweet':'hello world','created_at':'2202-32-12'})


