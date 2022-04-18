from collections import OrderedDict
from datetime import datetime
from flask import Flask, request
import json
from mongodb.mongo_data_connector import connect_with_collection_data
from data_cleaning import *
from bson import json_util

app = Flask(__name__)


@app.route('/')
def hello_world():
    """Return all the tweets"""
    try:
        coll = connect_with_collection_data()
        x = (list(coll.find()))
        return json.dumps(x, default=json_util.default)
    except Exception as e:
        return json.dumps(e)
# ======================================================================================================================
@app.route('/overall_tweet/<keyword>/<country>/<date>')
def overall_tweet_based_on_keyword(keyword,country,date):
    """overall number of tweets on coronavirus per country in the last n months
       :param
       coll = store the collection details
       count = used to store the count of data returned by the query
    """
    try:
        coll = connect_with_collection_data()
        count = 0
        for row in coll.aggregate( [{'$match': { '$text': { '$search': keyword },'country':{'$regex':country,'$options' : 'i'}, 'date': {'$gte': datetime.strptime(date,'%Y-%m-%d')}}},{'$project':{'tweet':1,'country':1,'date':1}}]):
            count+=1
        return {'total_number':count}
    except Exception as e:
        print('some error occured ',e)
        return "error"
# ======================================================================================================================

# ======================================================================================================================
@app.route('/number_of_tweet_per_country/<country>/<date>')
def overall_per_country(country,date):
    """overall number of tweets per country on a daily basis
       :param
       coll = store the collection details
       count = used to store the count of data returned by the query
    """
    try:
        coll = connect_with_collection_data()
        count = 0
        for row in coll.aggregate([{'$match':{'country':{'$regex':country,'$options' : 'i'},'$expr': {'$eq': [date, { '$dateToString': {'date': "$date", 'format': "%Y-%m-%d"}}]}}},{'$project':{'tweet':1,'country':1,'date':1}}]):
            count+=1
        return {"total_tweet":count}
    except Exception as e:
        print("some error occured",e)
        return "error"
# ======================================================================================================================

# ======================================================================================================================
@app.route('/top_100_word')
def top_100_word_occuring():
    """top 100 words occurring on tweets involving coronavirus first fetch data from mongodb and
    clean the data and retrun reponse in json format
    :param
        coll = store the collection details
        words = dictionary to store the data returned by the query
        sorted_d = sorted ordered dictionary to store dictionary data
        top_100_word = store top 100 most frequent word from the tweet
    """
    try:
        coll = connect_with_collection_data()
        words = {}
        for row in coll.aggregate([{'$match': { '$text': { '$search': "coronavirus"}}},{'$project':{'tweet':1}}]):
            for word in clean_tweet(row['tweet']).split(" "):
                if word not in words:
                    words[word] = 1
                else:
                    words[word] += 1
        sorted_d = OrderedDict(sorted(words.items(), key=lambda x:-x[1]))
        top_100_word = {k: sorted_d[k] for k in list(sorted_d.keys())[:100]}
        return json.dumps(top_100_word)
    except Exception as e:
        print("some error occured ",e)
        return "404"
# ======================================================================================================================



@app.route('/tweet/<string:country>')
def get_tweet(country):
    """Return all the tweets per country"""
    try:
        db = connect_with_collection_data()
        x = list(db.aggregate([{'$match': {'country': {'$regex': country, '$options': 'i'}}},
                               {'$project': {'tweet': 1, 'country': 1, 'date': 1}}]))
        return json.dumps(x, default=json_util.default)
    except Exception as e:
        return json.dumps(e)


@app.route('/tweet/date', methods=['GET'])
def search_date():
    args = request.args
    start_date = args.get('start_date')
    end_date = args.get('end_date')

    try:
        db = connect_with_collection_data()
        # x = list(db.find({}))
        if end_date is None:
            x = list(db.aggregate([{'$match': {'date': {'$gte': datetime.strptime(start_date, '%Y-%m-%d')}}},
                                   {'$project': {'tweet': 1, 'country': 1, 'date': 1}}]))
        elif start_date is None:
            x = list(db.aggregate([{'$match': {'date': {'lte': datetime.strptime(end_date, '%Y-%m-%d')}}},
                                   {'$project': {'tweet': 1, 'country': 1, 'date': 1}}]))
        else:
            x = list(db.aggregate([{'$match': {'$and': [
                {'date': {'$gte': datetime.strptime(start_date, '%Y-%m-%d')},
                 'date': {'$lte': datetime.strptime(end_date, '%Y-%m-%d')}}
            ]}},
                {'$project': {'tweet': 1, 'country': 1, 'date': 1}}]))
        return json.dumps(x, default=json_util.default)

    except Exception as e:
        return json.dumps(e)


# query 8
@app.route('/tweet/analysis/<string:country>', methods=['POST'])
def impact_analysis(country):
    request_payload = request.json
    trend = request_payload['trend']
    # keyword = 'covid'
    # trend = 'economy'

    covid_keys = ['death', 'hospitalisation', 'medicine', ]
    economy_keys = ['GDP', 'unemployment', 'employment', 'layoffs', 'market', 'stock', 'index']

    try:
        db = connect_with_collection_data()
        list_country = []
        if trend == 'covid':
            for e in covid_keys:
                x = list(db.aggregate([
                    {"$match": {'$and': [{'$text': {'$search': e}}, {'country': {'$regex': country}}]}},
                    {"$group": {"_id": {"country": "$country"}, "numTweets": {"$sum": 1}}}
                ]))
                for i in x:
                    ans = {'country': i['_id']['country'], 'count': i['numTweets']}
                    for j in list_country:
                        if j == ans:
                            j['country'] += i['_id']['country']
                            j['count'] += i['numTweets']
                            continue
                    list_country.append(ans)
        else:
            for e in economy_keys:
                x = list(db.aggregate([
                    {"$match": {'$and': [{'$text': {'$search': e}}, {'country': {'$regex': country}}]}},
                    {"$group": {"_id": {"country": "$country"}, "numTweets": {"$sum": 1}}}
                ]))
                for i in x:
                    ans = {'country': i['_id']['country'], 'count': i['numTweets']}
                    for j in list_country:
                        if j == ans:
                            j['country'] += i['_id']['country']
                            j['count'] += i['numTweets']
                            continue
                    list_country.append(ans)

        return json.dumps(list_country, default=json_util.default)
    except Exception as e:
        return json.dumps(e)

    
    

if __name__ == '__main__':
    app.run(debug=True)
