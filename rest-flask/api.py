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



if __name__ == '__main__':
    app.run(debug=True)
