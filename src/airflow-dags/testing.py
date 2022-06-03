import json
import operator
from datetime import datetime

from src.mongodb.mongo_data_connector import mongodb_connection

mongo_conn = mongodb_connection()
db_name = "tweet_db"
db = mongo_conn[db_name]
coll = db["test_airflow"]
coll_name = db['metadata table']

print(coll.count_documents({}))

# list_ids = ["1532580160649986049","1532580159114575873","1532580156379889665"]

def test_tweets_daily_basis_metadata(li_ids):
    # start_datetime = '27-05-2022 10:47:07'

    batch_list = list(coll_name.find({'record_ids':li_ids}))
    dict_tweet_daily_after = batch_list[0]['tweet_daily_after']
    dict_tweet_daily_before = batch_list[0]['tweet_daily_before']

    dict_count = {}
    i = 0
    for key, value in dict_tweet_daily_after.items():
        if key in dict_tweet_daily_before:
            diff = dict_tweet_daily_after[key] - dict_tweet_daily_before[key]
            if diff > 0:
                dict_count[key] = dict_tweet_daily_after[key] - dict_tweet_daily_before[key]
        else:
            dict_count[key] = dict_tweet_daily_after[key]
        i += 1

    res = {val[0]: val[1] for val in sorted(dict_count.items(), key=lambda x: (-x[1], x[0]))}
    print('meta', res)
    return res


def test_raw_daily_basis(li_ids):
    list_id = li_ids
    count1 = list(coll.aggregate([
        #  filter by ids
        {"$project": {'_id': 1, "is_covid_tweet": 1, "country_code": 1, "date": {
            "$dateToString": {"format": '%Y-%m-%d', "date": {"$dateFromString": {"dateString": '$created_at'}}}}}},
        {"$match": {"$and": [{"is_covid_tweet": {'$eq': True}}, {"_id": {"$in": list_id}}]}},
        {"$group": {"_id": {"country_code": "$country_code", "date": "$date"}, "count": {"$sum": 1}}}]))

    output_dict = {}

    for i in count1:
        x = i['_id']['country_code']
        y = i['_id']['date']
        z = i['count']
        key = str(x) + " " + str(y)
        if key not in output_dict:
            output_dict[key] = z
        else:
            output_dict[key] += z

    res = {val[0]: val[1] for val in sorted(output_dict.items(), key=lambda x: (-x[1], x[0]))}
    print('raw',res)
    return res


def test_query1(li_ids):
    dict1 = test_tweets_daily_basis_metadata(li_ids)
    dict2 = test_raw_daily_basis(li_ids)

    if dict1 == dict2:
        print("Yes")
    else:
        print("Noo")


# test_query1(list_ids)

# query2
#
def helper(dict):
    ans_dict = {}

    for key, val in dict.items():
        x = 0
        for i in val:
            for k1, v1 in i.items():
                if k1 not in ans_dict:
                    ans_dict[k1] = v1
                else:
                    ans_dict[k1] += v1

    return ans_dict


def test_top_100_metadata(li_ids):

    batch_list = list(coll_name.find({'record_ids':li_ids}))
    top_words_after = batch_list[0]['top_words_after']
    top_words_before = batch_list[0]['top_words_before']

    dict_after = helper(top_words_after)
    dict_before = helper(top_words_before)

    ans_dict = {}
    for k, v in dict_after.items():
        if k not in dict_before:
            ans_dict[k] = v
        else:
            diff = dict_after[k] - dict_before[k]
            if diff > 0:
                ans_dict[k] = dict_after[k] - dict_before[k]

    res = {val[0]: val[1] for val in sorted(ans_dict.items(), key=lambda x: (-x[1], x[0]))}

    print("meta", res)
    return res

def test_top_100_words_from_raw_data(li_ids):
    list_id = li_ids

    count1 = list(coll.aggregate([
        {"$match": {"$and": [{"is_covid_tweet": {'$eq': True}}, {"_id": {"$in": list_id}}]}},
        {'$project': {"tweet_keywords": 1}},
        {'$unwind': "$tweet_keywords"},
        {"$group": {"_id": "$tweet_keywords", "count": {"$sum": 1}}},
        {'$project': {"_id": 1, "count": 1}}]))
        # {"$sort": {"count": -1}}, {"$limit": 100}]))

    output_dict = {}
    for i in count1:
        x = i['_id']
        y = i['count']
        if x not in output_dict:
            output_dict[x] = y
        else:
            output_dict[x] += y

    res = {val[0]: val[1] for val in sorted(output_dict.items(), key=lambda x: (-x[1], x[0]))}
    print('raw',res)
    return res


def test_query2(li_ids):
    dict1 = test_top_100_metadata(li_ids)
    dict2 = test_top_100_words_from_raw_data(li_ids)
    assert dict1 == dict2
    if dict1 == dict2:
        print("Yes")
    else:
        print("Noo")


# test_query2(list_ids)


##################################################################################





### query 3


def helper1(dict):
    ans_dict = {}

    for key, val in dict.items():
        x = 0
        for i in val:
            for k1, v1 in i.items():
                new_key = str(key) + " " + str(k1)
                if k1 not in ans_dict:
                    ans_dict[new_key] = v1
                else:
                    ans_dict[new_key] += v1

    return ans_dict


def test_top_100_country_basis_metadata(li_ids):

    batch_list = list(coll_name.find({'record_ids':li_ids}))
    top_words_after = batch_list[0]['top_words_after']
    top_words_before = batch_list[0]['top_words_before']

    dict_after = helper1(top_words_after)
    dict_before = helper1(top_words_before)

    ans_dict = {}
    for k, v in dict_after.items():
        if k not in dict_before:
            ans_dict[k] = v
        else:
            diff = dict_after[k] - dict_before[k]
            if diff > 0:
                ans_dict[k] = dict_after[k] - dict_before[k]

    res = {val[0]: val[1] for val in sorted(ans_dict.items(), key=lambda x: (-x[1], x[0]))}

    print("meta", res)
    return res


def test_top_100_words_from_raw_data_country_basis(li_ids):
    list_id = li_ids

    count1 = list(coll.aggregate([
        {"$match": {"$and": [{"is_covid_tweet": {'$eq': True}}, {"_id": {"$in": list_id}}]}},
        {'$project': {"tweet_keywords": 1, "country_code": 1}},
        {'$unwind': "$tweet_keywords"},
        {"$group": {"_id": {"tweet_keywords": "$tweet_keywords", "country_code": "$country_code"},
                    "count": {"$sum": 1}}},
        {'$project': {"_id": 1, "country_code": 1, "count": 1}}
        # {"$sort": {"count": -1}}, {"$limit": 100}
    ]))

    output_dict = {}
    for i in count1:
        x = str(i['_id']['country_code']) + " " + str(i['_id']['tweet_keywords'])
        y = i['count']
        if x not in output_dict:
            output_dict[x] = y
        else:
            output_dict[x] += y
    # #
    res = {val[0]: val[1] for val in sorted(output_dict.items(), key=lambda x: (-x[1], x[0]))}

    print("raw", res)
    return res


def test_query3(li_ids):
    dict1 = test_top_100_country_basis_metadata(li_ids)
    dict2 = test_top_100_words_from_raw_data_country_basis(li_ids)
    assert dict1 == dict2
    if dict1 == dict2:
        print("Yes")
    else:
        print("Noo")


# test_query3(list_ids)

#
# #######################################################################################
#
# # query4

def test_donation_metadata(li_ids):

    batch_list = list(coll_name.find({'record_ids':li_ids}))
    dict_donation_after = batch_list[0]['donation_after']
    dict_donation_before = batch_list[0]['donation_before']

    output_dict = {}

    for k, v in dict_donation_after.items():
        if k not in dict_donation_before:
            output_dict[k] = dict_donation_after[k]
        else:
            diff = dict_donation_after[k] - dict_donation_before[k]
            if diff > 0:
                output_dict[k] = dict_donation_after[k] - dict_donation_before[k]

    res = {val[0]: val[1] for val in sorted(output_dict.items(), key=lambda x: (-x[1], x[0]))}
    print('meta',res)
    return res


def test_donation_raw_data(li_ids):
    list_id = li_ids

    count1 = list(coll.aggregate([
        {'$match': {"$and": [{"donation_amount": {'$ne': 0}}, {"_id": {"$in": list_id}}]}},
        {'$project': {'country_code': 1, 'count': 1}},
        {"$group": {"_id": "$country_code", "count": {"$sum": 1}}}
    ]))

    output_dict = {}

    for i in count1:
        x = i['_id']
        y = i['count']
        if x not in output_dict:
            output_dict[x] = y
        else:
            output_dict[x] += y

    res = {val[0]: val[1] for val in sorted(output_dict.items(), key=lambda x: (-x[1], x[0]))}
    print('raw',res)
    return res


def test_query4(li_ids):
    dict1 = test_donation_metadata(li_ids)
    dict2 = test_donation_raw_data(li_ids)

    if dict1 == dict2:
        print("Yes")
    else:
        print("No")


# test_query4(list_ids)
