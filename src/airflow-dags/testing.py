import json
import operator
from datetime import datetime

from kafka import KafkaConsumer

from src.mongodb.mongo_data_connector import mongodb_connection

mongo_conn = mongodb_connection()
db_name = "tweet_db"
db = mongo_conn[db_name]
coll = db["test_airflow"]
coll_name = db['metadata table']


my_consumer = KafkaConsumer(
    'random_data',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)


def duplicate_country():
    li = []
    for message in my_consumer:
        message = message.value
        li.append(message)
        if len(li) >= 3:
            my_consumer.close()
            break

    print(li)


duplicate_country()


def get_list_id():
    batch_list = list(coll_name.find())
    list_ids = batch_list[1]['record_ids']

    return list_ids


def test_tweets_daily_basis_metadata():
    start_datetime = '27-05-2022 10:47:07'

    batch_list = list(coll_name.find())
    created_at = batch_list[1]['start_datetime']
    list_ids = batch_list[1]['record_ids']
    dict_tweet_daily_after = batch_list[1]['tweet_daily_after']
    dict_tweet_daily_before = batch_list[1]['tweet_daily_before']

    dict_count = {}
    # list_id = []
    i = 0
    for key, value in dict_tweet_daily_after.items():
        if key in dict_tweet_daily_before:
            dict_count[key] = dict_tweet_daily_after[key] - dict_tweet_daily_before[key]
        else:
            dict_count[key] = dict_tweet_daily_after[key]
        # list_id.append(list_ids[i])
        i += 1

    res = {val[0]: val[1] for val in sorted(dict_count.items(), key=lambda x: (-x[1], x[0]))}
    # print('1', res)
    return res


def test_raw_daily_basis():
    list_id = get_list_id()
    # start_time = "2022-05-27"

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

    return res


# def test_query1():
#     dict1 = test_tweets_daily_basis()
#     dict2 = test_raw_daily_basis()
#
#     if dict1 == dict2:
#         print("Yes")
#     else:
#         print("Noo")


# test_query1()


# def test_query1(batch_id):
#     output_from_metadata_table = get_metadata_output(batch_id)
#     output_from_raw_data = get_raw_data_output(batch_id)
#     assert(compare(raw_data, metadata), True)


# ###########################################################################

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


def test_top_100_metadata():
    start_datetime = '27-05-2022 10:47:07'

    batch_list = list(coll_name.find())
    created_at = batch_list[1]['start_datetime']
    list_ids = batch_list[1]['record_ids']

    top_words_after = batch_list[1]['top_words_after']
    top_words_before = batch_list[1]['top_words_before']

    dict_after = helper(top_words_after)
    dict_before = helper(top_words_before)

    ans_dict = {}
    for k, v in dict_after.items():
        if k not in dict_before:
            ans_dict[k] = v
        else:
            ans_dict[k] = dict_after[k] - dict_before[k]

    res = {val[0]: val[1] for val in sorted(ans_dict.items(), key=lambda x: (-x[1], x[0]))}

    return res


def test_top_100_words_from_raw_data():
    list_id = get_list_id()

    count1 = list(coll.aggregate([
        {'$match': {"_id": {"$in": list_id}}},
        {'$project': {"tweet_keywords": 1}},
        {'$unwind': "$tweet_keywords"},
        {"$group": {"_id": "$tweet_keywords", "count": {"$sum": 1}}},
        {'$project': {"_id": 1, "count": 1}},
        {"$sort": {"count": -1}}, {"$limit": 100}]))

    output_dict = {}
    for i in count1:
        x = i['_id']
        y = i['count']
        if x not in output_dict:
            output_dict[x] = y
        else:
            output_dict[x] += y
    #
    res = {val[0]: val[1] for val in sorted(output_dict.items(), key=lambda x: (-x[1], x[0]))}
    # print('count', count1)

    return res
    # print(res)


def test_query2():
    dict1 = test_top_100_metadata()
    dict2 = test_top_100_words_from_raw_data()
    assert dict1 == dict2
    if dict1 == dict2:
        print("Yes")
    else:
        print("Noo")


# test_query2()


##################################################################################
# query3

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


def test_top_100_country_basis_metadata():
    start_datetime = '27-05-2022 10:47:07'

    batch_list = list(coll_name.find())
    created_at = batch_list[1]['start_datetime']
    list_ids = batch_list[1]['record_ids']

    top_words_after = batch_list[1]['top_words_after']
    top_words_before = batch_list[1]['top_words_before']

    dict_after = helper1(top_words_after)
    dict_before = helper1(top_words_before)

    ans_dict = {}
    for k, v in dict_after.items():
        if k not in dict_before:
            ans_dict[k] = v
        else:
            ans_dict[k] = dict_after[k] - dict_before[k]

    res = {val[0]: val[1] for val in sorted(ans_dict.items(), key=lambda x: (-x[1], x[0]))}

    print("meta", res)
    return res


def test_top_100_words_from_raw_data_country_basis():
    list_id = get_list_id()

    count1 = list(coll.aggregate([
        {'$match': {"_id": {"$in": list_id}}},
        {'$project': {"tweet_keywords": 1, "country_code": 1}},
        {'$unwind': "$tweet_keywords"},
        {"$group": {"_id": {"tweet_keywords": "$tweet_keywords", "country_code": "$country_code"},
                    "count": {"$sum": 1}}},
        {'$project': {"_id": 1, "country_code": 1, "count": 1}},
        {"$sort": {"count": -1}}, {"$limit": 100}
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
    # print('count', count1)
    # res['AU about'] = 10
    print("raw", res)
    return res


def test_query3():
    dict1 = test_top_100_country_basis_metadata()
    dict2 = test_top_100_words_from_raw_data_country_basis()
    assert dict1 == dict2
    if dict1 == dict2:
        print("Yes")
    else:
        print("Noo")


# test_query3()


#######################################################################################

# query4

def test_donation_metadata():
    start_datetime = '27-05-2022 10:47:07'

    batch_list = list(coll_name.find())
    created_at = batch_list[0]['start_datetime']
    list_ids = batch_list[0]['record_ids']
    dict_donation_after = batch_list[0]['donation_after']
    dict_donation_before = batch_list[0]['donation_before']

    output_dict = {}

    for k, v in dict_donation_after.items():
        if k not in output_dict:
            output_dict[k] = dict_donation_after[k]
        else:
            output_dict[k] = dict_donation_after[k] - dict_donation_before[k]

    res = {val[0]: val[1] for val in sorted(output_dict.items(), key=lambda x: (-x[1], x[0]))}

    # print(res)
    return res


def test_donation_raw_data():
    list_id = get_list_id()

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
    # print(res)

    return res


def test_query4():
    dict1 = test_donation_metadata()
    dict2 = test_donation_raw_data()

    if dict1 == dict2:
        print("Yes")
    else:
        print("No")


# test_query4()

# test_top_100_words_from_raw_data()

# list_ids = test_tweets_daily_basis()[0]
#
# start_time = "2022-05-27"
# list_dict = []
# final_dict = {}
# for i in list_ids:
#     x = list(coll.find({'_id': i}))
#     k = str(x[0]['country_code']) + ' ' + str(x[0]['created_at'][:10])
#     # print(k)
#     if k not in final_dict:
#         final_dict[k] = 1
#     else:
#         final_dict[k] += 1
#
#     # list_dict.append('country_code': x['country_code'])
#     print('final_dict', final_dict)
# return final_dict


# x = input('Enter date and code: ')
#
# dict1 = test_tweets_daily_basis()[1]
# dict2 = test_raw_daily_basis()
#
# ans1 = dict1.get(x, 0)
# ans2 = dict2.get(x, 0)
#
# if ans1 == ans2:
#     print("Yipee")
# else:
#     print("Phir se karo")


# count1 = list(db[COLL_OF_RAW_DATA].aggregate([ #  filter by ids {"$project":{'_id':1,"is_covid_tweet":1,
# "country_code":1,"date": {"$dateToString": {"format": '%Y-%m-%d',"date": {"$dateFromString": {"dateString":
# '$created_at'}}}}}}, {"$match":{'is_covid_tweet':{'$eq':True},"date": {'$eq': start_time},"country_code":"IN"}},
# {"$group":{"_id":"$country_code","count":{"$sum":1}}}]))


# def test_total_tweets():
#     # date_time_list = ti.xcom_pull(task_ids='make_collection', key='datetime_record')
#     start_datetime = '27-05-2022 10:47:07'
#
#     batch_list = list(coll_name.find())
#     created_at = batch_list[0]['start_datetime']
#     list_ids = batch_list[0]['record_ids']
#     dict_tweets_after = batch_list[0]['total_tweets_after']
#     dict_tweets_before = batch_list[0]['total_tweets_before']
#
#     dict_count = {}
#     list_key = []
#     i = 0
#     for key, value in dict_tweets_after.items():
#         if key in dict_tweets_before:
#             dict_count[key] = dict_tweets_after[key] - dict_tweets_before[key]
#         else:
#             dict_count[key] = dict_tweets_after[key]
#         list_key.append(key)
#         i += 1
#
#     print('ans', dict_count)
#     # print('after', dict_tweets_after)
#     # print('before', dict_tweets_before)
#     # print('start_date', start_datetime)
#     # print('created', created_at)
#     # print('ids', list_ids)
#     # print('batch_list', batch_list)
#     return [list_ids, dict_count]
#
#
# def test_raw_tweets():
#     list_id = test_total_tweets()[0]
#
#     count1 = list(coll.aggregate([
#         #  filter by ids
#         {"$project": {'_id': 1, "is_covid_tweet": 1, "country_code": 1, "date": {
#             "$dateToString": {"format": '%Y-%m-%d', "date": {"$dateFromString": {"dateString": '$created_at'}}}}}},
#         {"$match": {"$and": [{"is_covid_tweet": {'$eq': True}}, {"_id": {"$in": list_id}}]}},
#         {"$group": {"_id": {"country_code": "$country_code", "$month": "$date"}, "count": {"$sum": 1}}}]))
#
#     print(count1)
#
#
# test_raw_tweets()
#
