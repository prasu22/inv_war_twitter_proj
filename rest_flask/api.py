from collections import OrderedDict
from datetime import datetime
from flask import Flask, request
import json
from analytics.tweet_extracts import overall_tweet_per_country_in_last_n_month, total_tweet_per_country_on_daily_basis, \
    top_100_words_tweeted_in_world
from mongodb.mongo_data_connector import connect_with_collection_data
from pub_sub.data_analytics.data_cleaning import *
from bson import json_util
# from dotenv import load_dotenv
import os
from configparser import ConfigParser

app = Flask(__name__)

file = os.getenv("ADDRESS")
config = ConfigParser(converters={'list': lambda x: [i.strip() for i in x.split(',')]})
config.read(file)



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
@app.route('/overall_tweet/<country>/<date>')
def overall_tweet_based_on_keyword(country,date):
    # write down the required comment here
    """Find the total number of tweet per country in last n months
       :passing parameter in query
         country= name of country it is case insensitive
         date =  date in yyyy-mm-dd format, date from which you want to fetch data
       example:
            http://127.0.0.1:5000/overall_tweet/India/2022-04-22
            output: { overall_tweet_per_country: 23}
       :param
        row = store the list return function
       :return
        total number of tweet in last n month  per country
    """
   # not perfect
    try:
        row = overall_tweet_per_country_in_last_n_month(country,date)
        if len(row) > 0:
            return {"total_tweet_last_n_month":row[0]['total_tweet']}
        else:
            return {"total_tweet_last_n_month":0}
    except Exception as e:
        print('some error occured ',e)
        return {"error":e}

# ======================================================================================================================

# ======================================================================================================================
@app.route('/number_of_tweet_per_country/<country>/<date>')
def overall_per_country(country,date):
    """overall number of tweets per country on a daily basis
        :passing parameter in query
         country= name of country it is case insensitive
         date =  date in yyyy-mm-dd format, fetch data of particular date
        example:
            http://127.0.0.1:5000/overall_tweet/India/2022-04-22
            output: { tweet_per_country_on_daily_basis: 10}
       :param
       data = store the list return by the function
       :return
        return the total tweet on particular date in particular country
    """
    try:
        data = total_tweet_per_country_on_daily_basis(country,date)
        if len(data)>0:
            # print(data[0]['count'])
            return {"tweet_per_country_on_daily_basis":data[0]['count']}
        else:
            return {"tweet_per_country_on_daily_basis":0}
    except Exception as e:
        print("some error occured",e)
        return {"error":e}
# ======================================================================================================================

# ======================================================================================================================
@app.route('/top_100_words')
def top_100_word_occuring():
    """top 100 words occurring on tweets involving coronavirus all over world
    Example:
        http://127.0.0.1:5000/top_100_word
        output:{
                "covid": 90,
                "total": 84,
                "cases": 68,
                "coronavirus": 48,
                "deaths": 43,
                .....}
    :param
        word_dict = store the returned dictionary of words by the function
    :return
       return the top 100 word with there coresponding frequency in output as shown in example above
    """
    try:
        word_dict = top_100_words_tweeted_in_world()
        return json.dumps(word_dict)
    except Exception as e:
        print("some error occured ",e)
        return {"error":e}
# ======================================================================================================================

# query 4

@app.route('/top_100_word_all_countries')
def top_100_word_occuring_all_countries():

    try:
        coll = connect_with_collection_data()
        words = {}
        answer = []   # creating the empty list to store all the answers(dictionaries)
        for row in coll.aggregate([{'$match': {'tweet': {'$regex': 'corona.*|covid.*'}}},
                                   {'$group': {'_id': {'country':'$country'},'country': {'$first': '$country'},'tweet': {'$first': '$tweet'}}},
                                   {'$project':{'tweet':1,'country':1}}]):

            # extracting the country name for future purpose
            country_name = row['country']

            # clean the tweets and extracting only the useful words
            for word in clean_tweet(row['tweet']).split(" "):
                if word not in words:
                    words[word] = 1
                else:
                    words[word] += 1


            top_100_words = {}  # empty dictionary to store top words for every country one by one

            # sort the list of words in descending order

            sorted_d = OrderedDict(sorted(words.items(), key=lambda x:-x[1]))

            # get the top 100 words by frequency from it

            top_100_words = {k: sorted_d[k] for k in list(sorted_d.keys())[:100]}

            # adding country name along with the words for that country
            top_100_words['country'] = country_name

            # append the result to the list
            answer.append(top_100_words)

        if len(answer):
            return json.dumps(answer)
        else:
            # incase no results found this is going to print
            return "NO RECORD FOUND"

    except Exception as e:
        print("some error occured ",e)
        return "404"


@app.route('/top_100_word/<country>')
def top_100_word_occuring_with_country(country):

    ''' enter the country name and get the top 100 words used in the tweet by the users in that country'''

    try:
        coll = connect_with_collection_data()
        words = {}
        top_100_word = {}
        for row in coll.aggregate([{'$match': {'$and': [{'country': country}, {'tweet': {'$regex': 'corona.*|covid.*'}}]}},
                                   {'$project': {'tweet': 1, 'country': 1}}]):

            # clean the tweets and extracting only the useful words

            for word in clean_tweet(row['tweet']).split(" "):
                if word not in words:
                    words[word] = 1
                else:
                    words[word] += 1

            # sort the list of words in descending order
            sorted_d = OrderedDict(sorted(words.items(), key=lambda x: -x[1]))

            # get the top 100 words only from the dictionary
            top_100_word = {k: sorted_d[k] for k in list(sorted_d.keys())[:100]}

            # adding the name of country in the dictionary
            top_100_word['country'] = country

        if len(top_100_word):
            # returning the result to user
            return json.dumps(top_100_word)
        else:
            # no record match with the following country
            return "NO RECORD FOUND"

    except Exception as e:
        # error occured with doing the above task
        print("some error occured ", e)
        return "404"

############################################################################################

# query 5

@app.route('/top_10_preventions/<country>')
def top_10_preventions(country):

        try:
            list = ['wear mask','use sanitiser','stay home','social distancing','wash hands']
            answers = []
            collection = connect_with_collection_data()
            for word in list:
                count = 0
                ans = {}
                ans['word'] = word
                for row in collection.aggregate([
                    {'$match': {'$and': [{'country': {'$regex':country,'$options':'i'}},
                                         {'tweet': {'$regex': 'prevent.*|precaut.*', '$options': 'i'}},
                                         {'tweet': {'$regex': '.*who.*', '$options': 'i'}},
                                         {'$text':{'$search':word}}]}}
                    ]):
                    count += 1
                ans['count'] = count
                answers.append(ans)

            if len(answers):
                print(answers)
                answers = sorted(answers, key=lambda i: i['count'],reverse = True)
                new_dict = [value['word'] for value in answers ]

                # if highest value is zero then print no result found need to implement this

                return json.dumps(answers[:10])

                # return json.dumps(new_dict[:10])
            else:
                return "NO RECORD FOUND"

        except Exception as e:
            print("error",e)
            return json.dumps(e)


@app.route('/top_10_preventions')
def top_10_preventions_country_wise():

    try:
        list = ['wear mask','use sanitiser','stay home','social distancing','wash hands']
        answers = []
        final_answer = {}
        collection = connect_with_collection_data()
        for word in list:
            for row in collection.aggregate([
                {'$match': {'$and': [{'tweet': {'$regex': 'prevent.*|precaut.*', '$options': 'i'}},
                                     {'tweet': {'$regex': '.*who.*', '$options': 'i'}},
                                     {'tweet': {'$regex': word,'$options':'i'}}]}},
                                     {'$group': {'_id': {'country': '$country'}, 'total_appearance': {'$sum':1},'country': {'$first': '$country'}}},
                {'$project': {'_id':0}}

                ]):
                ans = row
                ans['word'] = word

                if row['country'] in final_answer:
                    final_answer[ans['country']].append({'word':ans['word'],'total_appearance':ans['total_appearance']})
                else:
                    final_answer[ans['country']] = [{'word':ans['word'],'total_appearance':ans['total_appearance']}]


            answers.append(final_answer)
        if len(answers):
            final_output = []
            for key in answers[0]:
                answers[0][key] = sorted(answers[0][key], key=lambda i: i['total_appearance'], reverse=True)
                dict =[]
                for aditya in answers[0][key]:
                    dict.append(aditya['word'])
                final_output.append({key: dict})
            print(final_output)

            return json.dumps(final_output)
        else:
            return "NO RECORD FOUND"

    except Exception as e:
        print("error",e)
        return json.dumps(e)

##################################################################################

# query 6

@app.route('/total_no_donations/<country>')
def total_no_donations_with_country_name(country):
    try:
        # matching with the country and look for the certain keywords
        collection = connect_with_collection_data()
        ans = {}
        for row in collection.aggregate([
            {'$match': {'$and': [{'country': {'$regex':country , '$options':'i'}},
                                 {'tweet': {'$regex': 'donat.*|contribut.*', '$options': 'i'}},
                                 {'tweet': {'$regex': 'covid.*|corona.*','$options':'i'}},
                                 {'tweet': {'$regex': '[$¢£¤¥֏؋৲৳৻૱௹฿៛₹](\d[0-9,.]+)'}}]}},
                                {'$count': "total donations "}
            ]):
            ans = row
            ans['country'] = country

        if ans:
            return json.dumps(ans)
        else:
            return "NO RECORD FOUND"
    except Exception as e:
        print(e)
        return 'something went wrong'

@app.route('/total_no_donations')
def total_no_donations_all_countries():
    try:
        # giving total_donations by all the countries
        collection = connect_with_collection_data()
        answers = []
        for row in collection.aggregate([
                    {'$match': {'$and':[{'tweet': {'$regex': 'contribut.*|donat.','$options': 'i'}},{'tweet': {'$regex': 'corona.*|covid.*','$options':'i'}},{'tweet': {'$regex': '[$¢£¤¥֏؋৲৳৻૱௹฿៛₹](\d[0-9,.]+)'}}]}},
                    {'$group': {'_id': {'country':'$country'},'total donations':{'$sum':1},'country': {'$first': '$country'}}},
                    {'$project': {'_id':0,'country':1,'total donations':1}},
                    {'$sort': {'total donations': -1}}]):
            ans = row
            answers.append(ans)

        if answers:
            return json.dumps(answers)
        else:
            return "NO RECORDS FOUND"
    except Exception as e:
        print(e)
        return "Something wrong happened"

##########################################################################



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

    covid_keys = list(map(str, config.getlist('covid', 'keywords')))
    economy_keys = list(map(str, config.getlist('economy', 'keywords')))

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
