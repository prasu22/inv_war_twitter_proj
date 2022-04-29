
from mongodb.mongo_data_connector import connect_with_collection_data, mongodb_connection


conn = mongodb_connection()
db = conn['tweet_new_db']
coll_top_100_words = db['top_100_words_country_code']
coll_top_10_preventions_country_code = db['top_10_prevention_country_code']



def top_100_word_occuring_with_country(country_code):

    result = []
    try:
        for row in coll_top_100_words.aggregate([{'$match':{'country_code': country_code}},{'$project':{'word':1,'count':1,'_id':0}},{'$sort':{'count':-1}},{'$limit':100}]):
            word_count = (row['word'] + ' :' + str(row['count']))
            result.append(word_count)
        return result
    except Exception as e:
        print("some error occured ", e)


# ====================================================================================================================================================
def top_100_word_occuring_country_wise():
    try:
        answer_to_return = {}
        for row in coll_top_100_words.aggregate([
                                   {'$group': {'_id': {'country_code': '$country_code','word': '$word'}, 'country_code': {'$first': '$country_code'},'word': {'$first':'$word'},'count':{'$first':'$count'}}},
                                   {'$sort':{'count':-1}},
                                   {'$project': {'word': 1, 'country_code': 1, 'count': 1, '_id': 0}},
                                   {'$limit':100}]):


            word_count = (row['word'] + ' :' + str(row['count']))

            if row['country_code'] in answer_to_return:
                answer_to_return[row['country_code']].append(word_count)
            else:
                answer_to_return[row['country_code']] = [word_count]
        return answer_to_return


    except Exception as e:
        print("some error occured ", e)
        return "404"

# ========================================================================================================================================================
# query 5

def top_10_prevention(country_code):
    try:
        answer = []
        for row in coll_top_10_preventions_country_code.aggregate([
            {'$match':{'country_code':country_code}},
            {'$sort': {'count': -1}},
            {'$project':{'_id':0,'country_code':1,'word':1,'count':1}},
            {'$limit':10}
        ]):

            word_count = row['word'] + ' : ' + str(row['count'])
            answer.append(word_count)
            print(word_count)

        if len(answer):
            return answer
        else:
            return "NO RECORD FOUND"

    except Exception as e:
        print("error",e)
        return "404"


def top_10_prevention_world_wide():
    answer =[]
    try:
        for row in coll_top_10_preventions_country_code.aggregate([
            {'$group':{'_id':'$word','count':{'$sum':'$count'}}},
            {'$sort':{'count':-1}},
            {'$limit':10}
        ]):
            word_count = row['_id'] + ' : ' + str(row['count'])
            answer.append(word_count)
        return answer
    except Exception as e:
        print('error',e)
        return "error occured"

################################################################################################################################

#
# answer = top_100_word_occuring_country_wise(coll_top_100_words)
# for country in answer:
#     print(country)
#     words = answer[country]
#     for word in words:
#         print(word[0], '   ', word[1])


# answer = top_100_word_occuring_country_wise(coll_top_100_words)
# if len(answer)>0:
#     print('helo')
# else:
#     print('ad')

# top_100_word_occuring_with_country('IN')

# hello = top_10_preventions('IN',coll_top_10_preventions)
# print(hello)

# hello = top_10_preventions('IN')

# top_10_prevention_world_wide()

