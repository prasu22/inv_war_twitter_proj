import json
from flask import Flask
from src.services.analytics_services import overall_tweet_per_country_in_last_n_month, top_100_words_tweeted_in_world, \
    total_tweet_per_country_on_daily_basis, top_10_prevention, top_100_word_occuring_with_country, \
    top_10_prevention_world_wide, total_no_of_donations, impact_analysis_on_economy_keys_month, \
    impact_analysis_on_covid_keys_month, impact_analysis_on_economy_keys, impact_analysis_on_covid_keys


import logging
LOGGER = logging.getLogger(__name__)
app = Flask(__name__)


@app.route('/')
def info():
    return "hello"



@app.route('/total_tweet_count/<country_code>/<from_date>/<to_date>')
def overall_tweet_based_on_keyword(country_code,from_date,to_date):
    """Find the total number of tweet per country in last n months
       :passing parameter in query
         country= name of country it is case insensitive
         from_date =  date in yyyy-mm format, date from which you want to fetch data
         to_data: date in yyyy-mm format, till date you want data
       example:
            http://127.0.0.1:5000/total_tweet_count/IN/2022-04/2022-05
            output: { "total_tweet_last_n_month": 23}
       :param
        count = store the total tweet in that period in particular country
       :return
        total number of tweet in last n month  per country
    """
    try:
        count = overall_tweet_per_country_in_last_n_month(country_code,from_date,to_date)
        return json.dumps(count)
    except Exception as e:
        LOGGER.error(f"Error:{e}")
        return {"error":e}

# # ======================================================================================================================
#
# # ======================================================================================================================
@app.route('/number_of_tweet_per_country/<country_code>/<date>')
def overall_per_country(country_code,date):
    """overall number of tweets per country on a daily basis
        :passing parameter in query
         country_code: code of country
         date =  date in yyyy-mm-dd format, fetch data of particular date
        example:
            http://127.0.0.1:5000/number_of_tweet_per_country/India/2022-04-22
            output: { tweet_per_country_on_daily_basis: 10}
       :param
       data = store the list return by the function
       :return
        return the total tweet on particular date in particular country
    """
    try:
        data = total_tweet_per_country_on_daily_basis(country_code,date)
        return json.dumps(data)
    except Exception as e:
        LOGGER.error(f"Error:{e}")
        return {"error":e}
# # ======================================================================================================================
#
# # ======================================================================================================================
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
        LOGGER.error(f"Error:{e}")
        return {"error":e}

# ======================================================================================================================

@app.route('/top_100_words_country_code/<country_code>')
def top_100_words_country(country_code):
    country_code = country_code.upper()
    try:
        word_dict = top_100_word_occuring_with_country(country_code)
        return json.dumps(word_dict)
    except Exception as e:
        LOGGER.error(f"Error:{e}")
        return {'error',e}



@app.route('/top_10_preventions/<country_code>')
def top_10_preventions_with_country(country_code):
    country_code = country_code.upper()
    try:
        word_list = top_10_prevention(country_code)
        return json.dumps(word_list)
    except Exception as e:
        LOGGER.error(f"Error:{e}")
        return {'error',e}


@app.route('/top_10_preventions_all_countries')
def top_10_preventions_all_countries():
    try:
        word_list = top_10_prevention_world_wide()
        return json.dumps(word_list)
    except Exception as e:
        LOGGER.error(f"Error:{e}")
        return {'error',e}

#####################################################################################################

@app.route('/total_no_donations/<country_code>')
def total_donations_with_country(country_code):
    country_code = country_code.upper()
    try:
        total_count = total_no_of_donations(country_code)
        return json.dumps(total_count)
    except Exception as e:
        LOGGER.error(f"Error:{e}")
        return {'error,',e}


@app.route('/impact_analysis_on_economy_keys_by_months/<country_code>')
def trend_impact_analysis_on_economy_keys_month(country_code):
    country_code = country_code.upper()
    try:
        total_count = impact_analysis_on_economy_keys_month(country_code)
        return json.dumps(total_count)
    except Exception as e:
        LOGGER.error(f"Error:{e}")
        return {'error,', e}


@app.route('/impact_analysis_on_covid_keys_by_months/<country_code>')
def trend_impact_analysis_on_covid_keys_month(country_code):
    country_code = country_code.upper()
    try:
        total_count = impact_analysis_on_covid_keys_month(country_code)
        return json.dumps(total_count)
    except Exception as e:
        LOGGER.error(f"Error:{e}")
        return {'error,', e}

@app.route('/impact_analysis_on_covid_keys/<country_code>')
def trend_impact_analysis_on_covid_keys(country_code):
    country_code = country_code.upper()
    try:
        total_count = impact_analysis_on_covid_keys(country_code)
        return json.dumps(total_count)
    except Exception as e:
        LOGGER.error(f"Error:{e}")
        return {'error,', e}


@app.route('/impact_analysis_on_economy_keys/<country_code>')
def trend_impact_analysis_on_economy_keys(country_code):
    country_code = country_code.upper()
    try:
        total_count = impact_analysis_on_economy_keys(country_code)
        return json.dumps(total_count)
    except Exception as e:
        LOGGER.error(f"Error:{e}")
        return {'error,', e}

# if __name__ == "__main__":
#     app.run(debug=True,host='0.0.0.0', port=80)

