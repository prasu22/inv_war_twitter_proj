from datetime import datetime
import re
from mongodb.mongo_data_connector import mongodb_connection
from rest_flask.data_cleaning import clean_tweet

# connection with mongodb database
conn = mongodb_connection()
db = conn['tweet_db']

# static keywords
keyword = ['covid','virus','coronavirus']
list_of_country = ['Afghanistan', 'Aland Islands', 'Albania', 'Algeria', 'American Samoa', 'Andorra', 'Angola', 'Anguilla', 'Antarctica', 'Antigua and Barbuda', 'Argentina', 'Armenia', 'Aruba', 'Australia', 'Austria', 'Azerbaijan', 'Bahamas', 'Bahrain', 'Bangladesh', 'Barbados', 'Belarus', 'Belgium', 'Belize', 'Benin', 'Bermuda', 'Bhutan', 'Bolivia, Plurinational State of', 'Bonaire, Sint Eustatius and Saba', 'Bosnia and Herzegovina', 'Botswana', 'Bouvet Island', 'Brazil', 'British Indian Ocean Territory', 'Brunei Darussalam', 'Bulgaria', 'Burkina Faso', 'Burundi', 'Cambodia', 'Cameroon', 'Canada', 'Cape Verde', 'Cayman Islands', 'Central African Republic', 'Chad', 'Chile', 'China', 'Christmas Island', 'Cocos (Keeling) Islands', 'Colombia', 'Comoros', 'Congo', 'Congo, The Democratic Republic of the', 'Cook Islands', 'Costa Rica', "Côte d'Ivoire", 'Croatia', 'Cuba', 'Curaçao', 'Cyprus', 'Czech Republic', 'Denmark', 'Djibouti', 'Dominica', 'Dominican Republic', 'Ecuador', 'Egypt', 'El Salvador', 'Equatorial Guinea', 'Eritrea', 'Estonia', 'Ethiopia', 'Falkland Islands (Malvinas)', 'Faroe Islands', 'Fiji', 'Finland', 'France', 'French Guiana', 'French Polynesia', 'French Southern Territories', 'Gabon', 'Gambia', 'Georgia', 'Germany', 'Ghana', 'Gibraltar', 'Greece', 'Greenland', 'Grenada', 'Guadeloupe', 'Guam', 'Guatemala', 'Guernsey', 'Guinea', 'Guinea-Bissau', 'Guyana', 'Haiti', 'Heard Island and McDonald Islands', 'Holy See (Vatican City State)', 'Honduras', 'Hong Kong', 'Hungary', 'Iceland', 'India', 'Indonesia', 'Iran, Islamic Republic of', 'Iraq', 'Ireland', 'Isle of Man', 'Israel', 'Italy', 'Jamaica', 'Japan', 'Jersey', 'Jordan', 'Kazakhstan', 'Kenya', 'Kiribati', "Korea, Democratic People's Republic of", 'Korea, Republic of', 'Kuwait', 'Kyrgyzstan', "Lao People's Democratic Republic", 'Latvia', 'Lebanon', 'Lesotho', 'Liberia', 'Libya', 'Liechtenstein', 'Lithuania', 'Luxembourg', 'Macao', 'Macedonia, Republic of', 'Madagascar', 'Malawi', 'Malaysia', 'Maldives', 'Mali', 'Malta', 'Marshall Islands', 'Martinique', 'Mauritania', 'Mauritius', 'Mayotte', 'Mexico', 'Micronesia, Federated States of', 'Moldova, Republic of', 'Monaco', 'Mongolia', 'Montenegro', 'Montserrat', 'Morocco', 'Mozambique', 'Myanmar', 'Namibia', 'Nauru', 'Nepal', 'Netherlands', 'New Caledonia', 'New Zealand', 'Nicaragua', 'Niger', 'Nigeria', 'Niue', 'Norfolk Island', 'Northern Mariana Islands', 'Norway', 'Oman', 'Pakistan', 'Palau', 'Palestinian Territory, Occupied', 'Panama', 'Papua New Guinea', 'Paraguay', 'Peru', 'Philippines', 'Pitcairn', 'Poland', 'Portugal', 'Puerto Rico', 'Qatar', 'Réunion', 'Romania', 'Russian Federation', 'Rwanda', 'Saint Barthélemy', 'Saint Helena, Ascension and Tristan da Cunha', 'Saint Kitts and Nevis', 'Saint Lucia', 'Saint Martin (French part)', 'Saint Pierre and Miquelon', 'Saint Vincent and the Grenadines', 'Samoa', 'San Marino', 'Sao Tome and Principe', 'Saudi Arabia', 'Senegal', 'Serbia', 'Seychelles', 'Sierra Leone', 'Singapore', 'Sint Maarten (Dutch part)', 'Slovakia', 'Slovenia', 'Solomon Islands', 'Somalia', 'South Africa', 'South Georgia and the South Sandwich Islands', 'Spain', 'Sri Lanka', 'Sudan', 'Suriname', 'South Sudan', 'Svalbard and Jan Mayen', 'Swaziland', 'Sweden', 'Switzerland', 'Syrian Arab Republic', 'Taiwan, Province of China', 'Tajikistan', 'Tanzania, United Republic of', 'Thailand', 'Timor-Leste', 'Togo', 'Tokelau', 'Tonga', 'Trinidad and Tobago', 'Tunisia', 'Turkey', 'Turkmenistan', 'Turks and Caicos Islands', 'Tuvalu', 'Uganda', 'Ukraine', 'United Arab Emirates', 'United Kingdom', 'United States', 'United States Minor Outlying Islands', 'Uruguay', 'Uzbekistan', 'Vanuatu', 'Venezuela, Bolivarian Republic of', 'Viet Nam', 'Virgin Islands, British', 'Virgin Islands, U.S.', 'Wallis and Futuna', 'Yemen', 'Zambia', 'Zimbabwe']
prevention_list = ['mask','sanitiser','stay home','social distancing','wash hands']
covid_keys = ['death', 'hospitalisation', 'medicine' ]
economy_keys = ['GDP', 'unemployment', 'employment', 'layoffs', 'market', 'stock', 'index']

# definition of functions


def preprocess_overalltweet(message):
    """
        store the data in collection after manupulation in mongodb collection overall_tweet_per_country
        :collection schema
         {
           _id: objectid
           country:string
           count: int
           month: int
        }
        :passing argument
        message : dictionary storing information of tweet
        :param
        new_dt = extract only date and time in string format
        created_at = convert string date object format
        month = extract the month number and store in this variable
        values = store the dictionary
    """
    print("query 1 called")
    new_dt = str(message['created_at'])[:19]
    created_at = datetime.strptime(new_dt, '%Y-%m-%d %H:%M:%S')
    print(created_at)
    # created_at = datetime.strptime(message['created_at'], '%Y-%m-%d %H:%M:%S')
    month = created_at.month
    print("date",created_at.month)
    country = message['country']
    if re.compile('|'.join(keyword),re.IGNORECASE).search(message['tweet']):
        if re.compile('|'.join(list_of_country),re.IGNORECASE).search(country):
            country=re.compile('|'.join(list_of_country),re.IGNORECASE).search(country).group(0).title()
            values = {'country': country, 'month': month, 'count': 1}
            print("last n month",values.keys(),values.values())
            if values is not None:
                if db['overall_tweet_per_country'].count_documents(
                        {"country": values['country'], "month": values['month']}) == 0:
                    # print("adsf",db)
                    db['overall_tweet_per_country'].insert_one(
                        {'count': values['count'], 'country': values['country'], 'month': values['month']})
                    # print("happys",db['overall_tweet_per_country'].find())
                else:
                    db['overall_tweet_per_country'].update_one(
                        {"country": values['country'], "month": values['month']}, {'$inc': {'count': 1}})



def preprocess_total_tweet_per_country(message):
    """
        store the data in collection after manupulation in mongodb collection overall_tweet_per_country_on_daily_basis
        :collection schema
         {
           _id: objectid
           country:string
           count: int
           date: string
        }
        :passing argument
        message : dictionary storing information of tweet
        :param
        new_dt = extract only date and time in string format
        created_at = convert string date object format
        country = store the country name
        daily_basis_tweet = store the dictionary
    """
    print("query 2 called")
    new_dt = str(message['created_at'])[:19]
    created_at = datetime.strptime(new_dt, '%Y-%m-%d %H:%M:%S')
    print(created_at)
    country = message['country']
    # print("overalltweet_per_country",country)
    if re.compile('|'.join(list_of_country), re.IGNORECASE).search(country):
        country = re.compile('|'.join(list_of_country), re.IGNORECASE).search(country).group(0).title()
        daily_basis_tweet = {'country': country,'count': 1,'date':created_at}
        # print("daily basis",data.keys(),data.values())
        if daily_basis_tweet is not None:
            print('something to insert in data',daily_basis_tweet.keys(),daily_basis_tweet.values())
            if db['overall_tweet_per_country_on_daily_basis'].count_documents({"country": daily_basis_tweet['country'], "date": str(daily_basis_tweet['date'])}) == 0:
                db['overall_tweet_per_country_on_daily_basis'].insert_one({'count': daily_basis_tweet['count'], 'country': daily_basis_tweet['country'], 'date': str(daily_basis_tweet['date'])})
            else:
                db['overall_tweet_per_country_on_daily_basis'].update_one({"country":daily_basis_tweet['country'],"date":str(daily_basis_tweet['date'])},{'$inc':{'count':1}})

def preprocess_top_100_words(message):
    """
        store the data in collection after manupulation in mongodb collection  top_100_words
        :collection schema
         {
           _id: objectid
           country:string
           count: int
           word: string
        }
        :passing argument
        message : dictionary storing information of tweet
        :param
        country = store the country name
        list_of_words = store the string of words with separated by _
    """
    print("query 3 called")

    if re.compile("coronavirus", re.IGNORECASE).search(message['tweet']):
        country = message['country']
        # print(country)
        if re.compile('|'.join(list_of_country), re.IGNORECASE).search(country):
            list_of_words = clean_tweet(message['tweet'])
            country = re.compile('|'.join(list_of_country), re.IGNORECASE).search(country).group(0).title()
            # print("list of words",list_of_words)
            # print("country:", country)
            if len(list_of_words) > 0:
                for words in list_of_words.split(" "):
                    if db['top_100_words'].count_documents({"word": words.title(), "country": country}) == 0:
                        db['top_100_words'].insert_one({'word': words.title(), "country": country, 'count': 1})
                    else:
                        db['top_100_words'].update_one({'word': words.title(), "country": country},
                                                       {'$inc': {'count': 1}})


def preprocess_top_10_precaution_word(message):
    """
           store the data in collection after manupulation in mongodb collection top_10_preventive_words
           :collection schema
             {
               _id: objectid
               country:string
               count: int
               word: string
            }
           :passing argument
           message : dictionary storing information of tweet
           :param
           new_dt = extract only date and time in string format
           created_at = convert string date object format
           country = store the country name
           word_list = store all the words match in tweet
    """
    print("query 5 called")
    new_dt = str(message['created_at'])[:19]
    created_at = datetime.strptime(new_dt, '%Y-%m-%d %H:%M:%S')
    print(created_at)
    if re.compile('|'.join(prevention_list), re.IGNORECASE).search(message['tweet']):
        country = message['country']
        print(country)
        words_list = re.compile('|'.join(prevention_list), re.IGNORECASE).findall(message['tweet'])
        print("list of word", words_list)
        if re.compile('|'.join(list_of_country), re.IGNORECASE).search(country):
            country = re.compile('|'.join(list_of_country), re.IGNORECASE).search(country).group(0).title()
            print("list of words", words_list)
            for words in words_list:
                if db['top_10_preventive_words'].count_documents({"word": words.title(), "country": country}) == 0:
                    db['top_10_preventive_words'].insert_one({'word': words.title(), "country": country, 'count': 1})
                else:
                    db['top_10_preventive_words'].update_one({'word': words.title(), "country": country},
                                                             {'$inc': {'count': 1}})
                print("inserted_success fully")


def preprocess_total_number_of_donation(message):
    """
       store the data in collection after manupulation in mongodb collection count_of_donation_in_covid
       :collection schema
         {
           _id: objectid
           country:string
           count: int
           date: string
        }
       :passing argument
       message : dictionary storing information of tweet
       :param
       country = store the country name

    """
    print("query 6 called")
    # list_of_new_words = ["donat.*", "contribut.*", "covid.*", "corona.*"]
    if re.compile("[donat.*,covid.*]", re.IGNORECASE).search(message['tweet']):
        country = message['country']
        if re.compile('|'.join(list_of_country), re.IGNORECASE).search(country):
           country = re.compile('|'.join(list_of_country), re.IGNORECASE).search(country).group(0).title()
           if db['count_of_donation_in_covid'].count_documents({"country":country})==0:
               db['count_of_donation_in_covid'].insert_one({"country":country,'count':1})
           else:
               db['count_of_donation_in_covid'].update_one({"country": country},{'$inc':{'count':1}})
           print("inserted_success fully")


def preprocess_tweets_based_on_trends(message):
    """
           store the data in collection after based on  trend  in different collection like if trend is covid than data insert in impact_analysis_on_covid_keys collection or if trend is economy then data insert in impact_analysis_on_economy_keys
           :collection schema
         {
           _id: objectid
           country:string
           count: int
           trend: string
        }
           :passing argument
           message : dictionary storing information of tweet
           :param
           country = store the country name
        """
    # print("query  called")
    print("query 8 executed")
    if re.compile("|".join(covid_keys), re.IGNORECASE).search(message['tweet']):
        country = message['country']
        print("query 8 country",country)
        if re.compile('|'.join(list_of_country), re.IGNORECASE).search(country):
            country = re.compile('|'.join(list_of_country), re.IGNORECASE).search(country).group(0).title()
            if db['impact_analysis_on_covid_keys'].count_documents({"country":country,"trend":"covid"})==0:
                db['impact_analysis_on_covid_keys'].insert_one({"country":country,'count':1,"trend":"covid"})
            else:
                db['impact_analysis_on_covid_keys'].update_one({"country":country,"trend":"covid"},{"$inc":{'count': 1}})
            print("query8 inserteds")
    if re.compile("|".join(economy_keys), re.IGNORECASE).search(message['tweet']):
        country = message['country']
        if re.compile('|'.join(list_of_country), re.IGNORECASE).search(country):
            country = re.compile('|'.join(list_of_country), re.IGNORECASE).search(country).group(0).title()
            if db['impact_analysis_on_economy_keys'].count_documents({"country": country, "trend": "economy"}) == 0:
                db['impact_analysis_on_economy_keys'].insert_one({"country": country, 'count': 1, "trend": "economy"})
            else:
                db['impact_analysis_on_economy_keys'].update_one({"country": country, "trend": "economy"},
                                                 {"$inc": {'count': 1}})
            print("query8 inserteds")