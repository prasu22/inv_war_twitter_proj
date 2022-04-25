import json
import re
from datetime import datetime
from kafka import KafkaConsumer
from mongodb.mongo_data_connector import connect_with_collection_data, mongodb_connection
from rest_flask.data_cleaning import clean_tweet


my_consumer = KafkaConsumer(
        'sendingdata',
        bootstrap_servers=['localhost : 9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

my_coll = connect_with_collection_data()
conn = mongodb_connection()
db = conn['tweet_db']

# ======================================================================================================================
# code used if we fetech data using fetch_data_from_twitter()
# static lists
keyword = ['covid','virus','coronavirus']
list_of_country = ['Afghanistan', 'Aland Islands', 'Albania', 'Algeria', 'American Samoa', 'Andorra', 'Angola', 'Anguilla', 'Antarctica', 'Antigua and Barbuda', 'Argentina', 'Armenia', 'Aruba', 'Australia', 'Austria', 'Azerbaijan', 'Bahamas', 'Bahrain', 'Bangladesh', 'Barbados', 'Belarus', 'Belgium', 'Belize', 'Benin', 'Bermuda', 'Bhutan', 'Bolivia, Plurinational State of', 'Bonaire, Sint Eustatius and Saba', 'Bosnia and Herzegovina', 'Botswana', 'Bouvet Island', 'Brazil', 'British Indian Ocean Territory', 'Brunei Darussalam', 'Bulgaria', 'Burkina Faso', 'Burundi', 'Cambodia', 'Cameroon', 'Canada', 'Cape Verde', 'Cayman Islands', 'Central African Republic', 'Chad', 'Chile', 'China', 'Christmas Island', 'Cocos (Keeling) Islands', 'Colombia', 'Comoros', 'Congo', 'Congo, The Democratic Republic of the', 'Cook Islands', 'Costa Rica', "Côte d'Ivoire", 'Croatia', 'Cuba', 'Curaçao', 'Cyprus', 'Czech Republic', 'Denmark', 'Djibouti', 'Dominica', 'Dominican Republic', 'Ecuador', 'Egypt', 'El Salvador', 'Equatorial Guinea', 'Eritrea', 'Estonia', 'Ethiopia', 'Falkland Islands (Malvinas)', 'Faroe Islands', 'Fiji', 'Finland', 'France', 'French Guiana', 'French Polynesia', 'French Southern Territories', 'Gabon', 'Gambia', 'Georgia', 'Germany', 'Ghana', 'Gibraltar', 'Greece', 'Greenland', 'Grenada', 'Guadeloupe', 'Guam', 'Guatemala', 'Guernsey', 'Guinea', 'Guinea-Bissau', 'Guyana', 'Haiti', 'Heard Island and McDonald Islands', 'Holy See (Vatican City State)', 'Honduras', 'Hong Kong', 'Hungary', 'Iceland', 'India', 'Indonesia', 'Iran, Islamic Republic of', 'Iraq', 'Ireland', 'Isle of Man', 'Israel', 'Italy', 'Jamaica', 'Japan', 'Jersey', 'Jordan', 'Kazakhstan', 'Kenya', 'Kiribati', "Korea, Democratic People's Republic of", 'Korea, Republic of', 'Kuwait', 'Kyrgyzstan', "Lao People's Democratic Republic", 'Latvia', 'Lebanon', 'Lesotho', 'Liberia', 'Libya', 'Liechtenstein', 'Lithuania', 'Luxembourg', 'Macao', 'Macedonia, Republic of', 'Madagascar', 'Malawi', 'Malaysia', 'Maldives', 'Mali', 'Malta', 'Marshall Islands', 'Martinique', 'Mauritania', 'Mauritius', 'Mayotte', 'Mexico', 'Micronesia, Federated States of', 'Moldova, Republic of', 'Monaco', 'Mongolia', 'Montenegro', 'Montserrat', 'Morocco', 'Mozambique', 'Myanmar', 'Namibia', 'Nauru', 'Nepal', 'Netherlands', 'New Caledonia', 'New Zealand', 'Nicaragua', 'Niger', 'Nigeria', 'Niue', 'Norfolk Island', 'Northern Mariana Islands', 'Norway', 'Oman', 'Pakistan', 'Palau', 'Palestinian Territory, Occupied', 'Panama', 'Papua New Guinea', 'Paraguay', 'Peru', 'Philippines', 'Pitcairn', 'Poland', 'Portugal', 'Puerto Rico', 'Qatar', 'Réunion', 'Romania', 'Russian Federation', 'Rwanda', 'Saint Barthélemy', 'Saint Helena, Ascension and Tristan da Cunha', 'Saint Kitts and Nevis', 'Saint Lucia', 'Saint Martin (French part)', 'Saint Pierre and Miquelon', 'Saint Vincent and the Grenadines', 'Samoa', 'San Marino', 'Sao Tome and Principe', 'Saudi Arabia', 'Senegal', 'Serbia', 'Seychelles', 'Sierra Leone', 'Singapore', 'Sint Maarten (Dutch part)', 'Slovakia', 'Slovenia', 'Solomon Islands', 'Somalia', 'South Africa', 'South Georgia and the South Sandwich Islands', 'Spain', 'Sri Lanka', 'Sudan', 'Suriname', 'South Sudan', 'Svalbard and Jan Mayen', 'Swaziland', 'Sweden', 'Switzerland', 'Syrian Arab Republic', 'Taiwan, Province of China', 'Tajikistan', 'Tanzania, United Republic of', 'Thailand', 'Timor-Leste', 'Togo', 'Tokelau', 'Tonga', 'Trinidad and Tobago', 'Tunisia', 'Turkey', 'Turkmenistan', 'Turks and Caicos Islands', 'Tuvalu', 'Uganda', 'Ukraine', 'United Arab Emirates', 'United Kingdom', 'United States', 'United States Minor Outlying Islands', 'Uruguay', 'Uzbekistan', 'Vanuatu', 'Venezuela, Bolivarian Republic of', 'Viet Nam', 'Virgin Islands, British', 'Virgin Islands, U.S.', 'Wallis and Futuna', 'Yemen', 'Zambia', 'Zimbabwe']
prevention_list = ['mask','sanitiser','stay home','social distancing','wash hands']
# =======================================================================================================================================
def overalltweet(message):
    created_at = datetime.strptime(message['created_at'], '%Y-%m-%d %H:%M:%S')
    month = created_at.month
    print("date",created_at.month)
    country = message['country']
    if 'RT @' not in message['tweet']:
        if re.compile('|'.join(keyword),re.IGNORECASE).search(message['tweet']):
            if re.compile('|'.join(list_of_country),re.IGNORECASE).search(country):
                country=re.compile('|'.join(list_of_country),re.IGNORECASE).search(country).group(0).title()
                data = {'country': country, 'month': month, 'count': 1}
                print("last n month",data.keys(),data.values())
                return data
    return None
# =======================================================================================================================================

# =======================================================================================================================================
def overalltweet_per_country(message):
    created_at = datetime.strptime(message['created_at'], '%Y-%m-%d %H:%M:%S').date()
    print(created_at)
    country = message['country']
    # print("overalltweet_per_country",country)
    if 'RT @' not in message['tweet']:
        if re.compile('|'.join(list_of_country), re.IGNORECASE).search(country):
            country = re.compile('|'.join(list_of_country), re.IGNORECASE).search(country).group(0).title()
            data = {'country': country,'count': 1,'date':created_at}
            # print("daily basis",data.keys(),data.values())
            return data
    return None
# =======================================================================================================================================

for message in my_consumer:
    message = message.value
    print(message['created_at'])
    new_dt = str(message['created_at'])[:19]
    created_at = datetime.strptime(new_dt,'%Y-%m-%d %H:%M:%S')
    print(created_at)
    try:
        if 'RT @' not in message['tweet']:
# =======================================================================================================================================
            # query1
            values = overalltweet(message)
            if values is not None:
                if db['overall_tweet_per_country'].count_documents({"country":values['country'],"month":values['month']})==0:
                    # print("adsf",db)
                    db['overall_tweet_per_country'].insert_one({'count':values['count'],'country':values['country'],'month':values['month']})
                    # print("happys",db['overall_tweet_per_country'].find())
                else:
                    db['overall_tweet_per_country'].update_one({"country":values['country'],"month":values['month']},{'$inc':{'count':1}})
# # =======================================================================================================================================
#             # query2
            # print("successfull for first one query")
            daily_basis_tweet = overalltweet_per_country(message)
            if daily_basis_tweet is not None:
                print('something to insert in data',daily_basis_tweet.keys(),daily_basis_tweet.values())
                if db['overall_tweet_per_country_on_daily_basis'].count_documents({"country": daily_basis_tweet['country'], "date": str(daily_basis_tweet['date'])}) == 0:
                    db['overall_tweet_per_country_on_daily_basis'].insert_one({'count': daily_basis_tweet['count'], 'country': daily_basis_tweet['country'], 'date': str(daily_basis_tweet['date'])})
                else:
                    db['overall_tweet_per_country_on_daily_basis'].update_one({"country":daily_basis_tweet['country'],"date":str(daily_basis_tweet['date'])},{'$inc':{'count':1}})
# # =======================================================================================================================================
#             # query3
            if re.compile("coronavirus",re.IGNORECASE).search(message['tweet']):
                country = message['country']
                # print(country)
                if re.compile('|'.join(list_of_country), re.IGNORECASE).search(country):
                    list_of_words = clean_tweet(message['tweet'])
                    country = re.compile('|'.join(list_of_country), re.IGNORECASE).search(country).group(0).title()
                    # print("list of words",list_of_words)
                    # print("country:", country)
                    if len(list_of_words) > 0:
                        for words in list_of_words.split(" "):
                           if db['top_100_words'].count_documents({"word":words,"country":country})==0:
                               db['top_100_words'].insert_one({'word':words,"country":country,'count':1})
                           else:
                               db['top_100_words'].update_one({'word': words, "country": country},{'$inc':{'count':1}})

# =======================================================================================================================================
              #query5
                if re.compile('|'.join(prevention_list), re.IGNORECASE).search(message['tweet']):
                    country = message['country']
                    print(country)
                    words_list = re.compile('|'.join(prevention_list), re.IGNORECASE).findall(message['tweet'])
                    print("list of word",words_list)
                    if re.compile('|'.join(list_of_country), re.IGNORECASE).search(country):
                        country = re.compile('|'.join(list_of_country), re.IGNORECASE).search(country).group(0).title()
                        print("list of words",words_list)
                        for words in words_list:
                           if db['top_10_preventive_words'].count_documents({"word":words,"country":country})==0:
                               db['top_10_preventive_words'].insert_one({'word':words,"country":country,'count':1})
                           else:
                               db['top_10_preventive_words'].update_one({'word': words, "country": country},{'$inc':{'count':1}})
                           print("inserted_success fully")
# =======================================================================================================================================
              #query6
                list_of_new_words = ["donat.*","contribut.*","covid.*","corona.*"]
                if re.compile("[donat.*,covid.*]", re.IGNORECASE).search(message['tweet']):
                    country = message['country']
                    if re.compile('|'.join(list_of_country), re.IGNORECASE).search(country):
                       country = re.compile('|'.join(list_of_country), re.IGNORECASE).search(country).group(0).title()
                       if db['count_of_donation_in_covid'].count_documents({"country":country})==0:
                           db['count_of_donation_in_covid'].insert_one({"country":country,'count':1})
                       else:
                           db['count_of_donation_in_covid'].update_one({"country": country},{'$inc':{'count':1}})
                       print("inserted_success fully")

# ======================================================================================================================





    except Exception as e:
        print(f"error {e}")
        pass
# ======================================================================================================================
