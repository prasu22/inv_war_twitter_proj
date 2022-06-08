# ======================================================================================================================
COUNTRY_CODE_MAPPED = {'Afghanistan': 'AF', 'Aland Islands': 'AX', 'Albania': 'AL', 'Algeria': 'DZ', 'American Samoa': 'AS', 'Andorra': 'AD', 'Angola': 'AO', 'Anguilla': 'AI', 'Antarctica': 'AQ', 'Antigua and Barbuda': 'AG', 'Argentina': 'AR', 'Armenia': 'AM', 'Aruba': 'AW', 'Australia': 'AU', 'Austria': 'AT', 'Azerbaijan': 'AZ', 'Bahamas': 'BS', 'Bahrain': 'BH', 'Bangladesh': 'BD', 'Barbados': 'BB', 'Belarus': 'BY', 'Belgium': 'BE', 'Belize': 'BZ', 'Benin': 'BJ', 'Bermuda': 'BM', 'Bhutan': 'BT', 'Bolivia, Plurinational State of': 'BO', 'Bonaire, Sint Eustatius and Saba': 'BQ', 'Bosnia and Herzegovina': 'BA', 'Botswana': 'BW', 'Bouvet Island': 'BV', 'Brazil': 'BR', 'British Indian Ocean Territory': 'IO', 'Brunei Darussalam': 'BN', 'Bulgaria': 'BG', 'Burkina Faso': 'BF', 'Burundi': 'BI', 'Cambodia': 'KH', 'Cameroon': 'CM', 'Canada': 'CA', 'Cape Verde': 'CV', 'Cayman Islands': 'KY', 'Central African Republic': 'CF', 'Chad': 'TD', 'Chile': 'CL', 'China': 'CN', 'Christmas Island': 'CX', 'Cocos (Keeling) Islands': 'CC', 'Colombia': 'CO', 'Comoros': 'KM', 'Congo': 'CG', 'Congo, The Democratic Republic of the': 'CD', 'Cook Islands': 'CK', 'Costa Rica': 'CR', "Côte d'Ivoire": 'CI', 'Croatia': 'HR', 'Cuba': 'CU', 'Curaçao': 'CW', 'Cyprus': 'CY', 'Czech Republic': 'CZ', 'Denmark': 'DK', 'Djibouti': 'DJ', 'Dominica': 'DM', 'Dominican Republic': 'DO', 'Ecuador': 'EC', 'Egypt': 'EG', 'El Salvador': 'SV', 'Equatorial Guinea': 'GQ', 'Eritrea': 'ER', 'Estonia': 'EE', 'Ethiopia': 'ET', 'Falkland Islands (Malvinas)': 'FK', 'Faroe Islands': 'FO', 'Fiji': 'FJ', 'Finland': 'FI', 'France': 'FR', 'French Guiana': 'GF', 'French Polynesia': 'PF', 'French Southern Territories': 'TF', 'Gabon': 'GA', 'Gambia': 'GM', 'Georgia': 'GE', 'Germany': 'DE', 'Ghana': 'GH', 'Gibraltar': 'GI', 'Greece': 'GR', 'Greenland': 'GL', 'Grenada': 'GD', 'Guadeloupe': 'GP', 'Guam': 'GU', 'Guatemala': 'GT', 'Guernsey': 'GG', 'Guinea': 'GN', 'Guinea-Bissau': 'GW', 'Guyana': 'GY', 'Haiti': 'HT', 'Heard Island and McDonald Islands': 'HM', 'Holy See (Vatican City State)': 'VA', 'Honduras': 'HN', 'Hong Kong': 'HK', 'Hungary': 'HU', 'Iceland': 'IS', 'India': 'IN', 'Indonesia': 'ID', 'Iran, Islamic Republic of': 'IR', 'Iraq': 'IQ', 'Ireland': 'IE', 'Isle of Man': 'IM', 'Israel': 'IL', 'Italy': 'IT', 'Jamaica': 'JM', 'Japan': 'JP', 'Jersey': 'JE', 'Jordan': 'JO', 'Kazakhstan': 'KZ', 'Kenya': 'KE', 'Kiribati': 'KI', "Korea, Democratic People's Republic of": 'KP', 'Korea, Republic of': 'KR', 'Kuwait': 'KW', 'Kyrgyzstan': 'KG', "Lao People's Democratic Republic": 'LA', 'Latvia': 'LV', 'Lebanon': 'LB', 'Lesotho': 'LS', 'Liberia': 'LR', 'Libya': 'LY', 'Liechtenstein': 'LI', 'Lithuania': 'LT', 'Luxembourg': 'LU', 'Macao': 'MO', 'Macedonia, Republic of': 'MK', 'Madagascar': 'MG', 'Malawi': 'MW', 'Malaysia': 'MY', 'Maldives': 'MV', 'Mali': 'ML', 'Malta': 'MT', 'Marshall Islands': 'MH', 'Martinique': 'MQ', 'Mauritania': 'MR', 'Mauritius': 'MU', 'Mayotte': 'YT', 'Mexico': 'MX', 'Micronesia, Federated States of': 'FM', 'Moldova, Republic of': 'MD', 'Monaco': 'MC', 'Mongolia': 'MN', 'Montenegro': 'ME', 'Montserrat': 'MS', 'Morocco': 'MA', 'Mozambique': 'MZ', 'Myanmar': 'MM', 'Namibia': 'NA', 'Nauru': 'NR', 'Nepal': 'NP', 'Netherlands': 'NL', 'New Caledonia': 'NC', 'New Zealand': 'NZ', 'Nicaragua': 'NI', 'Niger': 'NE', 'Nigeria': 'NG', 'Niue': 'NU', 'Norfolk Island': 'NF', 'Northern Mariana Islands': 'MP', 'Norway': 'NO', 'Oman': 'OM', 'Pakistan': 'PK', 'Palau': 'PW', 'Palestinian Territory, Occupied': 'PS', 'Panama': 'PA', 'Papua New Guinea': 'PG', 'Paraguay': 'PY', 'Peru': 'PE', 'Philippines': 'PH', 'Pitcairn': 'PN', 'Poland': 'PL', 'Portugal': 'PT', 'Puerto Rico': 'PR', 'Qatar': 'QA', 'Réunion': 'RE', 'Romania': 'RO', 'Russian Federation': 'RU', 'Rwanda': 'RW', 'Saint Barthélemy': 'BL', 'Saint Helena, Ascension and Tristan da Cunha': 'SH', 'Saint Kitts and Nevis': 'KN', 'Saint Lucia': 'LC', 'Saint Martin (French part)': 'MF', 'Saint Pierre and Miquelon': 'PM', 'Saint Vincent and the Grenadines': 'VC', 'Samoa': 'WS', 'San Marino': 'SM', 'Sao Tome and Principe': 'ST', 'Saudi Arabia': 'SA', 'Senegal': 'SN', 'Serbia': 'RS', 'Seychelles': 'SC', 'Sierra Leone': 'SL', 'Singapore': 'SG', 'Sint Maarten (Dutch part)': 'SX', 'Slovakia': 'SK', 'Slovenia': 'SI', 'Solomon Islands': 'SB', 'Somalia': 'SO', 'South Africa': 'ZA', 'South Georgia and the South Sandwich Islands': 'GS', 'Spain': 'ES', 'Sri Lanka': 'LK', 'Sudan': 'SD', 'Suriname': 'SR', 'South Sudan': 'SS', 'Svalbard and Jan Mayen': 'SJ', 'Swaziland': 'SZ', 'Sweden': 'SE', 'Switzerland': 'CH', 'Syrian Arab Republic': 'SY', 'Taiwan, Province of China': 'TW', 'Tajikistan': 'TJ', 'Tanzania, United Republic of': 'TZ', 'Thailand': 'TH', 'Timor-Leste': 'TL', 'Togo': 'TG', 'Tokelau': 'TK', 'Tonga': 'TO', 'Trinidad and Tobago': 'TT', 'Tunisia': 'TN', 'Turkey': 'TR', 'Turkmenistan': 'TM', 'Turks and Caicos Islands': 'TC', 'Tuvalu': 'TV', 'Uganda': 'UG', 'Ukraine': 'UA', 'United Arab Emirates': 'AE', 'United Kingdom': 'GB', 'United States': 'US', 'United States Minor Outlying Islands': 'UM', 'Uruguay': 'UY', 'Uzbekistan': 'UZ', 'Vanuatu': 'VU', 'Venezuela, Bolivarian Republic of': 'VE', 'Viet Nam': 'VN', 'Virgin Islands, British': 'VG', 'Virgin Islands, U.S.': 'VI', 'Wallis and Futuna': 'WF', 'Yemen': 'YE', 'Zambia': 'ZM', 'Zimbabwe': 'ZW'}
COUNTRY_NAME = "no country"
COUNTRY_CODE = "no country code"
# ======================================================================================================================

CURRENCY_MAPPING ={"$":'USD',"¢":"GHS","£":"EGP","¥":"JPY","฿":"THB","€":"EUR","₹":"INR"}
DONATION_KEYWORDS = ["donation","Money","contribution","donate","contribute","fund"]
DEFAULT_CURRENCY_NAME = "no currency"
DEFAULT_AMOUNT = 0


# ======================================================================================================================


# All collection names from data analytics directory
DATABASE_TWEET_NEW_DB = "tweet_new_db"
COLL_OF_RAW_DATA = "tweet_processed_data"
COLL_METADATA = 'metadata_table'
COLL_OF_TOTAL_TWEET_PER_COUNTRY = 'a_overall_tweet_per_country'
COLL_TOP_100_WORDS = "a_top_100_words"
COLL_OF_TOP_10_PREVENTIVE_WORDS = 'a_top_10_prevention_country_code'
COLL_OF_WORDS_FROM_TWEETS= 'a_top_100_words'
COLL_OF_DONATION_PER_COUNTRY = 'a_total_number_of_donation_per_country'
COLL_OF_IMPACT_ANALYSIS_ON_COVID_KEYS = 'a_impact_analysis_on_covid_keys'
COLL_OF_IMPACT_ANALYSIS_ON_ECONOMY_KEYS = 'a_impact_analysis_on_economy_keys'
COLL_OF_RANKING_COUNTRY_BASED_ON_TWEET_WITH_COVID_KEYS = "a_ranking_of_impacted_on_covid_keys_countries"
COLL_OF_RANKING_COUNTRY_BASED_ON_TWEET_WITH_ECONOMY_KEYS = 'a_ranking_of_impacted_on_economy_keys_countries'
COLL_OF_TWEET_PER_COUNTRY_ON_DAILY_BASIS = "a_overall_tweet_per_country_on_daily_basis"



# ======================================================================================================================
#message dictionary keys
COUNTRY_NAME_KEY = 'country'
COUNTRY_CODE_KEY = 'country_code'
TWEET_KEY = 'tweet'
CREATED_AT_KEY = 'created_at'
ID_KEY = '_id'
DONATION_AMOUNT_KEY ='donation_amount'
CURRENCY_NAME_KEY = 'currency_name'
DONATION_KEYWORDS_KEY = "donation_keywords"
PREVENTION_KEYWORDS_KEY = "prevention_keywords"
WHO_KEYWORDS_KEY = "WHO_keywords"
COVID_TRENDING_KEYWORD_KEY = "covid_trending_keywords"
ECONOMY_TRENDING_KEYWORD_KEY = "economy_trending_keywords"
COVID_KEYWORD_KEY = "covid_keywords"
MONTH_KEY = "month"
YEAR_KEY = "year"
COUNT_KEY = "count"
WORD_KEY = 'word'
TREND_KEY = "trend"
ECONOMY_VALUE = "economy"
COVID_VALUE = "covid"
BATCH_SIZE = 500
TWEET_KEYWORDS = "tweet_keywords"
IS_COVID_TWEET = "is_covid_tweet"
######## metadata table keys

RECORD_IDS = 'record_ids'
TWEET_DAILY_AFTER ='tweet_daily_after'
TWEET_DAILY_BEFORE = 'tweet_daily_before'
TOP_WORDS_AFTER = 'top_words_after'
TOP_WORDS_BEFORE ='top_words_before'
DONATION_AFTER ='donation_after'
DONATION_BEFORE ='donation_before'
TOTAL_TWEETS_BEFORE = 'total_tweets_before'
TOTAL_TWEETS_AFTER = 'total_tweets_after'
PREVENTION_BEFORE ='prevention_before'
PREVENTION_AFTER ='prevention_after'


# ======================================================================================================================
# data validation
BOOTSTRAP_SERVER="localhost:9092"
TOPIC1 = "twitter_stream_rest_apis"
TOPIC2 = "tweepy_stream_data"
TOPIC3 = "search_api"
# GROUP_ID="my-group"
GROUP_ID="my-group"
#put the confing.ini file in common


# ======================================================================================================================


