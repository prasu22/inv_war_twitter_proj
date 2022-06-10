# reset docker container by recreating image
- docker-compose down
- docker system prune -f
- docker rmi -f $(docker images -a -q)
- docker-compose up -d --build

# other useful docker commands
- docker images
- docker ps
- docker logs <container_id>
- docker kill <container_id>
- docker exec -it <container_id> /bin/bash
- docker build . -t <image_name>
- docker run -it -d -p 8080:8080 <image_name>



# Get Started

### To clone and develop the project environment


### Clone the [inv_war_twitter_proj](https://github.com/prasu22/inv_war_twitter_proj) repository
```
git clone https://github.com/prasu22/inv_war_twitter_proj.git
```
```
cd inv_war_twitter_proj
```

### Install requirements
```
pip install -r requirements.txt
```

## To Create Docker container

### Build the docker container
```
docker-compose up -d --build
```

- Note: If Kafka didn't start then, restart the Kafka container and zookeeper container
```
docker-compose stop kafka zookeeper
```
```
docker-compose up -d kafka zookeeper
```

### To run the airflow UI

Hit the server: http://localhost:8000/




## REST Endpoints


### GET-- {baseURL}/total_tweet_count/<country_code>/<from_date>/<to_date>
- Returns the count of tweeets for a particular country and dates provided by the users (date format is: yyyy-mm)

Response Status Code: 
- 200: Return count of total tweets
- 500: Internal Server Error

### GET-- {baseURL}/number_of_tweet_per_country/<country_code>/<date>
- Returns the number of tweets on a particular date and country provided by the user.

Response Status Code: 
- 200: Return number of tweets
- 500: Internal Server Error

  
### GET-- {baseURL}/top_100_words
- Returns the top 100 most occurred words in all tweets.
  
Response Status Code: 
- 200: Return the top 100 words
- 500: Internal Server Error
  
  
### GET-- {baseURL}/top_100_words_country_code/<country_code>
- Returns the top 100 most occurred words in all tweets from particular country.
  
Response Status Code: 
- 200: Return the top 100 words per country
- 500: Internal Server Error
  

### GET-- {baseURL}/total_no_donations/<country_code>
- Return all the donations amount and currency listed in tweets from particular country
  
Response Status Code: 
- 200: Return donation amount and currency
- 500: Internal Server Error
  

### GET-- {baseURL}/top_10_preventions/<country_code>
- returns the prevention words and their count present in the tweets per country which has WHO mentioned in them.
  
Response Status Code: 
  - 200: Return prevention words and their count per country
  - 500: Internal Server Error
 
 
### GET-- {baseURL}/top_10_preventions_all_countries
- returns the top 10 prevention words in the tweets.
  
Response Status Code: 
  - 200: Return prevention words and their count per country
  - 500: Internal Server Error 
 
 
### GET-- {baseURL}/impact_analysis_on_covid_keys/<country_code>
- returns the count of tweets which has specific words related to covid per country
 
 Response Status Code: 
  - 200: Return count and country code per country
  - 500: Internal Server Error
  
 
 ### GET-- {baseURL}/impact_analysis_on_economy_keys/<country_code>
- returns the count of tweets which has specific words related to economy per country
 
 Response Status Code: 
  - 200: Return count and country code per country
  - 500: Internal Server Error 
  

 ### GET-- {baseURL}/impact_analysis_on_economy_keys_by_months/<country_code>
- returns the count of tweets which has specific words related to economy per country
 
 Response Status Code: 
  - 200: Return count and country code per country
  - 500: Internal Server Error 

 For more information about API, refer this: [Doc](https://docs.google.com/document/d/11Y_E7dSwzooN6lTaS5ROtsU5Qge4SWvyZ0kYmn3yxBE/edit)

 ## Data Visualization
  
 Install the jupyter lab and run the [data_visualisation.ipynb](https://github.com/prasu22/inv_war_twitter_proj/blob/develop/data_visualisation.ipynb)
  
 Run each the API and get the bar charts accordingly.
  
