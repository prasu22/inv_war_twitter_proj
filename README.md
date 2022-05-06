# inv_war_twitter_proj
Training mock project 


## Connecting MongoDB with Airflow

### Deployment

This example is made to work with the Connecting the MongoDB Atlas with Airflow.

In order to create the image of docker, clone the repository

```
git clone https://github.com/prasu22/inv_war_twitter_proj.git
```

install dependencies with by copying requirements.txt in the root.
You can add in the ```Dockerfile``` :

```
COPY ./requirements.txt /requirements.txt
```
```
COPY config/airflow.cfg ${ AIRFLOW_USER_HOME}/airflow.cfg/
```

#### Or,
Install all the packages in the docker container manually

```
docker exec ti- <image-id>
```

Important packages require for connecting MongoDB Atlas:
```
pip install pymongo
```
```
pip install pymongo[srv]
```

### Creating the Dag

Create a simple Dag with PythonOpeartor calling MongoDB connection function
```
with DAG("Mongo_Conn",default_args=default_args, schedule_interval="@daily", catchup=False) as dag:
    t1=PythonOperator(task_id="Mongo-Conn", python_callable=mongodb_connection)
```

### Build

By default, docker-airflow runs Airflow with SequentialExecutor:
```
docker run -d -p 8080:8080 puckel/docker-airflow webserver
```
If you want to run another executor, use the other docker-compose.yml files provided in this repository.

For LocalExecutor :
```
docker-compose -f docker-compose-LocalExecutor.yml up -d
```
