#!/usr/bin/env bash

export AIRFLOW__CORE__LOAD_EXAMPLES=False
airflow db init
airflow scheduler &> /dev/null &
airflow users create -u admin -p admin -r Admin -e admin@admin.com -f admin -l admin
exec airflow webserver

