FROM python:3.8-slim

ARG AIRFLOW_VERSION=2.3.0
ARG AIRFLOW_HOME=/opt/airflow
ENV AIRFLOW_HOME=${AIRFLOW_HOME}

ARG APP_DIR=/opt/inv_war_twitter
ENV APP_DIR=${APP_DIR}
RUN mkdir -p ${APP_DIR}/logs
RUN mkdir -p ${APP_DIR}/config

RUN apt-get update -yqq && \
    apt-get upgrade -yqq && \
    apt-get install -yqq --no-install-recommends \ 
    wget \
    libczmq-dev \
    curl \
    libssl-dev \
    git \
    inetutils-telnet \
    bind9utils freetds-dev \
    libkrb5-dev \
    libsasl2-dev \
    libffi-dev libpq-dev \
    freetds-bin build-essential \
    default-libmysqlclient-dev \
    apt-utils \
    rsync \
    zip \
    unzip \
    gcc \
    vim \
    locales \
    && apt-get clean

COPY ./requirements.txt ./requirements.txt

RUN pip install --upgrade pip && \
    useradd -ms /bin/bash -d ${AIRFLOW_HOME} airflow && \
    pip install -r requirements.txt & \
    pip install apache-airflow[postgres]==${AIRFLOW_VERSION} --constraint https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.7.txt

COPY ./airflow_start.sh ./airflow_start.sh


RUN chmod +x ./airflow_start.sh

RUN chown -R airflow: ${AIRFLOW_HOME}
RUN chown -R airflow: ${APP_DIR}
COPY ./app_config.ini ${APP_DIR}/config/app_config.ini
USER airflow
WORKDIR ${AIRFLOW_HOME}

RUN mkdir dags
EXPOSE 8080

ENTRYPOINT [ "/airflow_start.sh" ]

    
