# Base Image
FROM python:3.8-slim

ARG API_VERSION=0.1.0
ARG API_HOME=/opt/restapi
ENV API_HOME=${API_HOME}
ENV API_VERSION=${API_VERSION}

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
    procps \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean


COPY ./requirements.txt ./requirements.txt
COPY ./api_server.sh ./api_server.sh
COPY ./app_config.ini ${APP_DIR}/config/app_config.ini

RUN pip install --upgrade pip && \
    useradd -ms /bin/bash -d ${API_HOME} restapi && \
    pip install -r requirements.txt


RUN chmod +x ./api_server.sh
RUN chown -R restapi: ${API_HOME}
RUN chown -R restapi: ${APP_DIR}

USER restapi
WORKDIR ${API_HOME}
EXPOSE 5056
ENTRYPOINT [ "/api_server.sh" ]

    
