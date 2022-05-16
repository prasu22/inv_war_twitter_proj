# Base Image
FROM python:3.8-slim

ARG PRODUCER_VERSION=0.1.0
ARG PRODUCER_HOME=/opt/producer
ENV PYTHONPATH "${PYTHONPATH}:${PRODUCER_HOME}"
ENV PRODUCER_HOME=${PRODUCER_HOME}

ARG APP_DIR=/opt/inv_war_twitter
ENV APP_DIR=${APP_DIR}
RUN mkdir -p ${APP_DIR}/logs
RUN mkdir -p ${APP_DIR}/config

COPY ./requirements.txt ./requirements.txt
copy ./producer_start.sh ./producer_start.sh


RUN pip install --upgrade pip && \
    useradd -ms /bin/bash -d ${PRODUCER_HOME} producer && \
    pip install -r requirements.txt

RUN chmod +x ./producer_start.sh
RUN chown -R producer: ${PRODUCER_HOME}
RUN chown -R producer: ${APP_DIR}

USER producer
WORKDIR ${PRODUCER_HOME}

ENTRYPOINT [ "/producer_start.sh" ]


