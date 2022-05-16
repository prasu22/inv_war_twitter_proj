FROM python:3.8-slim

ARG CONSUMER_VERSION=0.1.0
ARG CONSUMER_HOME=/opt/consumer
ENV PYTHONPATH "${PYTHONPATH}:${CONSUMER_HOME}"
ENV CONSUMER_HOME=${CONSUMER_HOME}

ARG APP_DIR=/opt/inv_war_twitter
ENV APP_DIR=${APP_DIR}
RUN mkdir -p ${APP_DIR}/logs
RUN mkdir -p ${APP_DIR}/config

COPY ./requirements.txt ./requirements.txt
copy ./consumer_start.sh ./consumer_start.sh
COPY ./app_config.ini ${APP_DIR}/config/app_config.ini


RUN pip install --upgrade pip && \
    useradd -ms /bin/bash -d ${CONSUMER_HOME} consumer && \
    pip install -r requirements.txt

RUN chmod +x ./consumer_start.sh
RUN chown -R consumer: ${CONSUMER_HOME}
RUN chown -R consumer: ${APP_DIR}


USER consumer
WORKDIR ${CONSUMER_HOME}
ENTRYPOINT [ "/consumer_start.sh" ]

