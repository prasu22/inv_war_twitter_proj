# syntax=docker/dockerfile:1
FROM python:3.7-alpine
WORKDIR /code
ENV PYTHONPATH "${PYTHONPATH}:/code"
RUN echo $PYTHONPATH
ENV FLASK_APP=./src/rest_flask/api.py
ENV FLASK_RUN_HOST=0.0.0.0
RUN mkdir -p /tmp/var/log/twitter_proj
RUN apk add --no-cache gcc musl-dev linux-headers
COPY requirements.txt .
RUN pip install -r requirements.txt
EXPOSE 5000
COPY . .
CMD ["flask", "run"]
