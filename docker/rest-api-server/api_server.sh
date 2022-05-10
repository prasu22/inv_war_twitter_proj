#!/usr/bin/env bash

#exec rest server
exec gunicorn src.rest_api.app:app -w 2 --threads 2 -b 0.0.0.0:5056