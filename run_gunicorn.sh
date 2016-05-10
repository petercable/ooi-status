#!/bin/bash

export PSYCOGREEN=true
gunicorn --log-config logging.conf -w 2 -k gevent -b 0.0.0.0:12571 ooi_status.api:app
