#!/bin/bash

export PSYCOGREEN=true
gunicorn -b 0.0.0.0:12571 -k gevent ooi_status.api:app
