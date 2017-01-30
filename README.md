# OOI Status

This project contains code to automate the monitoring of data ingestion in the OOI system.

To install this project for development (pip):

```
$ mkvirtualenv status
$ pip install -U pip
$ pip install -r requirements.txt
```

To install this project for development (conda):

```
$ conda env create -f conda_env.yml
```

To install this project for test/production (pip):

```
$ mkvirtualenv status
$ pip install -U pip
$ pip install -r requirements.txt
$ pip install .
```

To install this project for test/production (conda):

```
$ conda config --append channels ooi
$ conda config --append channels conda-forge
$ conda create -n status ooi-status
```

The project has two runnable services, the status monitor backend and a corresponding HTTP API service which allows
the backend to be configured and supports querying various status-related items. To run the backend:

```
$ ooi_status_monitor
```

And to run the HTTP API service:

```
$ PSYCOGREEN=true gunicorn -w 2 -k gevent -b 0.0.0.0:9000 ooi_status.api:app
```

See the gunicorn documentation for more information on the various options available for gunicorn.