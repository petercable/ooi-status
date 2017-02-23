import os
import datetime
from flask import Flask
from flask.json import JSONEncoder
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session

from ooi_data.postgres.model import MonitorBase, MetadataBase


class StatusJsonEncoder(JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime.date):
            if isinstance(o, datetime.datetime):
                return str(o.replace(microsecond=0))
            return str(o)
        if hasattr(o, 'as_dict'):
            return o.as_dict()
        return JSONEncoder.default(self, o)


if 'PSYCOGREEN' in os.environ:
    from gevent.monkey import patch_all
    patch_all()
    from psycogreen.gevent import patch_psycopg
    patch_psycopg()

    using_gevent = True
else:
    using_gevent = False


app = Flask(__name__)

app.config.from_object('ooi_status.default_settings')
if 'OOISTATUS_SETTINGS' in os.environ:
    app.config.from_envvar('OOISTATUS_SETTINGS')
app.json_encoder = StatusJsonEncoder


app.engine = create_engine(app.config['MONITOR_URL'])
app.metadata_engine = create_engine(app.config['METADATA_URL'])

app.sessionmaker = sessionmaker(bind=app.engine)
app.session = scoped_session(app.sessionmaker)
app.metadata_sessionmaker = sessionmaker(bind=app.metadata_engine)
app.metadata_session = scoped_session(app.metadata_sessionmaker)

MetadataBase.query = app.session.query_property()
MonitorBase.query = app.session.query_property()

if using_gevent:
    app.engine.pool._use_threadlocal = True


import ooi_status.api.views
