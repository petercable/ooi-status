"""
RSN health and status monitor for data particles received by OOI CI.

@author Dan Mergens
"""
import datetime
import logging
import os

import click
import pandas as pd
from apscheduler.schedulers.blocking import BlockingScheduler
from flask import Config
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql.elements import and_

from ooi_status.amqp_client import AmqpStatsClient
from ooi_status.emailnotifier import EmailNotifier
from ooi_status.metadata_queries import get_active_streams
from ooi_status.status_message import StatusMessage
from .get_logger import get_logger
from .model.status_model import (DeployedStream, ExpectedStream, ReferenceDesignator, NotifyAddress, create_database,
                                 StreamCondition)
from .queries import (resample_stream_count, resample_port_count,
                      create_daily_digest_plots, create_daily_digest_html, get_unique_sites,
                      get_stream_rates_dataframe, get_port_rates_dataframe, get_rollup_status)
from .stop_watch import stopwatch

log = get_logger(__name__, logging.INFO)
here = os.path.dirname(__file__)


class BaseStatusMonitor(object):
    def __init__(self, engine):
        self.engine = engine
        self.session_factory = sessionmaker(bind=engine, autocommit=True)
        self.session = self.session_factory()
        self._last_count = {}
        self._refdes_cache = {}
        self._expected_cache = {}
        self._deployed_cache = {}
        self.config = Config(here)

    def check_all(self):
        raise NotImplemented

    def _get_or_create_refdes(self, reference_designator):
        if reference_designator not in self._refdes_cache:
            refdes = ReferenceDesignator.get_or_create(self.session, reference_designator)
            self._refdes_cache[reference_designator] = refdes
        return self._refdes_cache[reference_designator]

    def _get_or_create_expected(self, stream, method):
        if (stream, method) not in self._expected_cache:
            expected = ExpectedStream.get_or_create(self.session, stream, method)
            self._expected_cache[(stream, method)] = expected
        return self._expected_cache[(stream, method)]

    def _get_or_create_stream(self, refdes, stream, method, count, timestamp, coll_time):
        refdes_obj = self._get_or_create_refdes(refdes)
        expected_obj = self._get_or_create_expected(stream, method)
        if (refdes, expected_obj) not in self._deployed_cache:
            deployed = DeployedStream.get_or_create(self.session, refdes_obj, expected_obj, count, timestamp, coll_time)
            self._deployed_cache[(refdes, expected_obj)] = deployed
        return self._deployed_cache[(refdes, expected_obj)]

    @stopwatch()
    def read_expected_csv(self, filename):
        """ Populate expected stream definitions from definition in CSV-formatted file"""
        df = pd.read_csv(filename)
        fields = ['stream', 'method', 'expected rate (Hz)', 'timeout']
        with self.session.begin():
            for stream, method, rate, timeout in df[fields].itertuples(index=False):
                es = self.session.query(ExpectedStream).filter(and_(ExpectedStream.name == stream,
                                                                    ExpectedStream.method == method)).first()
                if es is None:
                    es = ExpectedStream(name=stream, method=method)
                es.expected_rate = rate
                es.warn_interval = timeout * 2
                es.fail_interval = timeout * 10
                self.session.add(es)

    @stopwatch()
    def _check_status(self, rows):
        now = datetime.datetime.utcnow()

        with self.session.begin():
            for stream_metadata, elapsed, uid in rows:
                deployed = self._get_or_create_stream(stream_metadata.refdes, stream_metadata.stream,
                                                      stream_metadata.method, stream_metadata.count,
                                                      stream_metadata.last, now)

                condition = deployed.stream_condition
                status, interval = deployed.get_status(elapsed)

                if status is None:
                    continue

                if condition is None:
                    now = datetime.datetime.utcnow()
                    previous_status = 'not tracked'
                    condition = StreamCondition(deployed_stream=deployed, last_status=status,
                                                last_status_time=now)
                    self.session.add(condition)
                else:
                    previous_status = condition.last_status
                    condition.last_status = status
                    condition.last_status_time = now

                if previous_status != status:
                    yield StatusMessage(stream_metadata.refdes,
                                        stream_metadata.stream,
                                        uid,
                                        elapsed,
                                        previous_status,
                                        status,
                                        interval)

    def _add_rollup_status(self, status_generator):
        status_dict = {}
        for each in status_generator:
            rollup_status = status_dict.get(each.refdes)
            if rollup_status is None:
                status_dict[each.refdes] = rollup_status = get_rollup_status(self.session, each.refdes)
            each.instrument_status = rollup_status
            yield each

    def resample_count_data_hourly(self):
        window_start = self.config.get('RESAMPLE_WINDOW_START_HOURS')
        window_end = self.config.get('RESAMPLE_WINDOW_END_HOURS')
        # get a datetime object representing this HOUR
        now = datetime.datetime.utcnow().replace(second=0, minute=0)
        window_start_dt = now - datetime.timedelta(hours=window_start)
        window_end_dt = now - datetime.timedelta(hours=window_end)
        session = self.session_factory()

        for deployed_stream in self.session.query(DeployedStream):
            log.info('Resampling %s', deployed_stream)
            # resample all count data from now-48 to now-24 to 1 hour
            with session.begin():
                counts_df = get_stream_rates_dataframe(session, deployed_stream.id, window_end_dt, window_start_dt)
                resample_stream_count(session, deployed_stream.id, counts_df, 3600)

        for reference_designator in self.session.query(ReferenceDesignator):
            log.info('Resampling %s', reference_designator)
            with session.begin():
                counts_df = get_port_rates_dataframe(session, reference_designator.id, window_end_dt, window_start_dt)
                resample_port_count(session, reference_designator.id, counts_df, 3600)

    @staticmethod
    def get_notify_list(session, email_type):
        query = session.query(NotifyAddress).filter(NotifyAddress.email_type == email_type)
        return [na.email_addr for na in query]

    def daily_digest(self):
        session = self.session_factory()
        subject_format = self.config.get('DIGEST_SUBJECT')
        root_url = self.config.get('URL_ROOT')
        www_root = self.config.get('WWW_ROOT')
        date = datetime.date.today()
        notify_list = self.get_notify_list(session, 'digest')

        with session.begin():
            create_daily_digest_plots(session, www_root=www_root)
            notifier = self.get_email_notifier()
            for site in get_unique_sites(session):
                html = create_daily_digest_html(session, site=site, root_url=root_url)
                notify_subject = subject_format % (site, date)
                notifier.send_html(notify_list, notify_subject, html)

    def get_email_notifier(self):
        smtp_user = self.config.get('SMTP_USER')
        smtp_passwd = self.config.get('SMTP_PASS')
        smtp_host = self.config.get('SMTP_HOST')
        from_addr = self.config.get('EMAIL_FROM')
        root_url = self.config.get('URL_ROOT')
        return EmailNotifier(smtp_host, from_addr, root_url, smtp_user, smtp_passwd)


class PostgresStatusMonitor(BaseStatusMonitor):
    def __init__(self, engine, metadata_engine):
        super(PostgresStatusMonitor, self).__init__(engine)
        self.metadata_engine = metadata_engine
        self.metadata_session_factory = sessionmaker(bind=metadata_engine, autocommit=True)
        self.metadata_session = self.metadata_session_factory()

    def check_all(self):
        active = get_active_streams(self.metadata_session)
        changed = self._check_status(active)
        rolled = self._add_rollup_status(changed)
        log.info(list(rolled))


@click.command()
def main():
    engine = create_engine('postgresql+psycopg2://monitor@/monitor')
    create_database(engine)

    metadata_engine = create_engine('postgresql+psycopg2://awips@/metadata')
    monitor = PostgresStatusMonitor(engine, metadata_engine)

    monitor.config.from_object('ooi_status.default_settings')
    if 'OOISTATUS_SETTINGS' in os.environ:
        monitor.config.from_envvar('OOISTATUS_SETTINGS')

    for key in monitor.config:
        log.info('OOI_STATUS CONFIG: %r: %r', key, monitor.config[key])

    amqp_url = monitor.config.get('AMQP_URL')
    amqp_queue = monitor.config.get('AMQP_QUEUE')
    if amqp_url and amqp_queue:
        # start the asynchronous AMQP listener
        amqp_client = AmqpStatsClient(amqp_url, amqp_queue, engine)
        amqp_client.start_thread()

    scheduler = BlockingScheduler()
    log.info('adding jobs')

    # notify on change every minute
    scheduler.add_job(monitor.check_all, 'cron', second=0)
    log.info('starting jobs')
    scheduler.start()


if __name__ == '__main__':
    main()
