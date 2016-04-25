import logging
import os
import unittest

import datetime
import pandas as pd
from sqlalchemy.sql.elements import and_

from ooi_status.get_logger import get_logger
from sqlalchemy import create_engine

from ooi_status.model.status_model import create_database, ExpectedStream, DeployedStream
from ooi_status.status_monitor import CassStatusMonitor, UframeStatusMonitor
from ooi_status.stop_watch import stopwatch

log = get_logger(__name__, level=logging.INFO)

test_dir = os.path.dirname(__file__)
streamed = pd.read_csv(os.path.join(test_dir, 'data', 'ooi-status.csv'))
streamed['count'] = streamed['count'].astype('int')


def mock_query_cassandra(_):
    fields = ['subsite', 'node', 'sensor', 'method', 'stream', 'count', 'last']
    out = list(streamed[fields].itertuples(index=False))
    mask = streamed['method'] == 'streamed'
    streamed['count'].values[mask.values] += 60
    streamed['last'].values[mask.values] += 60
    return out[:100]


class StatusMonitorTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.engine = create_engine('postgresql+psycopg2://monitor@localhost/monitor_test')
        create_database(cls.engine, drop=True)


class CassStatusMonitorTest(StatusMonitorTest):
    @classmethod
    def setUpClass(cls):
        StatusMonitorTest.setUpClass()
        cls.monitor = CassStatusMonitor(cls.engine, None)
        cls.monitor.read_expected_csv(os.path.join(test_dir, 'data', 'expected-rates.csv'))

        now = datetime.datetime.utcnow()
        cls.monitor._counts_from_rows(mock_query_cassandra(None), now)
        with stopwatch('6 rounds'):
            for i in xrange(6):
                t = now + datetime.timedelta(seconds=(i+1) * 60)
                cls.monitor._counts_from_rows(mock_query_cassandra(None), t)

    def resolve_deployed_stream(self, name, method):
        return self.monitor.session.query(DeployedStream) \
            .join(ExpectedStream).filter(and_(ExpectedStream.name == name,
                                              ExpectedStream.method == method)).first()

    def test_read_expected(self):
        """ test read from file - depends on test_create_many_counts running first """
        with self.monitor.session.begin():
            self.assertEqual(self.monitor.session.query(ExpectedStream).count(), 366)

    def test_failed_stream(self):
        # telemetered streams are not getting updated, so they will be FAILED (if tracked)
        ds = self.resolve_deployed_stream('parad_k__stc_imodem_instrument', 'telemetered')
        self.assertIsNotNone(ds)
        self.assertEqual('FAILED', ds.status)

    def test_untracked_stream(self):
        # untracked stream, status is None
        ds = self.resolve_deployed_stream('cg_dcl_eng_dcl_gps_recovered', 'recovered_host')
        self.assertIsNotNone(ds)
        self.assertIsNone(ds.status)

    def test_degraded_stream(self):
        # BOTPT rate is 20 Hz so it should be partial for our test (which updates at 1 Hz)
        ds = self.resolve_deployed_stream('botpt_nano_sample', 'streamed')
        self.assertIsNotNone(ds)
        self.assertEqual('DEGRADED', ds.status)

    def test_operational_stream(self):
        # FLORT rate is 0.89 Hz so it should be operational for our test (which updates at 1 Hz)
        ds = self.resolve_deployed_stream('flort_d_data_record', 'streamed')
        self.assertIsNotNone(ds)
        self.assertEqual('OPERATIONAL', ds.status)


class UframeStatusMonitorTest(StatusMonitorTest):
    @classmethod
    def setUpClass(cls):
        StatusMonitorTest.setUpClass()
        cls.monitor = UframeStatusMonitor(cls.engine, 'uft21.ooi.rutgers.edu')
        cls.monitor.read_expected_csv(os.path.join(test_dir, 'data', 'expected-rates.csv'))
        cls.monitor.gather_all()

    def test_none(self):
        pass
