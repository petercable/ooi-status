import os
import unittest

import pandas as pd
from sqlalchemy import create_engine

from model.rsn_status_model import create_database
from rsn_status_monitor import CassStatusMonitor, UframeStatusMonitor
from stop_watch import stopwatch

test_dir = os.path.dirname(__file__)
streamed = pd.read_csv(os.path.join(test_dir, 'data', 'ooi-status.csv'))


def mock_query_cassandra(_):
    out = [x[2:9] for x in streamed.itertuples()]
    streamed['count'] += 10
    streamed['last'] += 10
    return out


class StatusMonitorTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.engine = create_engine('postgresql+psycopg2://monitor@localhost/monitor_test')
        create_database(cls.engine, drop=True)


class CassStatusMonitorTest(StatusMonitorTest):
    def setUp(self):
        self.monitor = CassStatusMonitor(self.engine, None)

    def test_create_many_counts(self):
        with stopwatch('100 rounds:'):
            for _ in xrange(100):
                self.monitor._counts_from_rows(mock_query_cassandra(None))


class UframeStatusMonitorTest(StatusMonitorTest):
    def setUp(self):
        self.monitor = UframeStatusMonitor(self.engine, 'uft21.ooi.rutgers.edu')
        self.refdes = 'RS10ENGC-XX00X-00-PRESTA001'
        self.stream = 'prest_real_time'
        self.method = 'streamed'
        with self.monitor.session.begin():
            self.ds = self.monitor._get_or_create_stream(self.refdes, self.stream, self.method)

    def test_query_api(self):
        count, timestamp = self.monitor._query_api(self.ds)
        self.assertNotEqual(count, 0)
        self.assertNotEqual(timestamp, 0)

    def test_create_counts(self):
        counts_obj = self.monitor._create_counts(self.ds)
        self.assertEqual(self.refdes, counts_obj.stream.ref_des.name)
        self.assertEqual(self.stream, counts_obj.stream.expected_stream.name)
        self.assertEqual(self.method, counts_obj.stream.expected_stream.method)
        self.assertNotEqual(counts_obj.particle_count, 0)
        self.assertNotEqual(counts_obj.timestamp, 0)
