import os
import unittest
import mock

import time

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from model.rsn_status_model import createDB, ExpectedStream, ReferenceDesignator, DeployedStream, Base
from rsn_status_monitor import RSNStatusMonitor

test_dir = os.path.dirname(__file__)
streamed = pd.read_csv(os.path.join(test_dir, 'data', 'ooi-status.csv'))


def mock_query_cassandra(self):
    out = [x[2:9] for x in streamed.itertuples()]
    streamed['count'] += 10
    streamed['last'] += 10
    return out


class CassandraTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.refdes = 'RS10ENGC-XX00X-00-PRESTA001'
        cls.stream = 'prest_real_time'
        cls.method = 'streamed'
        engine = create_engine('postgresql+psycopg2://monitor@localhost/monitor')
        Base.metadata.drop_all(bind=engine)
        createDB(engine)
        monitor = RSNStatusMonitor('0', '0')
        monitor.create_counts(mock_query_cassandra(None))
        return monitor

    def test_create_monitor(self):
        pass

    def test_query_cassandra(self):
        session = self.Session()
        with session.begin():
            ds = session.query(DeployedStream).get(1)  # TODO - get based on stream name
            monitor = RSNStatusMonitor()

            # first check to make sure that the cassandra query is working on an existing stream
            count, timestamp = monitor._query_cassandra(ds)
            print 'cassandra query - {0}: {1}'.format(count, timestamp)
            assert count > 500000

    def test_it(self):
        session = self.Session()
        with session.begin():
            ds = session.query(DeployedStream).get(1)  # TODO - get based on stream name
            monitor = RSNStatusMonitor()
            self.assertEqual(monitor.status(ds), 'FAILURE')

    def test_counts(self):
        session = self.Session()
        with session.begin():
            ds = session.query(DeployedStream).get(1)  # TODO - get based on stream name
            monitor = RSNStatusMonitor('0', '0')
            # now check to see if the create window is working properly. Setup a base, then check after the warn and
            # fail intervals
            base_window = monitor.create_window(ds)
            print 'base count: {0}'.format(base_window.particle_count)
            time.sleep(5.1)
            next_window = monitor.create_window(ds)
            count = next_window.particle_count
            print 'next count: {0}'.format(count)
            # assert next_window.particle_count == 5

            count, timestamp = monitor._query_cassandra(ds)
            print 'cassandra query - {0}: {1}'.format(count, timestamp)

            # sleep(ds.expected_stream.warn_interval)
            # warn_window = monitor.create_window(ds)
            # assert_

            # sleep(ds.expected_stream.fail_interval)
            # fail_window = monitor.create_window(ds)

            assert False


@mock.patch('rsn_status_monitor.RSNStatusMonitor._query_cassandra', new=mock_query_cassandra)
class DBTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.refdes = 'RS10ENGC-XX0XX-XX-PRESTA001'
        cls.stream = 'prest_real_time'
        cls.method = 'streamed'
        engine = create_engine('postgresql+psycopg2://monitor@localhost/monitor')
        Base.metadata.drop_all(bind=engine)
        createDB(engine)
        cls.Session = sessionmaker(bind=engine, autocommit=True)
        session = cls.Session()

        with session.begin():
            se = ExpectedStream(name=cls.stream, fail_interval=10, warn_interval=60, method=cls.method)
            rf = ReferenceDesignator(name=cls.refdes)
            dp = DeployedStream(expected_stream=se, ref_des=rf)
            session.add(dp)

    def test_query_postgres(self):
        session = self.Session()
        with session.begin():
            ds = session.query(DeployedStream).get(1)
            rsm = RSNStatusMonitor()
            print rsm._query_postgres(ds)

    def test_query_cassandra(self):
        session = self.Session()
        with session.begin():
            ds = session.query(DeployedStream).get(1)
            rsm = RSNStatusMonitor()
            rsm._query_cassandra(ds)
            rsm._query_cassandra(ds)
            rsm._query_cassandra(ds)
            count, timestamp = rsm._query_cassandra(ds)
            assert count == 40
            assert timestamp == 4

    def test_create_window(self):
        session = self.Session()
        with session.begin():
            ds = session.query(DeployedStream).get(1)
            rsm = RSNStatusMonitor()
            data_window = rsm.create_window(ds)
            assert data_window.particle_count == 10
            assert data_window.timestamp == 1

    def test_db(self):
        pass
