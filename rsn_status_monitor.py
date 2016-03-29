"""
RSN health and status monitor for data particles received by OOI CI.

@author Dan Mergens
"""
import requests
import click
from apscheduler.schedulers.blocking import BlockingScheduler

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session

import logging
from get_logger import get_logger
from model.rsn_status_model import Counts, createDB, DeployedStream
from stop_watch import StopWatch

log = get_logger(__name__, logging.DEBUG)

EDEX_BASE_URL = 'http://%s:12576/sensor/inv/%s/%s/%s'


def parse_reference_designator(ref_des):
    subsite, node, port, instrument = ref_des.split('-')
    sensor = port + '-' + instrument
    return subsite, node, sensor


class RSNStatusMonitor(object):
    def __init__(self, posthost, hostname):
        self.hostname = hostname
        engine = create_engine('postgresql+psycopg2://monitor@{posthost}'.format(posthost=posthost))
        createDB(engine)
        Session = scoped_session(sessionmaker(bind=engine, autocommit=True))
        self.session = Session()

    def status(self, deployed_stream):
        state = 'FAILURE'
        rate = self.last_rate(deployed_stream)
        warn_interval = deployed_stream.expected_stream.warn_interval
        fail_interval = deployed_stream.expected_stream.fail_interval

        if warn_interval and rate < warn_interval:
            state = 'OPERATIONAL'
        elif fail_interval and rate < fail_interval:
            state = 'PARTIAL'
        return state

    def last_rate(self, deployed_stream):
        with self.session.begin():
            counts = self.session.query(Counts).filter(Counts.stream == deployed_stream).order_by(Counts.timestamp.desc())[:2]
            return counts[0].rate(counts[1])

    def create_window(self, deployed_stream):
        count, timestamp = self._query_cassandra(deployed_stream)
        previous_count = self.session.query(Counts).filter(Counts.stream==deployed_stream).\
            orderby(Counts.timestamp.desc()).first()

        if previous_count.particle_count == count:  # nothing to update
            return previous_count

        counts = Counts(stream=deployed_stream, particle_count=count, timestamp=timestamp)
        self.session.add(counts)
        return counts

    def gather_all(self):
        log.debug('gathering so hard')
        with StopWatch('time to collect all'):
            with self.session.begin():
                for stream in self.session.query(DeployedStream).all():
                    log.info('stream is: %s', stream)
                    self.create_window(stream)

    def create_counts(self, rows):
        for subsite, node, sensor, stream, method, count, timestamp in rows():
            refdes = '-'.join((subsite, node, sensor))

            session.query(ReferenceDesignator).filter(ReferenceDesignator.name==refdes).first()

    def _query_cassandra(self):
        return cassandra.execute('select subsite, node, sensor, stream, method, count, last from stream_metadata')

    def _query_api(self, deployed_stream):
        """
        Get most recent metadata for the stream from cassandra
        :param deployed_stream: deployed stream object from postgres
        :return: (count, timestamp)
        """
        count = 0  # total number of particles for this stream in cassandra
        timestamp = 0  # last timestamp for this stream in cassandra

        subsite, node, sensor = parse_reference_designator(deployed_stream.ref_des.name)
        stream = deployed_stream.expected_stream.name
        method = deployed_stream.expected_stream.method

        # get the latest metadata from cassandra
        url = EDEX_BASE_URL % (self.hostname, subsite, node, sensor) + '/metadata/times'
        response = requests.get(url)
        if response.status_code is not 200:
            print 'failed to get a valid JSON response'
            return count, timestamp

        # find the matching stream name and method in the return
        for dict in response.json():
            if dict['stream'] == stream and dict['method'] == method:
                count = dict['count']
                timestamp = dict['endTime']
                break

        return count, timestamp

    def _query_postgres(self, deployed_stream):
        """
        Get data associated with deployed stream
        :param deployed_stream: deployed stream object from postgres
        :return:
        """
        with self.session.begin():
            return self.session.query(Counts).filter(
                Counts.stream == deployed_stream).order_by(Counts.timestamp.desc()).first()

@click.command()
@click.option('--posthost', default='localhost', help='hostname for Postgres database')
@click.option('--hostname', default='uft21.ooi.rutgers.edu', help='hostname for uFrame sensor API')
def main(hostname, posthost):

    monitor = RSNStatusMonitor(posthost, hostname)
    scheduler = BlockingScheduler()
    log.info('adding job')
    # scheduler.add_job(monitor.gather_all, 'cron', second=0)
    scheduler.add_job(monitor.gather_all, 'interval', seconds=5)
    log.info('starting job')
    scheduler.start()


if __name__ == '__main__':
    main()
