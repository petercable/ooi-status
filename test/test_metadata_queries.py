import requests
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import unittest
import time

from ooi_status.event_notifier import EventNotifier
from ooi_status.metadata_queries import get_uid_from_refdes


class MetadataQueryTest(unittest.TestCase):
    def setUp(self):
        metadata_engine = create_engine('postgresql+psycopg2://awips@localhost/metadata')
        self.session = sessionmaker(bind=metadata_engine, autocommit=True)()
        self.host = 'localhost'
        self.port = 12587

    def test_uid_from_refdes(self):
        refdes = 'CE01ISSM-RID16-03-CTDBPC000'
        expected_uid = 'CGINS-CTDBPC-50013'
        uid = get_uid_from_refdes(self.session, refdes)

        self.assertEqual(expected_uid, uid)

    def test_create_event_json(self):
        ev = EventNotifier(self.session, 'localhost')
        uid = 'CGINS-CTDBPC-50013'
        stream = 'ctd_streamed_host_recovered_sio_mule'
        reason = 'the following streams have not reported data: blah blah'
        status = 'DEGRADED'
        expected_status = 'degraded'
        notes = 'more detailed notes?'
        json_data = ev.create_event_json(uid, stream, status, reason, notes)
        self.assertEqual(reason, json_data['reason'])
        self.assertEqual(expected_status, json_data['status'])
        self.assertEqual(notes, json_data['notes'])

    def query_sensor_status_events(self, uid):
        url = 'http://%s:%d/events/uid/%s?type=ASSET_STATUS' % (self.host, self.port, uid)
        r = requests.get(url)
        self.assertEqual(r.status_code, 200)  # OK
        return r

    def test_post_event(self):
        ev = EventNotifier(self.session, 'localhost')
        uid = 'CGINS-CTDBPC-50013'
        json_data = {
            'status': 'degraded', 'eventName': 'Automated status for CGINS-CTDBPC-50013',
            'reason': 'the following streams have not reported data: blah blah',
            'location': 'Status Monitor Service', 'eventType': 'ASSET_STATUS', 'eventStartTime': int(time.time()*1000),
            'notes': 'more detailed notes?', 'assetUid': 'CGINS-CTDBPC-50013', '@class': '.AssetStatusEvent',
            'severity': 0}

        r = ev.post_event(uid, json_data)
        created_id = r.json()['id']
        r = self.query_sensor_status_events(uid)
        # query the available id should match the
        available_ids = []
        for event in r.json():
            available_ids.append(event['eventId'])
        self.assertIn(created_id, available_ids)

