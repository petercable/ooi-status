import logging
from datetime import timedelta, datetime

import pandas as pd
from sqlalchemy.sql.elements import and_

from ooi_status.status_message import StatusEnum
from .get_logger import get_logger
from .model.status_model import (ExpectedStream, DeployedStream, StreamCondition, PortCount, ReferenceDesignator)

log = get_logger(__name__, logging.INFO)


def get_status_query(session, filter_refdes=None, filter_method=None, filter_status=None, filter_stream=None):
    query = session.query(StreamCondition).join(DeployedStream, ExpectedStream, ReferenceDesignator)

    filter_constraints = []
    if filter_refdes:
        filter_constraints.append(ReferenceDesignator.name.like('%%%s%%' % filter_refdes))
    if filter_method:
        filter_constraints.append(ExpectedStream.method.like('%%%s%%' % filter_method))
    if filter_stream:
        filter_constraints.append(ExpectedStream.name.like('%%%s%%' % filter_stream))
    if filter_status:
        filter_constraints.append(StreamCondition.last_status.like('%%%s%%' % filter_status))

    query = query.filter(*filter_constraints)
    log.info('query: %s %s', query, filter_constraints)
    return query


def resample_port_count(session, refdes_id, counts_df, seconds):
    fields = ['byte_count', 'seconds']
    if not counts_df.empty:
        resampled = counts_df.resample('%dS' % seconds).sum()[fields]
        # drop the old records
        session.query(PortCount).filter(PortCount.id.in_(list(counts_df.id.values))).delete(
            synchronize_session=False)
        # insert the new records
        for collected_time, byte_count, seconds in resampled.itertuples():
            sc = PortCount(reference_designator_id=refdes_id, collected_time=collected_time,
                           byte_count=byte_count, seconds=seconds)
            session.add(sc)
        return resampled


def get_port_data_rates(session, refdes_id):
    counts_df = get_port_rates_dataframe(session, refdes_id, None, None)
    if not counts_df.empty:
        counts_df = counts_df.resample('1H').mean()
        counts_df['rate'] = counts_df.byte_count / counts_df.seconds
    return counts_df


def get_status_by_instrument(session, filter_refdes=None, filter_method=None, filter_stream=None, filter_status=None):
    query = get_status_query(session,
                             filter_refdes=filter_refdes,
                             filter_method=filter_method,
                             filter_stream=filter_stream,
                             filter_status=filter_status)

    # group by reference designator
    grouped = {}
    for sc in query:
        refdes = sc.deployed_stream.reference_designator.name
        grouped.setdefault(refdes, []).append(sc)

    out = {}
    # create a rollup status
    for refdes in grouped:
        conditions = grouped[refdes]
        overall = _rollup_statuses(set(c.last_status for c in conditions))
        out[refdes] = {
            'overall': overall,
            'status': [c.as_dict() for c in conditions]
        }

    return out


def get_status_by_refdes_id(session, refdes_id):
    query = session.query(StreamCondition).join(DeployedStream,
                                                ReferenceDesignator).filter(ReferenceDesignator.id == refdes_id)
    conditions = list(query)
    overall = _rollup_statuses(set(c.last_status for c in conditions))
    return {
        'overall': overall,
        'status': [c.as_dict() for c in conditions]}


def get_status_by_stream(session, filter_refdes=None, filter_method=None, filter_stream=None, filter_status=None):
    return {
        'status': [status.as_dict() for status in get_status_query(session,
                                                                   filter_refdes=filter_refdes,
                                                                   filter_method=filter_method,
                                                                   filter_stream=filter_stream,
                                                                   filter_status=filter_status)]}


def get_status_by_stream_id(session, deployed_id):
    return session.query(StreamCondition).filter(StreamCondition.stream_id == deployed_id).first()


#### RATES ####

def get_port_rates_dataframe(session, refdes_id, start, end):
    now = datetime.utcnow()
    if start is None:
        start = now - timedelta(days=1)
    if end is None:
        end = now
    query = session.query(PortCount).filter(and_(PortCount.reference_designator_id == refdes_id,
                                                 PortCount.collected_time >= start,
                                                 PortCount.collected_time < end)).order_by(PortCount.collected_time)
    counts_df = pd.read_sql_query(query.statement, query.session.bind, index_col='collected_time')
    counts_df['rate'] = counts_df.byte_count / counts_df.seconds
    return counts_df


def _rollup_statuses(statuses):
    if StatusEnum.FAILED in statuses:
        return StatusEnum.FAILED
    elif StatusEnum.DEGRADED in statuses:
        return StatusEnum.DEGRADED
    elif StatusEnum.OPERATIONAL in statuses:
        return StatusEnum.OPERATIONAL
    return StatusEnum.NOT_TRACKED


def _rollup_status_query(query):
    statuses = set((status[0] for status in query))
    return _rollup_statuses(statuses)


def get_rollup_status_by_id(session, refdes_id):
    query = session.query(StreamCondition.last_status).join(DeployedStream, ReferenceDesignator)
    query = query.filter(ReferenceDesignator.id == refdes_id)
    return _rollup_status_query(query)


def get_rollup_status(session, refdes):
    query = session.query(StreamCondition.last_status).join(DeployedStream, ReferenceDesignator)
    query = query.filter(ReferenceDesignator.name == refdes)
    return _rollup_status_query(query)
