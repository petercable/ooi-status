from datetime import timedelta

import pandas as pd
from sqlalchemy import func
from sqlalchemy.sql.elements import and_

from ooi_status.model.status_model import ExpectedStream, DeployedStream, StreamCount

RATE_ACCEPTABLE_DEVIATION = 0.2


def build_counts_subquery(session, timestamp, stream_id=None):
    subquery = session.query(StreamCount.stream_id,
                             func.sum(StreamCount.particle_count).label('particle_count'),
                             func.sum(StreamCount.seconds).label('seconds'))
    subquery = subquery.group_by(StreamCount.stream_id).filter(StreamCount.collected_time > timestamp)
    if stream_id:
        subquery = subquery.filter(StreamCount.stream_id == stream_id)
    return subquery.subquery()


def get_status_query(session, base_time, filter_refdes=None, filter_method=None, filter_stream=None, stream_id=None):
    filter_constraints = []
    if filter_refdes:
        filter_constraints.append(DeployedStream.reference_designator.like('%%%s%%' % filter_refdes))
    if filter_method:
        filter_constraints.append(ExpectedStream.method.like('%%%s%%' % filter_method))
    if filter_stream:
        filter_constraints.append(ExpectedStream.name.like('%%%s%%' % filter_stream))

    five_ago = base_time - timedelta(minutes=5)
    hour_ago = base_time - timedelta(hours=1)
    day_ago = base_time - timedelta(days=1)
    week_ago = base_time - timedelta(days=7)

    five_min_subquery = build_counts_subquery(session, five_ago, stream_id)
    one_hour_subquery = build_counts_subquery(session, hour_ago, stream_id)
    one_day_subquery = build_counts_subquery(session, day_ago, stream_id)
    one_week_subquery = build_counts_subquery(session, week_ago, stream_id)

    # Overall query, joins the three above subqueries with the DeployedStream table to produce our data
    query = session.query(DeployedStream,
                          five_min_subquery.c.seconds,
                          five_min_subquery.c.particle_count,
                          one_hour_subquery.c.seconds,
                          one_hour_subquery.c.particle_count,
                          one_day_subquery.c.seconds,
                          one_day_subquery.c.particle_count,
                          one_week_subquery.c.seconds,
                          one_week_subquery.c.particle_count
                          ).join(five_min_subquery)
    # all subqueries but the current time are OUTER JOIN so that we will still generate a row
    # even if no data exists
    for subquery in [one_hour_subquery, one_day_subquery, one_week_subquery]:
        query = query.outerjoin(subquery)

    # Apply any filter constraints if supplied
    if filter_constraints:
        query = query.join(ExpectedStream)
        query = query.filter(and_(*filter_constraints))

    return query


def resample(session, stream_id, start_time, end_time, seconds):
    # fetch all count data in our window
    query = session.query(StreamCount).filter(and_(StreamCount.stream_id == stream_id,
                                                   StreamCount.collected_time >= start_time,
                                                   StreamCount.collected_time < end_time))
    counts_df = pd.read_sql_query(query.statement, query.session.bind, index_col='collected_time')
    # resample to our new interval
    resampled = counts_df.resample('%dS' % seconds).sum()
    # drop the old records
    session.query(StreamCount).filter(StreamCount.id.in_(list(counts_df.id.values))).delete(synchronize_session=False)
    # insert the new records
    for collected_time, _, _, particle_count, seconds in resampled.itertuples():
        sc = StreamCount(stream_id=stream_id, collected_time=collected_time,
                         particle_count=particle_count, seconds=seconds)
        session.add(sc)
    return resampled


def get_hourly_rates(session, stream_id):
    query = session.query(StreamCount).filter(StreamCount.stream_id == stream_id)
    counts_df = pd.read_sql_query(query.statement, query.session.bind, index_col='collected_time')
    counts_df = counts_df.resample('1H').mean()
    counts_df['rate'] = counts_df.particle_count / counts_df.seconds
    return counts_df


def compute_rate(current_count, current_timestamp, previous_count, previous_timestamp):
    if not all([current_count, current_timestamp, previous_count, previous_timestamp]):
        return 0
    elapsed = (current_timestamp - previous_timestamp).total_seconds()
    if elapsed == 0:
        return 0
    return 1.0 * (current_count - previous_count) / elapsed


def create_status_dict(row, base_time):
    if row:
        (deployed_stream, five_min_time, five_min_count, one_hour_time, one_hour_count,
         one_day_time, one_day_count, one_week_time, one_week_count) = row

        row_dict = deployed_stream.asdict()

        five_min_rate = five_min_count / five_min_time
        one_hour_rate = one_hour_count / one_hour_time
        one_day_rate = one_day_count / one_day_time
        one_week_rate = one_week_count / one_week_time

        counts = {
            'current': deployed_stream.particle_count,
            'five_mins': five_min_count,
            'one_hour': one_hour_count,
            'one_day': one_day_count,
            'one_week': one_week_count
        }

        rates = {
            'five_mins': five_min_rate,
            'one_hour': one_hour_rate,
            'one_day': one_day_rate,
            'one_week': one_week_rate
        }

        row_dict['last_seen'] = deployed_stream.last_seen
        row_dict['counts'] = counts
        row_dict['rates'] = rates

        if deployed_stream.last_seen:
            elapsed = base_time - deployed_stream.last_seen
            elasped_seconds = elapsed.total_seconds()
            row_dict['elapsed'] = str(elapsed)
        else:
            elasped_seconds = 999999999
        row_dict['elapsed_seconds'] = elasped_seconds

        es = deployed_stream.expected_stream
        expected_rate = es.expected_rate if deployed_stream.expected_rate is None else deployed_stream.expected_rate
        warn_interval = es.warn_interval if deployed_stream.warn_interval is None else deployed_stream.warn_interval
        fail_interval = es.fail_interval if deployed_stream.fail_interval is None else deployed_stream.fail_interval

        if expected_rate > 0:
            five_min_percent = 1 - RATE_ACCEPTABLE_DEVIATION
            one_day_percent = 1 - (RATE_ACCEPTABLE_DEVIATION / (86400 / 300.0))
            five_min_thresh = expected_rate * five_min_percent
            one_day_thresh = expected_rate * one_day_percent
            thresh = {'one_day_thresh': one_day_thresh, 'five_min_thresh': five_min_thresh}
            row_dict['rate_thresholds'] = thresh

        status = 'OPERATIONAL'
        if not any([expected_rate, fail_interval, warn_interval]):
            status = 'NOSTATUS'
        elif fail_interval > 0:
            if elasped_seconds > fail_interval * 700:
                status = 'DEAD'
            elif elasped_seconds > fail_interval:
                status = 'FAILED'
        elif 0 < warn_interval < elasped_seconds:
            status = 'DEGRADED'
        elif expected_rate > 0 and (five_min_rate < five_min_thresh and one_day_rate < one_day_thresh):
            status = 'DEGRADED'

        row_dict['status'] = status
        return row_dict
