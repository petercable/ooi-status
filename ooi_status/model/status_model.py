"""
Track the CI particles being ingested into cassandra and store information into Postgres.

@author Dan Mergens
"""
import toolz
from sqlalchemy import Column, Integer, String, Float, ForeignKey, DateTime, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()


class ExpectedStream(Base):
    __tablename__ = 'expected_stream'
    __table_args__ = (
        UniqueConstraint('name', 'method'),
    )
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    method = Column(String, nullable=False)
    expected_rate = Column(Float)
    warn_interval = Column(Integer)
    fail_interval = Column(Integer)

    def asdict(self):
        fields = ['id', 'name', 'method', 'expected_rate', 'warn_interval', 'fail_interval']
        return {field: getattr(self, field) for field in fields}

    def __repr__(self):
        return '{0} {1} {2} Hz {3}/{4}'.format(self.name, self.method, self.expected_rate, self.warn_interval, self.fail_interval)


class DeployedStream(Base):
    __tablename__ = 'deployed_stream'
    __table_args__ = (
        UniqueConstraint('reference_designator', 'expected_stream_id'),
    )
    id = Column(Integer, primary_key=True)
    reference_designator = Column(String, nullable=False)
    expected_stream_id = Column(Integer, ForeignKey('expected_stream.id'), nullable=False)
    particle_count = Column(Integer, nullable=False)
    last_seen = Column(DateTime, nullable=False)
    collected = Column(DateTime, nullable=False)
    expected_rate = Column(Float)
    warn_interval = Column(Integer)
    fail_interval = Column(Integer)
    expected_stream = relationship(ExpectedStream, backref='deployed_streams', lazy='joined')

    def asdict(self):
        fields = ['id', 'reference_designator', 'expected_stream', 'expected_rate', 'warn_interval', 'fail_interval']
        return {field: getattr(self, field) for field in fields}

    def __repr__(self):
        return '{0} {1} {2} {3}'.format(self.reference_designator, self.expected_stream, self.collected, self.particle_count)

    @property
    def status(self):
        expected_rate = self.expected_rate if self.expected_rate else self.expected_stream.expected_rate

        if expected_rate:
            counts = self.stream_counts[-10:]
            elapsed = sum((x.seconds for x in counts))
            total = sum((x.particle_count for x in counts))
            if elapsed:
                rate = total / elapsed
            else:
                rate = 0

            if rate == 0:
                return 'FAILED'
            elif rate < expected_rate:
                return 'DEGRADED'
            else:
                return 'OPERATIONAL'


class StreamCount(Base):
    __tablename__ = 'stream_count'
    id = Column(Integer, primary_key=True)
    stream_id = Column(Integer, ForeignKey('deployed_stream.id'), nullable=False)
    collected_time = Column(DateTime, nullable=False)
    particle_count = Column(Integer, default=0)
    seconds = Column(Float, default=0)
    stream = relationship(DeployedStream, backref='stream_counts')

    def __repr__(self):
        return 'StreamCount(id=%d, count=%d, seconds=%f, rate=%f)' % (self.id, self.particle_count, self.seconds, self.particle_count / self.seconds)


def create_database(engine, drop=False):
    if drop:
        Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)
