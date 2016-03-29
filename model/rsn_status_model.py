"""
Track the CI particles being ingested into cassandra and store information into Postgres.

@author Dan Mergens
"""
from sqlalchemy import Column, Integer, String, ForeignKey, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()


class ReferenceDesignator(Base):
    __tablename__ = 'reference_designator'
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)

    def __repr__(self):
        return '{0}'.format(self.name)


class ExpectedStream(Base):
    __tablename__ = 'expected_stream'
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    method = Column(String, nullable=False)
    warn_interval = Column(Integer, default=0)  # 0 means untracked
    fail_interval = Column(Integer, default=0)  # 0 means untracked

    def __repr__(self):
        return '{0} {1} {2} {3}'.format(self.name, self.method, self.warn_interval, self.fail_interval)


class DeployedStream(Base):
    __tablename__ = 'deployed_stream'
    id = Column(Integer, primary_key=True)
    ref_des_id = Column(Integer, ForeignKey('reference_designator.id'), nullable=False)
    ref_des = relationship(ReferenceDesignator)
    expected_stream_id = Column(Integer, ForeignKey('expected_stream.id'), nullable=False)
    expected_stream = relationship(ExpectedStream)

    def __repr__(self):
        return '{0} {1}'.format(self.ref_des, self.expected_stream)


class Counts(Base):
    __tablename__ = 'counts'
    id = Column(Integer, primary_key=True)
    stream_id = Column(Integer, ForeignKey('deployed_stream.id'), nullable=False)
    stream = relationship(DeployedStream)
    particle_count = Column(Integer, nullable=False)
    timestamp = Column(DateTime, nullable=False)

    def rate(self, count):
        return abs((count.particle_count - self.particle_count) / (count.timestamp - self.timestamp).to_seconds())

    def __repr__(self):
        return '{0} {1} particles at {2}'.format(self.stream, self.particle_count, self.timestamp)


def createDB(engine):
    Base.metadata.create_all(bind=engine)
