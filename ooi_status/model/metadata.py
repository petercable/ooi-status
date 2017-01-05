# coding: utf-8
from sqlalchemy import (BigInteger, Column, Integer, String,
                        UniqueConstraint, DateTime, ForeignKey, Sequence)
from sqlalchemy.orm import relationship

from .base import Base, NtpSecsTimestamp

metadata = Base.metadata


class PartitionMetadata(Base):
    __tablename__ = 'partition_metadata'
    __table_args__ = (
        UniqueConstraint('subsite', 'node', 'sensor', 'method', 'stream', 'bin', 'store'),
    )
    id = Column(Integer, Sequence('partition_metadata_seq'), primary_key=True)
    subsite = Column(String(16), nullable=False)
    node = Column(String(16), nullable=False)
    sensor = Column(String(16), nullable=False)
    method = Column(String(255), nullable=False)
    stream = Column(String(255), nullable=False)
    store = Column(String(255), nullable=False)
    bin = Column(BigInteger, nullable=False)
    count = Column(BigInteger, nullable=False)
    first = Column(NtpSecsTimestamp, nullable=False)
    last = Column(NtpSecsTimestamp, nullable=False)
    modified = Column(DateTime)

    def __repr__(self):
        return str({'id': self.id, 'bin': self.bin, 'count': self.count, 'first': self.first, 'last': self.last,
                    'method': self.method, 'node': self.node, 'sensor': self.sensor, 'subsite': self.subsite,
                    'store': self.store, 'stream': self.stream, 'modified': self.modified})


class StreamMetadata(Base):
    __tablename__ = 'stream_metadata'
    __table_args__ = (
        UniqueConstraint('subsite', 'node', 'sensor', 'method', 'stream'),
    )

    id = Column(Integer, Sequence('stream_metadata_seq'), primary_key=True)
    count = Column(BigInteger, nullable=False)
    first = Column(NtpSecsTimestamp, nullable=False)
    last = Column(NtpSecsTimestamp, nullable=False)
    method = Column(String(255), nullable=False)
    node = Column(String(16), nullable=False)
    sensor = Column(String(16), nullable=False)
    subsite = Column(String(16), nullable=False)
    stream = Column(String(255), nullable=False)

    @property
    def refdes(self):
        return '-'.join((self.subsite, self.node, self.sensor))

    def __repr__(self):
        return str({'id': self.id, 'count': self.count, 'first': self.first, 'last': self.last,
                    'method': self.method, 'node': self.node, 'sensor': self.sensor, 'subsite': self.subsite,
                    'stream': self.stream})


class ProcessedMetadata(Base):
    __tablename__ = 'processed_metadata'
    __table_args__ = (
        UniqueConstraint('processor_name', 'partition_id'),
    )
    id = Column(Integer, primary_key=True)
    processor_name = Column(String, nullable=False)
    processed_time = Column(DateTime, nullable=False)
    partition_id = Column(Integer, ForeignKey('partition_metadata.id', ondelete='CASCADE'))
    partition = relationship(PartitionMetadata)
