from sqlalchemy import Integer, Column, String, Text

from airflow.models import TaskInstance
from airflow.models.base import Base
from airflow.utils.db import provide_session
from airflow.utils.state import State


class Pool(Base):
    __tablename__ = "slot_pool"

    id = Column(Integer, primary_key=True)
    pool = Column(String(50), unique=True)
    slots = Column(Integer, default=0)
    description = Column(Text)

    def __repr__(self):
        return self.pool

    def to_json(self):
        return {
            'id': self.id,
            'pool': self.pool,
            'slots': self.slots,
            'description': self.description,
        }

    @provide_session
    def used_slots(self, session):
        """
        Returns the number of slots used at the moment
        """
        running = (
            session
            .query(TaskInstance)
            .filter(TaskInstance.pool == self.pool)
            .filter(TaskInstance.state == State.RUNNING)
            .count()
        )
        return running

    @provide_session
    def queued_slots(self, session):
        """
        Returns the number of slots used at the moment
        """
        return (
            session
            .query(TaskInstance)
            .filter(TaskInstance.pool == self.pool)
            .filter(TaskInstance.state == State.QUEUED)
            .count()
        )

    @provide_session
    def open_slots(self, session):
        """
        Returns the number of slots open at the moment
        """
        used_slots = self.used_slots(session=session)
        queued_slots = self.queued_slots(session=session)
        return self.slots - used_slots - queued_slots
