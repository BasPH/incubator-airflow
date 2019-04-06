# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from sqlalchemy import Column, String, Boolean, Integer, Text

from airflow import configuration, settings
from airflow.models.base import Base, ID_LEN
from airflow.models.dagbag import DagBag
from airflow.utils.db import provide_session
from airflow.utils.sqlalchemy import UtcDateTime, Interval


class DagModel(Base):

    __tablename__ = "dag"
    """
    These items are stored in the database for state related information
    """
    dag_id = Column(String(ID_LEN), primary_key=True)
    # A DAG can be paused from the UI / DB
    # Set this default value of is_paused based on a configuration value!
    is_paused_at_creation = configuration.conf\
        .getboolean('core',
                    'dags_are_paused_at_creation')
    is_paused = Column(Boolean, default=is_paused_at_creation)
    # Whether the DAG is a subdag
    is_subdag = Column(Boolean, default=False)
    # Whether that DAG was seen on the last DagBag load
    is_active = Column(Boolean, default=False)
    # Last time the scheduler started
    last_scheduler_run = Column(UtcDateTime)
    # Last time this DAG was pickled
    last_pickled = Column(UtcDateTime)
    # Time when the DAG last received a refresh signal
    # (e.g. the DAG's "refresh" button was clicked in the web UI)
    last_expired = Column(UtcDateTime)
    # Whether (one  of) the scheduler is scheduling this DAG at the moment
    scheduler_lock = Column(Boolean)
    # Foreign key to the latest pickle_id
    pickle_id = Column(Integer)
    # The location of the file containing the DAG object
    fileloc = Column(String(2000))
    # String representing the owners
    owners = Column(String(2000))
    # Description of the dag
    description = Column(Text)
    # Default view of the inside the webserver
    default_view = Column(String(25))
    # Schedule interval
    schedule_interval = Column(Interval)

    def __repr__(self):
        return "<DAG: {self.dag_id}>".format(self=self)

    @property
    def timezone(self):
        return settings.TIMEZONE

    @staticmethod
    @provide_session
    def get_dagmodel(dag_id, session=None):
        return session.query(DagModel).filter(DagModel.dag_id == dag_id).first()

    @classmethod
    @provide_session
    def get_current(cls, dag_id, session=None):
        return session.query(cls).filter(cls.dag_id == dag_id).first()

    def get_default_view(self):
        if self.default_view is None:
            return configuration.conf.get('webserver', 'dag_default_view').lower()
        else:
            return self.default_view

    # @provide_session
    # def get_last_dagrun(self, session=None, include_externally_triggered=False):
    #     return get_last_dagrun(self.dag_id, session=session,
    #                            include_externally_triggered=include_externally_triggered)

    @property
    def safe_dag_id(self):
        return self.dag_id.replace('.', '__dot__')

    def get_dag(self):
        return DagBag(dag_folder=self.fileloc).get_dag(self.dag_id)

    @provide_session
    def create_dagrun(self,
                      run_id,
                      state,
                      execution_date,
                      start_date=None,
                      external_trigger=False,
                      conf=None,
                      session=None):
        """
        Creates a dag run from this dag including the tasks associated with this dag.
        Returns the dag run.

        :param run_id: defines the the run id for this dag run
        :type run_id: str
        :param execution_date: the execution date of this dag run
        :type execution_date: datetime.datetime
        :param state: the state of the dag run
        :type state: airflow.utils.state.State
        :param start_date: the date this dag run should be evaluated
        :type start_date: datetime.datetime
        :param external_trigger: whether this dag run is externally triggered
        :type external_trigger: bool
        :param session: database session
        :type session: sqlalchemy.orm.session.Session
        """

        return self.get_dag().create_dagrun(run_id=run_id,
                                            state=state,
                                            execution_date=execution_date,
                                            start_date=start_date,
                                            external_trigger=external_trigger,
                                            conf=conf,
                                            session=session)
