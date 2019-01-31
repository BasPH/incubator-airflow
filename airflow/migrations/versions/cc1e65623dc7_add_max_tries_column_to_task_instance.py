# flake8: noqa
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

"""add max tries column to task instance

Revision ID: cc1e65623dc7
Revises: 127d2bf2dfa7
Create Date: 2017-06-19 16:53:12.851141

"""
from collections import defaultdict

from alembic import op
import sqlalchemy as sa
from airflow import settings
from airflow.models import DagBag

from sqlalchemy import Column, Integer, String
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.ext.declarative import declarative_base

# revision identifiers, used by Alembic.
revision = 'cc1e65623dc7'
down_revision = '127d2bf2dfa7'
branch_labels = None
depends_on = None

Base = declarative_base()
BATCH_SIZE = 5000
ID_LEN = 250


class TaskInstance(Base):
    __tablename__ = "task_instance"

    task_id = Column(String(ID_LEN), primary_key=True)
    dag_id = Column(String(ID_LEN), primary_key=True)
    execution_date = Column(sa.DateTime, primary_key=True)
    max_tries = Column(Integer)
    try_number = Column(Integer, default=0)


def upgrade():
    op.add_column('task_instance', sa.Column('max_tries', sa.Integer, server_default="-1"))
    # Check if table task_instance exist before data migration. This check is
    # needed for database that does not create table until migration finishes.
    # Checking task_instance table exists prevent the error of querying
    # non-existing task_instance table.
    connection = op.get_bind()
    inspector = Inspector.from_engine(connection)
    tables = inspector.get_table_names()

    if 'task_instance' in tables:
        # Get current session
        sessionmaker = sa.orm.sessionmaker()
        session = sessionmaker(bind=connection)
        dagbag = DagBag(settings.DAGS_FOLDER)
        query = session.query(sa.func.count(TaskInstance.max_tries)).filter(
            TaskInstance.max_tries == -1
        )
        # Separate db query in batch to prevent loading entire table
        # into memory and cause out of memory error.
        while query.scalar():
            tis = session.query(TaskInstance).filter(
                TaskInstance.max_tries == -1
            ).limit(BATCH_SIZE).all()

            # Instead of processing the DAG for every TI, first construct a
            # collection of {dag_id:{task_id:[tis]}} to traverse each DAG only once.
            dagids_tasks = defaultdict(lambda: defaultdict(list))
            for ti in tis:
                dagids_tasks[ti.dag_id][ti.task_id].append(ti)

            for dag_id, tasks in dagids_tasks.items():
                dag = dagbag.get_dag(dag_id)
                for task_id, task_instances in tasks.items():
                    if not dag or not dag.has_task(task_id):
                        # task_instance table might not have the up-to-date
                        # information, i.e DAG or task might be modified or
                        # deleted in DagBag but is reflected in task_instance
                        # table. In this case we do not retry the task that can't
                        # be parsed.
                        for ti in task_instances:
                            ti.max_tries = ti.try_number
                    else:
                        task = dag.get_task(task_id)
                        if task.retries:
                            for ti in task_instances:
                                ti.max_tries = task.retries
                        else:
                            for ti in task_instances:
                                ti.max_tries = ti.try_number

                    for ti in task_instances:
                        session.merge(ti)

            session.commit()
        # Commit the current session.
        session.commit()


def downgrade():
    engine = settings.engine
    if engine.dialect.has_table(engine, 'task_instance'):
        connection = op.get_bind()
        sessionmaker = sa.orm.sessionmaker()
        session = sessionmaker(bind=connection)
        dagbag = DagBag(settings.DAGS_FOLDER)
        query = session.query(sa.func.count(TaskInstance.max_tries)).filter(
            TaskInstance.max_tries != -1
        )
        while query.scalar():
            tis = session.query(TaskInstance).filter(
                TaskInstance.max_tries != -1
            ).limit(BATCH_SIZE).all()

            # Instead of processing the DAG for every TI, first construct a
            # collection of {dag_id:{task_id:[tis]}} to traverse each DAG only once.
            dagids_tasks = defaultdict(lambda: defaultdict(list))
            for ti in tis:
                dagids_tasks[ti.dag_id][ti.task_id].append(ti)

            for dag_id, tasks in dagids_tasks.items():
                dag = dagbag.get_dag(dag_id)
                for task_id, task_instances in tasks.items():
                    if not dag or not dag.has_task(task_id):
                        for ti in task_instances:
                            ti.try_number = 0
                    else:
                        task = dag.get_task(task_id)
                        # max_tries - try_number is number of times a task instance
                        # left to retry by itself. So the current try_number should be
                        # max number of self retry (task.retries) minus number of
                        # times left for task instance to try the task.
                        for ti in task_instances:
                            ti.try_number = max(0, task.retries - (ti.max_tries -
                                                                   ti.try_number))
                    for ti in task_instances:
                        ti.max_tries = -1
                        session.merge(ti)
            session.commit()
        session.commit()
    op.drop_column('task_instance', 'max_tries')
