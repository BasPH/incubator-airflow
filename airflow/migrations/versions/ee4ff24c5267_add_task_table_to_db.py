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

"""Add Task table to DB

Revision ID: ee4ff24c5267
Revises: 9635ae0956e7
Create Date: 2018-12-07 12:23:11.527460

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.engine.reflection import Inspector

# revision identifiers, used by Alembic.
revision = 'ee4ff24c5267'
down_revision = '9635ae0956e7'
branch_labels = None
depends_on = None


def upgrade():
    conn = op.get_bind()
    inspector = Inspector.from_engine(conn)
    tables = inspector.get_table_names()

    if 'task' not in tables:
        op.create_table(
            'task',
            sa.Column('task_id', sa.String(length=250), nullable=False),
            sa.Column('dag_id', sa.String(length=250), nullable=False),
            sa.Column('pool', sa.String(length=50), nullable=True),
            sa.Column('queue', sa.String(length=50), nullable=True),
            sa.PrimaryKeyConstraint('task_id', 'dag_id')
        )

        op.create_table(
            'task_deps',
            sa.Column('task_id_from', sa.String(length=250), nullable=False),
            sa.Column('task_id_to', sa.String(length=250), nullable=False),
            sa.Column('dag_id', sa.String(length=250), nullable=False),
            sa.PrimaryKeyConstraint('task_id_from', 'task_id_to', 'dag_id')
        )

        op.add_column('dag', sa.Column('last_modified', sa.DateTime(), nullable=True))


def downgrade():
    op.drop_table('task_deps')
    op.drop_table('task')
    op.drop_column('dag', 'last_modified')
