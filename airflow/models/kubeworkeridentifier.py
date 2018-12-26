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

import uuid

from sqlalchemy import Column, Boolean, String, true as sqltrue

from airflow.models import Base
from airflow.utils.db import provide_session


class KubeWorkerIdentifier(Base):
    __tablename__ = "kube_worker_uuid"
    one_row_id = Column(Boolean, server_default=sqltrue(), primary_key=True)
    worker_uuid = Column(String(255))

    @staticmethod
    @provide_session
    def get_or_create_current_kube_worker_uuid(session=None):
        (worker_uuid,) = session.query(KubeWorkerIdentifier.worker_uuid).one()
        if worker_uuid == '':
            worker_uuid = str(uuid.uuid4())
            KubeWorkerIdentifier.checkpoint_kube_worker_uuid(worker_uuid, session)
        return worker_uuid

    @staticmethod
    @provide_session
    def checkpoint_kube_worker_uuid(worker_uuid, session=None):
        if worker_uuid:
            session.query(KubeWorkerIdentifier).update({
                KubeWorkerIdentifier.worker_uuid: worker_uuid
            })
            session.commit()
