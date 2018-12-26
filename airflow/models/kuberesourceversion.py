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

from sqlalchemy import Boolean, Column, true as sqltrue, String

from airflow.models import Base
from airflow.utils.db import provide_session


class KubeResourceVersion(Base):
    __tablename__ = "kube_resource_version"
    one_row_id = Column(Boolean, server_default=sqltrue(), primary_key=True)
    resource_version = Column(String(255))

    @staticmethod
    @provide_session
    def get_current_resource_version(session=None):
        (resource_version,) = session.query(KubeResourceVersion.resource_version).one()
        return resource_version

    @staticmethod
    @provide_session
    def checkpoint_resource_version(resource_version, session=None):
        if resource_version:
            session.query(KubeResourceVersion).update({
                KubeResourceVersion.resource_version: resource_version
            })
            session.commit()

    @staticmethod
    @provide_session
    def reset_resource_version(session=None):
        session.query(KubeResourceVersion).update({
            KubeResourceVersion.resource_version: '0'
        })
        session.commit()
        return '0'
