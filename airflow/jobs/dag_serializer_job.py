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

import os
import signal
import sys

from airflow import settings
from airflow.jobs.base_job import BaseJob
from airflow.scheduler.dag_serialization import AirflowDagsWatcher


class DagSerializerJob(BaseJob):

    __mapper_args__ = {"polymorphic_identity": "DagSerializerJob"}

    def __init__(self, dags_folder=settings.DAGS_FOLDER, *args, **kwargs):
        self._dags_folder = dags_folder
        super().__init__(*args, **kwargs)

        self._dags_folder_watcher = AirflowDagsWatcher(self._dags_folder)
        self._register_signal_handlers()
        # TODO once-of check for changes in the DAGs folder, which could have been made while Airflow was off (or it's the first time Airflow is started)

    def _register_signal_handlers(self):
        """Register handlers for certain signals."""
        signal.signal(signalnum=signal.SIGINT, handler=self._graceful_shutdown)  # CTRL + C
        signal.signal(signalnum=signal.SIGTERM, handler=self._graceful_shutdown)

    def _graceful_shutdown(self, signum, frame):
        """Try to clean up any processes before shutting down."""
        self.log.info("Received signal %s, shutting down gracefully", signal.Signals(signum).name)

        self.log.info("Stopping DAGs folder watcher")
        self._dags_folder_watcher.stop()
        self.log.info("Stopped DAGs folder watcher")

        self.log.info("Cleaned up all resources. Goodbye. 👋")
        sys.exit(os.EX_OK)

    def _execute(self):
        """Run the DAG serializer."""
        self.log.info("Starting the DAG serializer")
        self._dags_folder_watcher.start()
        self.log.info("DAG serializer stopped")


if __name__=="__main__":
    job = DagSerializerJob()
    job.run()
