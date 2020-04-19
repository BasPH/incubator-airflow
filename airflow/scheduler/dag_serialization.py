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

"""
This module provides functionality for watching a given directory holding DAG files for changes, and process
these changes. It relies on the watchdog library. Watchdog:
- Queues events internally (in a class EventQueue)
- Provides
"""

import logging
import time

from watchdog.events import FileSystemEvent, PatternMatchingEventHandler, FileModifiedEvent, FileMovedEvent
from watchdog.observers import Observer

from airflow.models import DAG, DagBag
from airflow.models.dagcode import DagCode
from airflow.models.serialized_dag import SerializedDagModel
from airflow.stats import Stats


class AirflowFileSystemEventHandler(PatternMatchingEventHandler):
    """
    This FileSystemEventHandler handles changes in the DAGs folder.

    See https://pythonhosted.org/watchdog/api.html?highlight=patternmatchingeventhandler#event-classes
    """

    def __init__(self):
        # The PatternMatchingEventHandler has more arguments, but there will be only one single
        # AirflowFileSystemEventHandler, so argument values are hardcoded.
        super().__init__(patterns=["*.py"], ignore_directories=False)

    def on_any_event(self, event):
        Stats.incr("dags_folder_events")

    def on_created(self, event: FileSystemEvent):
        if not event.is_directory:
            logging.info("Processing created file: %s", event.src_path)
            DAGSerializer.serialize_to_db(event.src_path)

    def on_deleted(self, event):
        """
        TODO what to do when file is deleted?
        My idea: delete DAG references but keep run history. Display red warning that DAG file was deleted.
        User can click "delete" to remove run history, or re-add DAG file.
        """

    def on_modified(self, event: FileModifiedEvent):
        logging.info("Processing modified file: %s", event.src_path)
        DAGSerializer.serialize_to_db(event.src_path)

    def on_moved(self, event: FileMovedEvent):
        # Only moves within the DAGs folder call on_moved().
        # A move from outside the DAGs folder to inside the DAGs folder calls on_created().
        # Therefore, we can simply change the file location in the DB, without re-processing the file.
        logging.info("Processing move from %s to %s", event.src_path, event.dest_path)
        DAGSerializer.move_serialized_dag(event.src_path, event.dest_path)


class AirflowDagsWatcher:
    """
    Class watching for filesystem changes in a given DAGs folder.

    .. code-block:: python

        watcher = AirflowDagsWatcher("/path/to/dags")
        watcher.start()

    """

    def __init__(self, dags_folder):
        self._dags_folder = dags_folder
        self._dags_folder_watcher = Observer()
        self._dags_folder_watcher.schedule(
            event_handler=AirflowFileSystemEventHandler(), path=self._dags_folder, recursive=True
        )

    def start(self):
        logging.info("Start watching DAGs in %s", self._dags_folder)
        self._dags_folder_watcher.start()
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logging.warning("Stopping the DAGs folder watcher")
            self._dags_folder_watcher.stop()
        self._dags_folder_watcher.join()

    def stop(self):
        self._dags_folder_watcher.stop()


class DAGSerializer:
    @staticmethod
    def serialize_to_db(file_path):
        start = time.time()
        dagbag = DagBag(
            dag_folder=file_path, include_examples=False, safe_mode=True, store_serialized_dags=True
        )
        dags = dagbag.process_file(file_path)
        logging.info("Found %s DAGs in %s: %s", len(dags), file_path, dags)
        DAG.bulk_sync_to_db(dags)
        SerializedDagModel.bulk_sync_to_db(dags)
        duration = time.time() - start
        Stats.timing("dagfile_serialization_time", duration * 1000)
        logging.info("Serialized %s DAGs in DB: %s", len(dags), dags)
        logging.debug("Serializing %s into DB took %s seconds", file_path, duration)

    @staticmethod
    def move_serialized_dag(src_file_path, dest_file_path):
        """
        The hashed file location serves as the primary key in the DagCode table.
        Therefore moving the file location is basically a new file in Airflow's DB, so delete the old first.
        """

        # If the old path doesn't exist in the DB there's no DAG, so no need to do any parsing
        start = time.time()
        DagCode.move_fileloc(src_file_path, dest_file_path)
        SerializedDagModel.move_fileloc(src_file_path, dest_file_path)
        duration = time.time() - start
        Stats.timing("dagfile_move_time", duration * 1000)
        logging.debug(
            "Registering move from %s to %s took %s seconds", src_file_path, dest_file_path, duration
        )
