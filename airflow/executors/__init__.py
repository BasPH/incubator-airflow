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

"""
Executors for running Airflow tasks on various systems, on a single machine (e.g. Sequential/Local) or
distributed (e.g. Celery/Kubernetes).
"""

import sys

from airflow import configuration, settings
from airflow.exceptions import AirflowException
from airflow.executors.base_executor import BaseExecutor  # noqa: F401
from airflow.executors.local_executor import LocalExecutor
from airflow.executors.sequential_executor import SequentialExecutor
from airflow.utils.log.logging_mixin import LoggingMixin

DEFAULT_EXECUTOR = None


def _integrate_plugins():
    """Integrate plugins to the context."""
    from airflow.plugins_manager import executors_modules
    for executors_module in executors_modules:
        sys.modules[executors_module.__name__] = executors_module
        globals()[executors_module._name] = executors_module  # pylint: disable=protected-access


def get_default_executor():
    """
    Get the configured executor. Returns cached executor if already created, otherwise initializes a new
    executor.

    :return: Subclass of BaseExecutor
    :rtype: airflow.executors.BaseExecutor
    """

    if settings.EXECUTOR is not None:
        return settings.EXECUTOR

    executor_name = configuration.conf.get('core', 'EXECUTOR')
    settings.EXECUTOR = _get_executor(executor_name)
    log = LoggingMixin().log
    log.info("Using executor %s", executor_name)
    return settings.EXECUTOR


class Executors:
    """Mapping from executor class to executor name."""
    LocalExecutor = "LocalExecutor"
    SequentialExecutor = "SequentialExecutor"
    CeleryExecutor = "CeleryExecutor"
    DaskExecutor = "DaskExecutor"
    KubernetesExecutor = "KubernetesExecutor"


def _get_executor(executor_name):
    """
    Creates a new instance of the named executor. In case the executor name is not know in Airflow, look for
    it in the plugins.
    """
    if executor_name == Executors.LocalExecutor:
        return LocalExecutor()
    elif executor_name == Executors.SequentialExecutor:
        return SequentialExecutor()
    elif executor_name == Executors.CeleryExecutor:
        from airflow.executors.celery_executor import CeleryExecutor
        return CeleryExecutor()
    elif executor_name == Executors.DaskExecutor:
        from airflow.executors.dask_executor import DaskExecutor
        return DaskExecutor()
    elif executor_name == Executors.KubernetesExecutor:
        from airflow.executors.kubernetes_executor import KubernetesExecutor
        return KubernetesExecutor()
    else:
        # Loading plugins
        _integrate_plugins()
        executor_path = executor_name.split('.')
        if len(executor_path) != 2:
            raise AirflowException(
                "Executor {0} not supported: "
                "please specify in format plugin_module.executor".format(executor_name))

        if executor_path[0] in globals():
            return globals()[executor_path[0]].__dict__[executor_path[1]]()
        else:
            raise AirflowException("Executor {0} not supported.".format(executor_name))
