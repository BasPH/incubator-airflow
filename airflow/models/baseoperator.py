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

from typing import List, Union, cast

from airflow.exceptions import AirflowException
# noinspection PyPep8Naming
from airflow.operators.base import BaseOperator


def chain(*tasks: Union[BaseOperator, List[BaseOperator]]):
    r"""
    Given a number of tasks, builds a dependency chain.
    Support mix airflow.models.BaseOperator and List[airflow.models.BaseOperator].
    If you want to chain between two List[airflow.models.BaseOperator], have to
    make sure they have same length.

    .. code-block:: python

         chain(t1, [t2, t3], [t4, t5], t6)

    is equivalent to::

         / -> t2 -> t4 \
       t1               -> t6
         \ -> t3 -> t5 /

    .. code-block:: python

        t1.set_downstream(t2)
        t1.set_downstream(t3)
        t2.set_downstream(t4)
        t3.set_downstream(t5)
        t4.set_downstream(t6)
        t5.set_downstream(t6)

    :param tasks: List of tasks or List[airflow.models.BaseOperator] to set dependencies
    :type tasks: List[airflow.models.BaseOperator] or airflow.models.BaseOperator
    """
    for index, up_task in enumerate(tasks[:-1]):
        down_task = tasks[index + 1]
        if isinstance(up_task, BaseOperator):
            up_task.set_downstream(down_task)
            continue
        if isinstance(down_task, BaseOperator):
            down_task.set_upstream(up_task)
            continue
        if not isinstance(up_task, List) or not isinstance(down_task, List):
            raise TypeError(
                'Chain not supported between instances of {up_type} and {down_type}'.format(
                    up_type=type(up_task), down_type=type(down_task)))
        up_task_list = cast(List[BaseOperator], up_task)
        down_task_list = cast(List[BaseOperator], down_task)
        if len(up_task_list) != len(down_task_list):
            raise AirflowException(
                f'Chain not supported different length Iterable '
                f'but get {len(up_task_list)} and {len(down_task_list)}')
        for up_t, down_t in zip(up_task_list, down_task_list):
            up_t.set_downstream(down_t)


def cross_downstream(from_tasks: List[BaseOperator],
                     to_tasks: Union[BaseOperator, List[BaseOperator]]):
    r"""
    Set downstream dependencies for all tasks in from_tasks to all tasks in to_tasks.

    .. code-block:: python

        cross_downstream(from_tasks=[t1, t2, t3], to_tasks=[t4, t5, t6])

    is equivalent to::

        t1 ---> t4
           \ /
        t2 -X -> t5
           / \
        t3 ---> t6


    .. code-block:: python

        t1.set_downstream(t4)
        t1.set_downstream(t5)
        t1.set_downstream(t6)
        t2.set_downstream(t4)
        t2.set_downstream(t5)
        t2.set_downstream(t6)
        t3.set_downstream(t4)
        t3.set_downstream(t5)
        t3.set_downstream(t6)

    :param from_tasks: List of tasks to start from.
    :type from_tasks: List[airflow.models.BaseOperator]
    :param to_tasks: List of tasks to set as downstream dependencies.
    :type to_tasks: List[airflow.models.BaseOperator]
    """
    for task in from_tasks:
        task.set_downstream(to_tasks)
