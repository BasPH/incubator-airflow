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

from datetime import datetime, timedelta
from typing import Union, Generator

from croniter import croniter
from dateutil.relativedelta import relativedelta  # noqa: F401 for doctest
from pendulum import Pendulum

from airflow.utils import timezone

cron_presets = {
    '@hourly': '0 * * * *',
    '@daily': '0 0 * * *',
    '@weekly': '0 0 * * 0',
    '@monthly': '0 0 1 * *',
    '@yearly': '0 0 1 1 *',
}


def date_range(
    start_date: Union[datetime, Pendulum],
    interval: Union[timedelta, relativedelta, str],
    end_date: Union[datetime, Pendulum] = None,
) -> Generator:
    """
    Get a range of dates between a given start and (optional) end date, with a given interval (just like
    builtins.range, the start_date is included in the result and the end_date is not). To get dates in reverse
    order, either switch the start_date and end_date, or call reversed on the result:
    ``reversed(date_range(...))``. The end_date is optional; the result is a generator and simply calling
    next() will return the next datetime. The result type equals the type of start_date.

    Examples:
    date_range(datetime(2019, 1, 1), timedelta(days=1), datetime(2019, 1, 4)) -> jan 1, 2 & 3
    date_range(datetime(2019, 1, 1), "0 0 * * *", datetime(2019, 1, 4))       -> jan 1, 2 & 3
    date_range(datetime(2019, 1, 1), "0 0 * * *", datetime(2019, 1, 1))       -> no result
    date_range(Pendulum(2019, 1, 1), "0 0 * * *", Pendulum(2019, 1, 4))       -> jan 1, 2 & 3 in Pendulum

    End date is optional:
        generator = dates.date_range(datetime(2019, 1, 1), timedelta(days=1))
        result = list(next(generator) for _ in range(3))                      -> jan 1, 2 & 3

    :param start_date: starting datetime
    :param interval: time interval
    :param end_date: ending datetime (optional)
    :return: Generator holding next datetime
    """

    if not start_date == end_date:
        yield start_date

    # Computing intervals depends on type of the given interval
    if isinstance(interval, (timedelta, relativedelta)):
        if not end_date:  # no end date, infinitely return next date
            while True:
                start_date += interval
                yield start_date

        if end_date >= start_date:  # end date after start date, return next date forward in time
            while start_date < end_date:
                start_date += interval
                if start_date < end_date:
                    yield start_date
        else:  # end date before start date, return next date backwards in time
            while start_date > end_date:
                start_date -= interval
                if start_date > end_date:
                    yield start_date

    elif isinstance(interval, str):
        # Assume cron expression
        cron = croniter(interval, start_date)

        # croniter does not return Pendulum so we make the conversion back if original type was Pendulum
        original_type = type(start_date)

        next_dt = start_date
        if end_date >= start_date:
            while start_date < end_date and next_dt < end_date:
                next_dt = cron.get_next(datetime)
                if next_dt < end_date:
                    if original_type == Pendulum:
                        yield Pendulum.instance(next_dt)
                    else:
                        yield next_dt
        else:
            while start_date > end_date and next_dt > end_date:
                next_dt = cron.get_prev(datetime)
                if next_dt > end_date:
                    if original_type == Pendulum:
                        yield Pendulum.instance(next_dt)
                    else:
                        yield next_dt

    else:
        raise TypeError("interval must be of types timedelta, relativedelta or string.")


def round_time(dt, delta, start_date=timezone.make_aware(datetime.min)):
    """
    Returns the datetime of the form start_date + i * delta
    which is closest to dt for any non-negative integer i.
    Note that delta may be a datetime.timedelta or a dateutil.relativedelta
    >>> round_time(datetime(2015, 1, 1, 6), timedelta(days=1))
    datetime.datetime(2015, 1, 1, 0, 0)
    >>> round_time(datetime(2015, 1, 2), relativedelta(months=1))
    datetime.datetime(2015, 1, 1, 0, 0)
    >>> round_time(datetime(2015, 9, 16, 0, 0), timedelta(1), datetime(2015, 9, 14, 0, 0))
    datetime.datetime(2015, 9, 16, 0, 0)
    >>> round_time(datetime(2015, 9, 15, 0, 0), timedelta(1), datetime(2015, 9, 14, 0, 0))
    datetime.datetime(2015, 9, 15, 0, 0)
    >>> round_time(datetime(2015, 9, 14, 0, 0), timedelta(1), datetime(2015, 9, 14, 0, 0))
    datetime.datetime(2015, 9, 14, 0, 0)
    >>> round_time(datetime(2015, 9, 13, 0, 0), timedelta(1), datetime(2015, 9, 14, 0, 0))
    datetime.datetime(2015, 9, 14, 0, 0)
    """

    if isinstance(delta, str):
        # It's cron based, so it's easy
        tz = start_date.tzinfo
        start_date = timezone.make_naive(start_date, tz)
        cron = croniter(delta, start_date)
        prev = cron.get_prev(datetime)
        if prev == start_date:
            return timezone.make_aware(start_date, tz)
        else:
            return timezone.make_aware(prev, tz)

    # Ignore the microseconds of dt
    dt -= timedelta(microseconds=dt.microsecond)

    # We are looking for a datetime in the form start_date + i * delta
    # which is as close as possible to dt. Since delta could be a relative
    # delta we don't know its exact length in seconds so we cannot rely on
    # division to find i. Instead we employ a binary search algorithm, first
    # finding an upper and lower limit and then disecting the interval until
    # we have found the closest match.

    # We first search an upper limit for i for which start_date + upper * delta
    # exceeds dt.
    upper = 1
    while start_date + upper * delta < dt:
        # To speed up finding an upper limit we grow this exponentially by a
        # factor of 2
        upper *= 2

    # Since upper is the first value for which start_date + upper * delta
    # exceeds dt, upper // 2 is below dt and therefore forms a lower limited
    # for the i we are looking for
    lower = upper // 2

    # We now continue to intersect the interval between
    # start_date + lower * delta and start_date + upper * delta
    # until we find the closest value
    while True:
        # Invariant: start + lower * delta < dt <= start + upper * delta
        # If start_date + (lower + 1)*delta exceeds dt, then either lower or
        # lower+1 has to be the solution we are searching for
        if start_date + (lower + 1) * delta >= dt:
            # Check if start_date + (lower + 1)*delta or
            # start_date + lower*delta is closer to dt and return the solution
            if (start_date + (lower + 1) * delta) - dt <= dt - (start_date + lower * delta):
                return start_date + (lower + 1) * delta
            else:
                return start_date + lower * delta

        # We intersect the interval and either replace the lower or upper
        # limit with the candidate
        candidate = lower + (upper - lower) // 2
        if start_date + candidate * delta >= dt:
            upper = candidate
        else:
            lower = candidate

    # in the special case when start_date > dt the search for upper will
    # immediately stop for upper == 1 which results in lower = upper // 2 = 0
    # and this function returns start_date.


def infer_time_unit(time_seconds_arr):
    """
    Determine the most appropriate time unit for an array of time durations
    specified in seconds.
    e.g. 5400 seconds => 'minutes', 36000 seconds => 'hours'
    """
    if len(time_seconds_arr) == 0:
        return 'hours'
    max_time_seconds = max(time_seconds_arr)
    if max_time_seconds <= 60 * 2:
        return 'seconds'
    elif max_time_seconds <= 60 * 60 * 2:
        return 'minutes'
    elif max_time_seconds <= 24 * 60 * 60 * 2:
        return 'hours'
    else:
        return 'days'


def scale_time_units(time_seconds_arr, unit):
    """
    Convert an array of time durations in seconds to the specified time unit.
    """
    if unit == 'minutes':
        return list(map(lambda x: x * 1.0 / 60, time_seconds_arr))
    elif unit == 'hours':
        return list(map(lambda x: x * 1.0 / (60 * 60), time_seconds_arr))
    elif unit == 'days':
        return list(map(lambda x: x * 1.0 / (24 * 60 * 60), time_seconds_arr))
    return time_seconds_arr


def days_ago(n, hour=0, minute=0, second=0, microsecond=0):
    """
    Get a datetime object representing `n` days ago. By default the time is
    set to midnight.
    """
    today = timezone.utcnow().replace(
        hour=hour,
        minute=minute,
        second=second,
        microsecond=microsecond)
    return today - timedelta(days=n)


def parse_execution_date(execution_date_str):
    """
    Parse execution date string to datetime object.
    """
    return timezone.parse(execution_date_str)
