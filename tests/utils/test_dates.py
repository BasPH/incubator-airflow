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

import unittest
from datetime import datetime, timedelta

import pendulum
from dateutil.relativedelta import relativedelta
from parameterized import parameterized
from pendulum import Pendulum

from airflow.utils import dates
from airflow.utils import timezone


class TestDates(unittest.TestCase):
    def test_days_ago(self):
        today = pendulum.today()
        today_midnight = pendulum.instance(datetime.fromordinal(today.date().toordinal()))

        self.assertTrue(dates.days_ago(0) == today_midnight)

        self.assertTrue(dates.days_ago(100) == today_midnight + timedelta(days=-100))

        self.assertTrue(dates.days_ago(0, hour=3) == today_midnight + timedelta(hours=3))
        self.assertTrue(dates.days_ago(0, minute=3) == today_midnight + timedelta(minutes=3))
        self.assertTrue(dates.days_ago(0, second=3) == today_midnight + timedelta(seconds=3))
        self.assertTrue(dates.days_ago(0, microsecond=3) == today_midnight + timedelta(microseconds=3))

    def test_parse_execution_date(self):
        execution_date_str_wo_ms = '2017-11-02 00:00:00'
        execution_date_str_w_ms = '2017-11-05 16:18:30.989729'
        bad_execution_date_str = '2017-11-06TXX:00:00Z'

        self.assertEqual(
            timezone.datetime(2017, 11, 2, 0, 0, 0),
            dates.parse_execution_date(execution_date_str_wo_ms))
        self.assertEqual(
            timezone.datetime(2017, 11, 5, 16, 18, 30, 989729),
            dates.parse_execution_date(execution_date_str_w_ms))
        self.assertRaises(ValueError, dates.parse_execution_date, bad_execution_date_str)

    @parameterized.expand([
        [datetime(2019, 1, 1), timedelta(days=2), datetime(2019, 1, 5), [datetime(2019, 1, 1), datetime(2019, 1, 3)]],  # Test if date_range doesn't return end_date and beyond.
        [datetime(2019, 1, 4), timedelta(days=1), datetime(2019, 1, 1), [datetime(2019, 1, 4), datetime(2019, 1, 3), datetime(2019, 1, 2)]],  # Test date_range with timedelta interval and end_date < start_date.
        [datetime(2019, 1, 1), relativedelta(years=1), datetime(2022, 1, 1), [datetime(2019, 1, 1), datetime(2020, 1, 1), datetime(2021, 1, 1)]],  # Test date_range with relativedelta interval.
        [datetime(2019, 1, 1), "0 0 * * *", datetime(2019, 1, 4), [datetime(2019, 1, 1), datetime(2019, 1, 2), datetime(2019, 1, 3)]],  # Test date_range with cron interval.
        [datetime(2019, 1, 4), "0 0 * * *", datetime(2019, 1, 1), [datetime(2019, 1, 4), datetime(2019, 1, 3), datetime(2019, 1, 2)]],  # Test date_range with cron interval and end_date < start_date.
        [datetime(2019, 1, 28), "0 0 */3 * *", datetime(2019, 2, 5), [datetime(2019, 1, 28), datetime(2019, 1, 31), datetime(2019, 2, 1), datetime(2019, 2, 4)]],  # Test date_range with an awkward cron interval, resulting in uneven intervals, see https://stackoverflow.com/questions/4549542/cron-job-every-three-days.
    ])
    def test_date_range(self, start, interval, end, expected):
        """Test date_range() for various inputs and expected outputs."""
        result = list(dates.date_range(start_date=start, interval=interval, end_date=end))
        self.assertEqual(result, expected)

    def test_date_range_no_end_date(self):
        """Test date_range without end_date."""
        start = datetime(2019, 1, 1)
        interval = timedelta(days=1)
        generator = dates.date_range(start_date=start, interval=interval)
        result = list(next(generator) for _ in range(3))
        expected = [datetime(2019, 1, 1, 0, 0), datetime(2019, 1, 2, 0, 0), datetime(2019, 1, 3, 0, 0)]
        self.assertEqual(result, expected)

    @parameterized.expand([
        [timedelta(days=1)],
        [relativedelta(days=1)],
        ["0 0 * * *"],
    ])
    def test_date_range_pendulum(self, interval):
        """Test if date_range returns Pendulum type given various interval types."""
        start = Pendulum(2019, 1, 1)
        end = Pendulum(2019, 1, 4)
        result = list(dates.date_range(start_date=start, interval=interval, end_date=end))
        expected = [Pendulum(2019, 1, 1), Pendulum(2019, 1, 2), Pendulum(2019, 1, 3)]
        self.assertEqual(result, expected)
        self.assertTrue(all(isinstance(x, Pendulum) for x in result))

    def test_date_range_equal_start_end(self):
        """Test if date_range with equal start and end returns nothing."""
        result = dates.date_range(start_date=datetime(2019, 1, 1), interval=timedelta(days=1), end_date=datetime(2019, 1, 1))
        with self.assertRaises(StopIteration):
            next(result)

    def test_date_range_with_timezone(self):
        """Test date_range() for dates with time zone information, checking if it is preserved."""
        start = Pendulum(2019, 1, 1, tzinfo=pendulum.timezone("Europe/Amsterdam"))
        interval = timedelta(days=1)
        end = Pendulum(2019, 1, 4, tzinfo=pendulum.timezone("Europe/Amsterdam"))
        result = list(dates.date_range(start_date=start, interval=interval, end_date=end))
        self.assertEqual(result, [Pendulum(2019, 1, 1, tzinfo=pendulum.timezone("Europe/Amsterdam")), Pendulum(2019, 1, 2, tzinfo=pendulum.timezone("Europe/Amsterdam")), Pendulum(2019, 1, 3, tzinfo=pendulum.timezone("Europe/Amsterdam"))])
        self.assertNotEquals(result, [Pendulum(2019, 1, 1), Pendulum(2019, 1, 2), Pendulum(2019, 1, 3)])
