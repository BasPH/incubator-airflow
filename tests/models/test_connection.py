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

from mock import patch

from airflow import configuration
from airflow.models.connection import Connection


class ConnectionTest(unittest.TestCase):
    @patch.object(configuration, 'get')
    def test_connection_extra_no_encryption(self, mock_get):
        """
        Tests extras on a new connection without encryption. The fernet key
        is set to a non-base64-encoded string and the extra is stored without
        encryption.
        """
        test_connection = Connection(extra='testextra')
        self.assertEqual(test_connection.extra, 'testextra')

    @patch.object(configuration, 'get')
    def test_connection_extra_with_encryption(self, mock_get):
        """
        Tests extras on a new connection with encryption. The fernet key
        is set to a base64 encoded string and the extra is encrypted.
        """
        # 'dGVzdA==' is base64 encoded 'test'
        mock_get.return_value = 'dGVzdA=='
        test_connection = Connection(extra='testextra')
        self.assertEqual(test_connection.extra, 'testextra')

    def test_connection_from_uri_without_extras(self):
        uri = 'scheme://user:password@host%2flocation:1234/schema'
        connection = Connection(uri=uri)
        self.assertEqual(connection.conn_type, 'scheme')
        self.assertEqual(connection.host, 'host/location')
        self.assertEqual(connection.schema, 'schema')
        self.assertEqual(connection.login, 'user')
        self.assertEqual(connection.password, 'password')
        self.assertEqual(connection.port, 1234)
        self.assertIsNone(connection.extra)

    def test_connection_from_uri_with_extras(self):
        uri = 'scheme://user:password@host%2flocation:1234/schema?' \
              'extra1=a%20value&extra2=%2fpath%2f'
        connection = Connection(uri=uri)
        self.assertEqual(connection.conn_type, 'scheme')
        self.assertEqual(connection.host, 'host/location')
        self.assertEqual(connection.schema, 'schema')
        self.assertEqual(connection.login, 'user')
        self.assertEqual(connection.password, 'password')
        self.assertEqual(connection.port, 1234)
        self.assertDictEqual(connection.extra_dejson, {'extra1': 'a value',
                                                       'extra2': '/path/'})

    def test_connection_from_uri_with_colon_in_hostname(self):
        uri = 'scheme://user:password@host%2flocation%3ax%3ay:1234/schema?' \
              'extra1=a%20value&extra2=%2fpath%2f'
        connection = Connection(uri=uri)
        self.assertEqual(connection.conn_type, 'scheme')
        self.assertEqual(connection.host, 'host/location:x:y')
        self.assertEqual(connection.schema, 'schema')
        self.assertEqual(connection.login, 'user')
        self.assertEqual(connection.password, 'password')
        self.assertEqual(connection.port, 1234)
        self.assertDictEqual(connection.extra_dejson, {'extra1': 'a value',
                                                       'extra2': '/path/'})

    def test_connection_from_uri_with_encoded_password(self):
        uri = 'scheme://user:password%20with%20space@host%2flocation%3ax%3ay:1234/schema'
        connection = Connection(uri=uri)
        self.assertEqual(connection.conn_type, 'scheme')
        self.assertEqual(connection.host, 'host/location:x:y')
        self.assertEqual(connection.schema, 'schema')
        self.assertEqual(connection.login, 'user')
        self.assertEqual(connection.password, 'password with space')
        self.assertEqual(connection.port, 1234)

    def test_connection_from_uri_with_encoded_user(self):
        uri = 'scheme://domain%2fuser:password@host%2flocation%3ax%3ay:1234/schema'
        connection = Connection(uri=uri)
        self.assertEqual(connection.conn_type, 'scheme')
        self.assertEqual(connection.host, 'host/location:x:y')
        self.assertEqual(connection.schema, 'schema')
        self.assertEqual(connection.login, 'domain/user')
        self.assertEqual(connection.password, 'password')
        self.assertEqual(connection.port, 1234)

    def test_connection_from_uri_with_encoded_schema(self):
        uri = 'scheme://user:password%20with%20space@host:1234/schema%2ftest'
        connection = Connection(uri=uri)
        self.assertEqual(connection.conn_type, 'scheme')
        self.assertEqual(connection.host, 'host')
        self.assertEqual(connection.schema, 'schema/test')
        self.assertEqual(connection.login, 'user')
        self.assertEqual(connection.password, 'password with space')
        self.assertEqual(connection.port, 1234)

    def test_connection_from_uri_no_schema(self):
        uri = 'scheme://user:password%20with%20space@host:1234'
        connection = Connection(uri=uri)
        self.assertEqual(connection.conn_type, 'scheme')
        self.assertEqual(connection.host, 'host')
        self.assertEqual(connection.schema, '')
        self.assertEqual(connection.login, 'user')
        self.assertEqual(connection.password, 'password with space')
        self.assertEqual(connection.port, 1234)
