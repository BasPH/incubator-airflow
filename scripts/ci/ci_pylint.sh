#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -exo pipefail

# TODO When Airflow is fully Pylint compatible, remove git-lint and run pylint on complete changed files
# TODO Using git-lint is an intermediate solution only for integrating Pylint!
if [[ ! -z TRAVIS_COMMIT_RANGE ]]
then
    # If running in Travis
    git reset --soft ${TRAVIS_COMMIT_RANGE%...*} && git lint
else
    # If running locally
    CURRENT_BRANCH=$(git symbolic-ref --short HEAD)
    OLDEST_COMMIT_NOT_ON_MASTER=$(git log ${CURRENT_BRANCH} --not master --no-merges --format="%H" | tail -1)
    git reset --soft ${OLDEST_COMMIT_NOT_ON_MASTER} && git lint && git reset HEAD@{1}
fi
