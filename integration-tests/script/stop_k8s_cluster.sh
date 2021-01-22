#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e

if ($BUILD_DRUID_CLSUTER); then

  DRUID_HOME_DIR=$(dirname "$PWD")

  sudo /usr/local/bin/minikube delete

  echo "rm -rf $DRUID_HOME_DIR/tmp"
  sudo rm -rf $DRUID_HOME_DIR/tmp

  echo "rm -rf $DRUID_HOME_DIR/druid-operator"
  sudo rm -rf $DRUID_HOME_DIR/druid-operator

  docker ps
  ls -alt $DRUID_HOME_DIR
fi

