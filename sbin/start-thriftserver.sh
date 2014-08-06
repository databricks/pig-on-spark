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

#
# Shell script for starting the Spark SQL Thrift server

# Enter posix mode for bash
set -o posix

# Figure out where Spark is installed
FWDIR="$(cd `dirname $0`/..; pwd)"

if [[ "$@" = *--help ]] || [[ "$@" = *-h ]]; then
  echo "Usage: ./sbin/start-thriftserver [options]"
  $FWDIR/bin/spark-submit --help 2>&1 | grep -v Usage 1>&2
  exit 0
fi

CLASS="org.apache.spark.sql.hive.thriftserver.HiveThriftServer2"
exec "$FWDIR"/bin/spark-submit --class $CLASS spark-internal $@
