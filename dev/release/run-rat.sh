#!/usr/bin/env bash
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

RAT_VERSION=0.13
RELEASE_DIR=$(cd "$(dirname "$BASH_SOURCE")"; pwd)
RAT_JAR="${RELEASE_DIR}/apache-rat-${RAT_VERSION}.jar"

# download apache rat
if [ ! -f "${RAT_JAR}" ]; then
  curl -s https://repo1.maven.org/maven2/org/apache/rat/apache-rat/${RAT_VERSION}/apache-rat-${RAT_VERSION}.jar > "${RAT_JAR}"
fi

RAT="java -jar ${RAT_JAR} -x "

# generate the rat report
# Run RAT from inside the target directory so it produces relative paths
cd "$1"
$RAT . > rat.txt
python3 $RELEASE_DIR/check-rat-report.py $RELEASE_DIR/rat_exclude_files.txt rat.txt > filtered_rat.txt
cat filtered_rat.txt
UNAPPROVED=`cat filtered_rat.txt  | grep "NOT APPROVED" | wc -l`

if [ "0" -eq "${UNAPPROVED}" ]; then
  echo "No unapproved licenses"
else
  echo "${UNAPPROVED} unapproved licences. Check rat report: rat.txt"
  exit 1
fi
