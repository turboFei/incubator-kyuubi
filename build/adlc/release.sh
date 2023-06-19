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

set -o pipefail
set -e
set -x

KYUUBI_HOME="$(cd "`dirname "$0"`/../.."; pwd)"
cd $KYUUBI_HOME

./build/adlc/adlc_version

VERSION=$(build/mvn help:evaluate -Dexpression=project.version 2>/dev/null| grep -v "INFO"| grep -v "WARNING"| tail -n 1)

build/dist --spark-provided --hive-provided --flink-provided -Pspark-3.1 -Drat.skip=true -s build/adlc/maven-settings.xml

# 1. prepare variables
TARDIR_NAME=apache-kyuubi-$VERSION-bin-ebay
TARDIR="$KYUUBI_HOME/$TARDIR_NAME"
DISTDIR="$KYUUBI_HOME/dist"
rm -rf $TARDIR
cp -r $DISTDIR $TARDIR

# 2. compress the binary
tar czf "$TARDIR_NAME.tgz" -C . $TARDIR_NAME

# 3. md5 and RELEASE
md5sum $TARDIR_NAME.tgz > $TARDIR_NAME.md5
cp $TARDIR/RELEASE $TARDIR_NAME.RELEASE
