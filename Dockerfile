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

FROM hub.tess.io/adihadoop/maven:3.8-jdk-8-slim AS builder

RUN apt update && apt -y --allow-downgrades install tar zip git

RUN /workspace/build/adlc/release.sh

RUN mkdir -p /apache/releases &&\
    tar zxvf /workspace/apache-kyuubi-*.tgz -C /apache/releases &&\
    ln -fsn /apache/releases/apache-kyuubi-* /apache/kyuubi

FROM scratch
COPY --from=builder /apache /apache
COPY --from=builder /workspace/*.tgz ./
COPY --from=builder /workspace/*.md5 ./
COPY --from=builder /workspace/*.RELEASE ./

LABEL com.ebay.adi.adlc.include="*.tgz,*.md5,*.RELEASE"
# please update the version when releaseing new Kyuubi binary
LABEL com.ebay.adi.adlc.tag="1.8.0-ebay-SNAPSHOT"
