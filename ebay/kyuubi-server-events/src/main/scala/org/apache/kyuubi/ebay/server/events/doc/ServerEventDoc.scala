/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kyuubi.ebay.server.events.doc

case class ServerEventDoc(
    serverName: String,
    startTime: Long,
    eventTime: Long,
    state: String,
    serverIP: String,
    serverConf: String,
    serverEnv: String,
    BUILD_USER: String,
    BUILD_DATE: String,
    REPO_URL: String,
    VERSION_INFO: String,
    eventType: String)
  extends EventDoc {
  override def docId: String = serverIP + "_" + EventDoc.timestampFormat.format(startTime)
  override def indexPartitionTime: Long = startTime
}
