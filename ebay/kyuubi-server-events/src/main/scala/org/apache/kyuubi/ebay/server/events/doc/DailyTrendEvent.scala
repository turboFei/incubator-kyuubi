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

/**
 * The daily trend event for session or operation.
 * @param date the event date
 * @param sessionCluster the session cluster
 * @param count the session/operation count
 * @param clusterUsers the cluster user count
 * @param queueUsers the queue user count
 * @param sessionTypeCounts the session type and count map
 */
case class DailyTrendEvent(
    date: String,
    sessionCluster: String,
    sessionQueue: String,
    count: Long,
    clusterUsers: Long,
    queueUsers: Long,
    sessionTypeCounts: Map[String, Long])
  extends EventDoc {
  override def docId: String = s"${date}_${sessionCluster}_${sessionQueue}"
}
