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

package org.apache.kyuubi.events

import org.apache.kyuubi.Utils
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.server.KyuubiServer
import org.apache.kyuubi.session.AbstractSession

/**
 * @param sessionId server session id
 * @param clientVersion client version
 * @param sessionName if user not specify it, we use empty string instead
 * @param user session user
 * @param clientIP client ip address
 * @param serverIP A unique Kyuubi server id, e.g. kyuubi server ip address and port,
 *                 it is useful if has multi-instance Kyuubi Server
 * @param remoteSessionId remote engine session id
 * @param startTime session create time
 * @param conf session config
 * @param openedTime session opened time
 * @param endTime session end time
 * @param totalOperations how many queries and meta calls
 * @param engineId engine id. For engine on yarn, it is applicationId.
 */
case class KyuubiSessionEvent(
    sessionId: String,
    clientVersion: Int,
    sessionName: String,
    user: String,
    clientIP: String,
    serverIP: String,
    conf: Map[String, String],
    startTime: Long,
    var sessionCluster: String = "",
    var remoteSessionId: String = "",
    var engineId: String = "",
    var openedTime: Long = -1L,
    var endTime: Long = -1L,
    var totalOperations: Int = 0) extends KyuubiEvent {
  override def partitions: Seq[(String, String)] =
    ("day", Utils.getDateFromTimestamp(startTime)) :: Nil
}

object KyuubiSessionEvent {
  def apply(session: AbstractSession): KyuubiSessionEvent = {
    assert(KyuubiServer.kyuubiServer != null)
    val serverIP = KyuubiServer.kyuubiServer.frontendServices.head.connectionUrl
    val sessionName: String = session.normalizedConf.getOrElse(KyuubiConf.SESSION_NAME.key, "")
    KyuubiSessionEvent(
      session.handle.toString,
      session.protocol.getValue,
      sessionName,
      session.user,
      session.ipAddress,
      serverIP,
      session.conf,
      session.createTime)
  }
}
