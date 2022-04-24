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
package org.apache.kyuubi.engine.trino.event

import org.apache.kyuubi.{KyuubiSQLException, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.trino.session.TrinoSessionImpl
import org.apache.kyuubi.events.KyuubiEvent

case class TrinoSessionEvent(
    sessionId: String,
    username: String,
    ip: String,
    connectionUrl: String,
    catalog: String,
    startTime: Long,
    var endTime: Long = -1L,
    var totalOperations: Int = 0) extends KyuubiEvent {

  override def partitions: Seq[(String, String)] =
    ("day", Utils.getDateFromTimestamp(startTime)) :: Nil

}

object TrinoSessionEvent {

  def apply(session: TrinoSessionImpl): TrinoSessionEvent = {
    val sessionConf = session.sessionManager.getConf
    val connectionUrl = sessionConf.get(KyuubiConf.ENGINE_TRINO_CONNECTION_URL).getOrElse(
      throw KyuubiSQLException("Trino server url can not be null!"))
    val catalog = sessionConf.get(KyuubiConf.ENGINE_TRINO_CONNECTION_CATALOG).getOrElse(
      throw KyuubiSQLException("Trino default catalog can not be null!"))
    new TrinoSessionEvent(
      session.handle.identifier.toString,
      session.user,
      session.ipAddress,
      connectionUrl = connectionUrl,
      catalog = catalog,
      session.createTime)
  }
}
