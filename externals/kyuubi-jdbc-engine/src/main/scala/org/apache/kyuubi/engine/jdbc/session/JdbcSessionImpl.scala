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
package org.apache.kyuubi.engine.jdbc.session

import java.sql.Connection

import scala.util.{Failure, Success, Try}

import org.apache.hive.service.rpc.thrift.TProtocolVersion

import org.apache.kyuubi.engine.jdbc.connection.ConnectionProvider
import org.apache.kyuubi.session.{AbstractSession, SessionManager}

class JdbcSessionImpl(
    protocol: TProtocolVersion,
    user: String,
    password: String,
    ipAddress: String,
    conf: Map[String, String],
    sessionManager: SessionManager)
  extends AbstractSession(protocol, user, password, ipAddress, conf, sessionManager) {

  private[jdbc] var sessionConnection: Connection = _

  private val kyuubiConf = sessionManager.getConf

  override def open(): Unit = {
    info(s"Starting to open jdbc session.")
    if (sessionConnection == null) {
      sessionConnection = ConnectionProvider.create(kyuubiConf)
    }
    super.open()
    info(s"The jdbc session is started.")
  }

  override def close(): Unit = {
    Try {
      if (sessionConnection != null) {
        sessionConnection.close()
      }
    } match {
      case Success(_) =>
        info(s"Closed session connection.")
      case Failure(exception) =>
        warn("Failed to close session connection, ignored it.", exception)
    }
    super.close()
  }

}
