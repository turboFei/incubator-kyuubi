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

import java.io.{PrintWriter, StringWriter}
import java.util.TimeZone

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.commons.lang3.time.FastDateFormat

import org.apache.kyuubi.ebay.server.events.doc.EventDoc.{dateFormat, indexDelimiter}
import org.apache.kyuubi.events.{KyuubiEvent, KyuubiOperationEvent, KyuubiServerInfoEvent, KyuubiSessionEvent}

trait EventDoc {
  def docId: String
  def indexPartitionTime: Long
  final def formatPartitionIndex(index: String): String = {
    index + indexDelimiter + dateFormat.format(indexPartitionTime)
  }
  final def toJson: String = EventDoc.mapper.writeValueAsString(this)
}

object EventDoc {
  final val mapper = new ObjectMapper().registerModule(DefaultScalaModule)
  final val dateFormat = FastDateFormat.getInstance("yyyyMMdd", TimeZone.getDefault)
  final val timestampFormat = FastDateFormat.getInstance("yyyyMMddHHmmss", TimeZone.getDefault)
  final val indexDelimiter = "_"

  /**
   * Copied from org.apache.spark.util.Utils::exceptionString
   */
  private def exceptionString(exception: Option[Throwable]): String = {
    exception.map { e =>
      // Use e.printStackTrace here because e.getStackTrace doesn't include the cause
      val stringWriter = new StringWriter()
      e.printStackTrace(new PrintWriter(stringWriter))
      stringWriter.toString
    }.getOrElse("")
  }

  def apply(event: KyuubiEvent): EventDoc = {
    event match {
      case e: KyuubiServerInfoEvent =>
        ServerEventDoc(
          e.serverName,
          e.startTime,
          e.eventTime,
          e.state,
          e.serverIP,
          mapper.writeValueAsString(e.serverConf),
          mapper.writeValueAsString(e.serverEnv),
          e.BUILD_USER,
          e.BUILD_DATE,
          e.REPO_URL,
          mapper.writeValueAsString(e.VERSION_INFO),
          e.eventType)
      case e: KyuubiSessionEvent =>
        SessionEventDoc(
          e.sessionId,
          e.clientVersion,
          e.sessionType,
          e.sessionName,
          e.user,
          e.clientIP,
          e.serverIP,
          mapper.writeValueAsString(e.conf),
          e.eventTime,
          e.startTime,
          e.sessionCluster,
          e.remoteSessionId,
          e.engineId,
          e.openedTime,
          e.endTime,
          e.totalOperations,
          exceptionString(e.exception),
          e.eventType)
      case e: KyuubiOperationEvent =>
        OperationEventDoc(
          e.statementId,
          e.remoteId,
          e.statement,
          e.shouldRunAsync,
          e.state,
          e.eventTime,
          e.createTime,
          e.startTime,
          e.completeTime,
          exceptionString(e.exception),
          e.sessionId,
          e.sessionUser,
          e.sessionType,
          e.eventType)
      case _ => throw new RuntimeException(s"Unknown kyuubi event: $event")
    }
  }
}
