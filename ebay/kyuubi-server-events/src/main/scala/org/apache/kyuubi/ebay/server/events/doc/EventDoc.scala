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

import org.apache.kyuubi.Utils
import org.apache.kyuubi.config.{KyuubiConf, KyuubiEbayConf}
import org.apache.kyuubi.config.KyuubiConf.SERVER_SECRET_REDACTION_PATTERN
import org.apache.kyuubi.ebay.server.events.doc.EventDoc.{dateFormat, indexDelimiter}
import org.apache.kyuubi.events.{KyuubiEvent, KyuubiOperationEvent, KyuubiServerInfoEvent, KyuubiSessionEvent}

trait EventDoc {
  def docId: String
  def indexPartitionTime: Option[Long] = None
  final def formatPartitionIndex(index: String): String = indexPartitionTime match {
    case Some(partitionTime) => index + indexDelimiter + dateFormat.format(partitionTime)
    case _ => index
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

  private def mapToRedactedString(conf: KyuubiConf, map: Map[String, String]): String = {
    val redactionPattern = conf.get(SERVER_SECRET_REDACTION_PATTERN)
    val newMap = map.map { case (key, value) =>
      Utils.redact(redactionPattern, Seq((key, value))).head
    }
    mapper.writeValueAsString(newMap)
  }

  def apply(event: KyuubiEvent, conf: KyuubiConf = KyuubiConf()): EventDoc = {
    event match {
      case e: KyuubiServerInfoEvent =>
        ServerEventDoc(
          e.serverName,
          e.startTime,
          e.eventTime,
          e.state,
          e.serverIP,
          mapToRedactedString(conf, e.serverConf),
          mapToRedactedString(conf, e.serverEnv),
          e.BUILD_USER,
          e.BUILD_DATE,
          e.REPO_URL,
          mapToRedactedString(conf, e.VERSION_INFO),
          e.eventType)
      case e: KyuubiSessionEvent =>
        val reserveMetadata = e.conf.getOrElse(
          KyuubiEbayConf.SESSION_METADATA_RESERVE.key,
          KyuubiEbayConf.SESSION_METADATA_RESERVE.defaultValStr).toBoolean
        val sessionConf = if (reserveMetadata) {
          e.conf
        } else {
          Map(KyuubiEbayConf.SESSION_METADATA_RESERVE.key -> "false")
        }
        SessionEventDoc(
          e.sessionId,
          e.clientVersion,
          e.sessionType,
          e.sessionName,
          e.user,
          e.clientIP,
          e.serverIP,
          mapToRedactedString(conf, sessionConf),
          e.eventTime,
          e.startTime,
          e.sessionCluster,
          e.queue,
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
          e.kyuubiInstance,
          e.eventType,
          e.sessionCluster,
          e.sessionQueue)
      case _ => throw new RuntimeException(s"Unknown kyuubi event: $event")
    }
  }
}
