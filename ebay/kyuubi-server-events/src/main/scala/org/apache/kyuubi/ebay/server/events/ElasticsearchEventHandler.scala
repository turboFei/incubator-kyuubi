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

package org.apache.kyuubi.ebay.server.events

import java.time.Duration
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import org.apache.kyuubi.{Logging, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiEbayConf._
import org.apache.kyuubi.ebay.server.events.doc.{EventDoc, OperationEventDoc, ServerEventDoc, SessionEventDoc}
import org.apache.kyuubi.events.KyuubiEvent
import org.apache.kyuubi.events.handler.EventHandler
import org.apache.kyuubi.util.ThreadUtils

class ElasticsearchEventHandler(conf: KyuubiConf) extends EventHandler[KyuubiEvent] with Logging {
  import ElasticsearchEventHandler._
  private val serverEventIndex = conf.get(ELASTIC_SEARCH_SERVER_EVENT_INDEX)
  private val sessionEventIndex = conf.get(ELASTIC_SEARCH_SESSION_EVENT_INDEX)
  private val operationEventIndex = conf.get(ELASTIC_SEARCH_OPERATION_EVENT_INDEX)
  private val serverEventAlias = conf.get(ELASTIC_SEARCH_SERVER_EVENT_ALIAS)
  private val sessionEventAlias = conf.get(ELASTIC_SEARCH_SESSION_EVENT_ALIAS)
  private val operationEventAlias = conf.get(ELASTIC_SEARCH_OPERATION_EVENT_ALIAS)
  private val indexSet = ConcurrentHashMap.newKeySet[String]()

  private val serverEventPurge = conf.get(ELASTIC_SEARCH_SERVER_EVENT_PURGE_ENABLED)
  private val sessionEventPurge = conf.get(ELASTIC_SEARCH_SESSION_EVENT_PURGE_ENABLED)
  private val operationEventPurge = conf.get(ELASTIC_SEARCH_OPERATION_EVENT_PURGE_ENABLED)

  ElasticsearchUtils.init(conf)
  initialize()

  private def initialize(): Unit = {
    Seq(serverEventAlias, sessionEventAlias, operationEventAlias).foreach { alias =>
      Utils.tryLogNonFatalError {
        val indexes = ElasticsearchUtils.getAliasIndexes(alias)
        indexSet.addAll(indexes.asJava)
      }
    }
    startEventIndexPurger()
  }

  override def apply(event: KyuubiEvent): Unit = {
    val eventDoc = EventDoc(event)
    ElasticsearchUtils.createDoc(getEventDocIndex(eventDoc), eventDoc.docId, eventDoc.toJson)
  }

  private def getEventDocIndex(eventDoc: EventDoc): String = {
    val (index, mapping, alias) = eventDoc match {
      case doc: ServerEventDoc =>
        (
          doc.formatPartitionIndex(serverEventIndex),
          eventMapping.getOrElse(KYUUBI_SERVER_EVENT_MAPPING_NAME, ""),
          serverEventAlias)
      case doc: SessionEventDoc =>
        (
          doc.formatPartitionIndex(sessionEventIndex),
          eventMapping.getOrElse(KYUUBI_SESSION_EVENT_MAPPING_NAME, ""),
          sessionEventAlias)
      case doc: OperationEventDoc =>
        (
          doc.formatPartitionIndex(operationEventIndex),
          eventMapping.getOrElse(KYUUBI_OPERATION_EVENT_MAPPING_NAME, ""),
          operationEventAlias)
    }
    if (!indexSet.contains(index)) {
      if (!ElasticsearchUtils.indexExists(index)) {
        ElasticsearchUtils.createIndex(index, mapping)
      }
      if (index != alias) {
        ElasticsearchUtils.addIndexAlias(index, alias)
      }
      indexSet.add(index)
    }
    index
  }

  private def purgeEventIndex(
      eventAlias: String,
      eventIndex: String,
      maxDays: Int,
      currentDayTime: Long): Unit = {
    val minTime = currentDayTime - Duration.ofDays(maxDays).toMillis
    val indexes = ElasticsearchUtils.getAliasIndexes(eventAlias)
    indexSet.addAll(indexes.asJava)
    indexes.filter(_.startsWith(eventIndex)).foreach { index =>
      val partitionDay = index.stripPrefix(eventIndex + EventDoc.indexDelimiter)
      try {
        if (EventDoc.dateFormat.parse(partitionDay).getTime < minTime) {
          ElasticsearchUtils.deleteIndex(index)
          indexSet.remove(index)
        }
      } catch {
        case e: Throwable => error(s"Error purging index[$index]", e)
      }
    }
  }

  private def startEventIndexPurger(): Unit = {
    if (serverEventPurge || sessionEventPurge || operationEventPurge) {
      val serverEventMaxDays = conf.get(ELASTIC_SEARCH_SERVER_EVENT_MAX_DAYS)
      val sessionEventMaxDays = conf.get(ELASTIC_SEARCH_SESSION_EVENT_MAX_DAYS)
      val operationEventMaxDays = conf.get(ELASTIC_SEARCH_OPERATION_EVENT_MAX_DAYS)

      val task = new Runnable {
        override def run(): Unit = {
          try {
            val currentTime = System.currentTimeMillis()
            val currentDayTime =
              EventDoc.dateFormat.parse(EventDoc.dateFormat.format(currentTime)).getTime
            if (serverEventPurge) {
              purgeEventIndex(
                serverEventAlias,
                serverEventIndex,
                serverEventMaxDays,
                currentDayTime)
            }
            if (sessionEventPurge) {
              purgeEventIndex(
                sessionEventAlias,
                sessionEventIndex,
                sessionEventMaxDays,
                currentDayTime)
            }
            if (operationEventPurge) {
              purgeEventIndex(
                operationEventAlias,
                operationEventIndex,
                operationEventMaxDays,
                currentDayTime)
            }
          } catch {
            case e: Throwable => error("Error purging the elasticsearch events", e)
          }
        }
      }
      val interval = Duration.ofDays(1).toMillis
      ThreadUtils.newDaemonSingleThreadScheduledExecutor(
        "elasticsearch-events-purger").scheduleWithFixedDelay(
        task,
        interval,
        interval,
        TimeUnit.MILLISECONDS)
    }
  }
}

object ElasticsearchEventHandler {
  final val mapper = new ObjectMapper().registerModule(DefaultScalaModule)
  final val KYUUBI_EVENT_MAPPING_RESOURCE = "kyuubi-event-mapping.json"
  final val KYUUBI_SERVER_EVENT_MAPPING_NAME = "kyuubi-server-event"
  final val KYUUBI_SESSION_EVENT_MAPPING_NAME = "kyuubi-session-event"
  final val KYUUBI_OPERATION_EVENT_MAPPING_NAME = "kyuubi-operation-event"

  final lazy val eventMapping: Map[String, String] = {
    val source = getClass.getClassLoader.getResourceAsStream(KYUUBI_EVENT_MAPPING_RESOURCE)
    val eventMapping = mapper.readTree(source)
    Map(
      KYUUBI_SERVER_EVENT_MAPPING_NAME -> eventMapping.get(
        KYUUBI_SERVER_EVENT_MAPPING_NAME).toString,
      KYUUBI_SESSION_EVENT_MAPPING_NAME -> eventMapping.get(
        KYUUBI_SESSION_EVENT_MAPPING_NAME).toString,
      KYUUBI_OPERATION_EVENT_MAPPING_NAME -> eventMapping.get(
        KYUUBI_OPERATION_EVENT_MAPPING_NAME).toString)
  }
}
