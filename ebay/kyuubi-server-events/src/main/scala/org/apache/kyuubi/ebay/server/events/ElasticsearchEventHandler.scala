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
import org.apache.kyuubi.ebay.server.events.doc.{DailyTrendEvent, EventDoc, OperationEventDoc, ServerEventDoc, SessionEventDoc, UserDailyTrendEvent}
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
    startEventAggregator()
  }

  override def apply(event: KyuubiEvent): Unit = {
    val eventDoc = EventDoc(event, conf)
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
    val prefix = eventIndex + EventDoc.indexDelimiter
    indexes.filter(_.startsWith(prefix)).foreach { index =>
      val partitionDay = index.stripPrefix(prefix)
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
    val serverEventPurge = conf.get(ELASTIC_SEARCH_SERVER_EVENT_PURGE_ENABLED)
    val sessionEventPurge = conf.get(ELASTIC_SEARCH_SESSION_EVENT_PURGE_ENABLED)
    val operationEventPurge = conf.get(ELASTIC_SEARCH_OPERATION_EVENT_PURGE_ENABLED)

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

  private def startEventAggregator(): Unit = {
    if (conf.get(ELASTIC_SEARCH_AGG_ENABLED)) {
      val aggInterval = conf.get(ELASTIC_SEARCH_AGG_INTERVAL)
      val aggDays = conf.get(ELASTIC_SEARCH_AGG_DAYS)
      val aggSize = conf.get(ELASTIC_SEARCH_AGG_SIZE)
      val aggMinDocCount = conf.get(ELASTIC_SEARCH_AGG_MIN_DOC_COUNT)

      val dailySessionIndex = conf.get(ELASTIC_SEARCH_DAILY_SESSION_INDEX)
      val userDailySessionIndex = conf.get(ELASTIC_SEARCH_USER_DAILY_SESSION_INDEX)
      val dailyOpIndex = conf.get(ELASTIC_SEARCH_DAILY_OPERATION_INDEX)
      val userDailyOpIndex = conf.get(ELASTIC_SEARCH_USER_DAILY_OPERATION_INDEX)

      val task = new Runnable {
        override def run(): Unit = {
          try {
            Seq(
              (dailySessionIndex, KYUUBI_SESSION_DAILY_TREND_MAPPING_NAME),
              (userDailySessionIndex, KYUUBI_USER_SESSION__DAILY_TREND_MAPPING_NAME),
              (dailyOpIndex, KYUUBI_OPERATION_DAILY_TREND_MAPPING_NAME),
              (userDailyOpIndex, KYUUBI_USER_OPERATION_DAILY_TREND_MAPPING_NAME)).foreach {
              case (index, mapping) =>
                if (!ElasticsearchUtils.indexExists(index)) {
                  ElasticsearchUtils.createIndex(index, eventMapping.getOrElse(mapping, ""))
                }
            }
            val currentTime = System.currentTimeMillis()
            val currentDayTime =
              EventDoc.dateFormat.parse(EventDoc.dateFormat.format(currentTime)).getTime

            Seq(
              (sessionEventAlias, sessionEventIndex, dailySessionIndex, "user", false),
              (sessionEventAlias, sessionEventIndex, userDailySessionIndex, "user", true),
              (operationEventAlias, operationEventIndex, dailyOpIndex, "sessionUser", false),
              (
                operationEventAlias,
                operationEventIndex,
                userDailyOpIndex,
                "sessionUser",
                true)).foreach {
              case (eventAlias, eventIndex, aggIndex, userFiled, aggByUser) =>
                aggregateClusterEvent(
                  eventAlias,
                  eventIndex,
                  currentDayTime,
                  aggDays,
                  aggSize,
                  aggMinDocCount,
                  aggIndex,
                  userFiled,
                  aggByUser)
            }
          } catch {
            case e: Throwable => error("Error aggregating the elasticsearch events", e)
          }
        }
      }

      ThreadUtils.newDaemonSingleThreadScheduledExecutor(
        "elasticsearch-events-aggregator").scheduleWithFixedDelay(
        task,
        0,
        aggInterval,
        TimeUnit.MILLISECONDS)
    }
  }

  def aggregateClusterEvent(
      eventAlias: String,
      eventIndex: String,
      currentDayTime: Long,
      aggDays: Int,
      aggSize: Int,
      aggMinDocCount: Int,
      aggIndex: String,
      userField: String,
      aggByUser: Boolean): Unit = {
    val minTime = currentDayTime - Duration.ofDays(aggDays).toMillis
    val indexes = ElasticsearchUtils.getAliasIndexes(eventAlias)

    val prefix = eventIndex + EventDoc.indexDelimiter
    indexes.filter(_.startsWith(prefix)).foreach { index =>
      val partitionDay = index.stripPrefix(prefix)
      try {
        if (EventDoc.dateFormat.parse(partitionDay).getTime >= minTime) {
          val dayIndex = s"$eventIndex${EventDoc.indexDelimiter}$partitionDay"
          val aggResult =
            ElasticsearchUtils.getCountAggregation(
              dayIndex,
              userField,
              aggByUser,
              aggSize,
              aggMinDocCount)
          val docsToUpdate = aggResult.map { result =>
            val doc = if (aggByUser) {
              UserDailyTrendEvent(
                result.user,
                partitionDay,
                result.cluster,
                result.count,
                result.sessionTypeCounts)
            } else {
              DailyTrendEvent(
                partitionDay,
                result.cluster,
                result.queue,
                result.count,
                result.clusterUserCount,
                result.queueUserCount,
                result.sessionTypeCounts)
            }
            (aggIndex, doc.docId, doc.toJson)
          }
          if (docsToUpdate.nonEmpty) {
            ElasticsearchUtils.bulkUpdate(docsToUpdate)
          }
        }
      } catch {
        case e: Throwable => error(s"Error aggregating index[$aggIndex]/$partitionDay", e)
      }
    }
  }
}

object ElasticsearchEventHandler {
  final val mapper = new ObjectMapper().registerModule(DefaultScalaModule)
  final val KYUUBI_EVENT_MAPPING_RESOURCE = "kyuubi-event-mapping.json"
  final val KYUUBI_SERVER_EVENT_MAPPING_NAME = "kyuubi-server-event"
  final val KYUUBI_SESSION_EVENT_MAPPING_NAME = "kyuubi-session-event"
  final val KYUUBI_OPERATION_EVENT_MAPPING_NAME = "kyuubi-operation-event"

  final val KYUUBI_SESSION_DAILY_TREND_MAPPING_NAME = "session-daily-trend-event"
  final val KYUUBI_USER_SESSION__DAILY_TREND_MAPPING_NAME = "user-session-daily-trend-event"
  final val KYUUBI_OPERATION_DAILY_TREND_MAPPING_NAME = "operation-daily-trend-event"
  final val KYUUBI_USER_OPERATION_DAILY_TREND_MAPPING_NAME = "user-operation-daily-trend-event"

  final lazy val eventMapping: Map[String, String] = {
    val source = getClass.getClassLoader.getResourceAsStream(KYUUBI_EVENT_MAPPING_RESOURCE)
    val eventMapping = mapper.readTree(source)
    Map(
      KYUUBI_SERVER_EVENT_MAPPING_NAME -> eventMapping.get(
        KYUUBI_SERVER_EVENT_MAPPING_NAME).toString,
      KYUUBI_SESSION_EVENT_MAPPING_NAME -> eventMapping.get(
        KYUUBI_SESSION_EVENT_MAPPING_NAME).toString,
      KYUUBI_OPERATION_EVENT_MAPPING_NAME -> eventMapping.get(
        KYUUBI_OPERATION_EVENT_MAPPING_NAME).toString,
      KYUUBI_SESSION_DAILY_TREND_MAPPING_NAME -> eventMapping.get(
        KYUUBI_SESSION_DAILY_TREND_MAPPING_NAME).toString,
      KYUUBI_USER_SESSION__DAILY_TREND_MAPPING_NAME -> eventMapping.get(
        KYUUBI_USER_SESSION__DAILY_TREND_MAPPING_NAME).toString,
      KYUUBI_OPERATION_DAILY_TREND_MAPPING_NAME -> eventMapping.get(
        KYUUBI_OPERATION_DAILY_TREND_MAPPING_NAME).toString,
      KYUUBI_USER_OPERATION_DAILY_TREND_MAPPING_NAME -> eventMapping.get(
        KYUUBI_USER_OPERATION_DAILY_TREND_MAPPING_NAME).toString)
  }
}
