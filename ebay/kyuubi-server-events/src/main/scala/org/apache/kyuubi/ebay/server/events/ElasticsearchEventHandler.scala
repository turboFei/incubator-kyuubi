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

import java.util.concurrent.ConcurrentHashMap

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.{KyuubiConf, KyuubiEbayConf}
import org.apache.kyuubi.ebay.server.events.doc.{EventDoc, OperationEventDoc, ServerEventDoc, SessionEventDoc}
import org.apache.kyuubi.events.KyuubiEvent
import org.apache.kyuubi.events.handler.EventHandler

class ElasticsearchEventHandler(conf: KyuubiConf) extends EventHandler[KyuubiEvent] with Logging {
  import ElasticsearchEventHandler._

  private val sessionEventIndex = conf.get(KyuubiEbayConf.ELASTIC_SEARCH_SESSION_EVENT_INDEX)
  private val operationEventIndex = conf.get(KyuubiEbayConf.ELASTIC_SEARCH_OPERATION_EVENT_INDEX)
  private val serverEventIndex = conf.get(KyuubiEbayConf.ELASTIC_SEARCH_SERVER_EVENT_INDEX)
  private val indexSet = ConcurrentHashMap.newKeySet[String]()

  private def getEventDocIndex(eventDoc: EventDoc): String = {
    val (index, mapping) = eventDoc match {
      case doc: ServerEventDoc =>
        serverEventIndex + doc.indexSuffix ->
          eventMapping.getOrElse(KYUUBI_SERVER_EVENT_MAPPING_NAME, "")
      case doc: SessionEventDoc =>
        sessionEventIndex + doc.indexSuffix ->
          eventMapping.getOrElse(KYUUBI_SESSION_EVENT_MAPPING_NAME, "")
      case doc: OperationEventDoc =>
        operationEventIndex + doc.indexSuffix ->
          eventMapping.getOrElse(KYUUBI_OPERATION_EVENT_MAPPING_NAME, "")
    }
    if (!indexSet.contains(index)) {
      if (!ElasticsearchUtils.indexExists(index)) {
        ElasticsearchUtils.createIndex(index, mapping)
      }
      indexSet.add(index)
    }
    index
  }

  override def apply(event: KyuubiEvent): Unit = {
    val eventDoc = EventDoc(event)
    ElasticsearchUtils.createDoc(getEventDocIndex(eventDoc), eventDoc.docId, eventDoc.toJson)
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
