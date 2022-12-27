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

import scala.collection.JavaConverters._

import org.apache.kyuubi.WithKyuubiServer
import org.apache.kyuubi.config.{KyuubiConf, KyuubiEbayConf}
import org.apache.kyuubi.ebay.server.events.doc.EventDoc
import org.apache.kyuubi.events.{KyuubiOperationEvent, KyuubiServerInfoEvent, KyuubiSessionEvent}
import org.apache.kyuubi.operation.{ExecuteStatement, HiveJDBCTestHelper}
import org.apache.kyuubi.service.ServiceState
import org.apache.kyuubi.session.{KyuubiSessionImpl, KyuubiSessionManager}

class ElasticsearchEventHandlerSuite extends WithKyuubiServer with HiveJDBCTestHelper {
  import ElasticsearchEventHandler._

  override protected def jdbcUrl: String = getJdbcUrl
  override protected val conf: KyuubiConf = {
    KyuubiConf().set(KyuubiConf.ENGINE_SHARE_LEVEL, "connection")
      .set(KyuubiEbayConf.SESSION_CLUSTER_MODE_ENABLED, true)
      .set(KyuubiEbayConf.SESSION_CLUSTER, "test")
      .set(KyuubiEbayConf.OPERATION_INTERCEPT_ENABLED, true)
      .set(KyuubiEbayConf.SESSION_CLUSTER_LIST, Seq("test"))
  }

  test("event mapping non empty") {
    assert(eventMapping.get(KYUUBI_SERVER_EVENT_MAPPING_NAME).nonEmpty)
    assert(eventMapping.get(KYUUBI_SESSION_EVENT_MAPPING_NAME).nonEmpty)
    assert(eventMapping.get(KYUUBI_OPERATION_EVENT_MAPPING_NAME).nonEmpty)
  }

  test("test server/session/operations events and docs fields") {
    val sessionMgr = server.backendService.sessionManager.asInstanceOf[KyuubiSessionManager]
    withJdbcStatement() { _ =>
      val serverEvent = KyuubiServerInfoEvent(server, ServiceState.STARTED).get
      val serverEventDoc = EventDoc(serverEvent)
      assert(mapper.readTree(serverEvent.toJson).fields().asScala.map(_.getKey).toSet ===
        mapper.readTree(serverEventDoc.toJson).fields().asScala.map(_.getKey).toSet)

      val session = sessionMgr.allSessions().head.asInstanceOf[KyuubiSessionImpl]
      val sessionEvent = KyuubiSessionEvent(session)
      val sessionEventDoc = EventDoc(sessionEvent)
      assert(mapper.readTree(sessionEvent.toJson).fields().asScala.map(_.getKey).toSet ===
        mapper.readTree(sessionEventDoc.toJson).fields().asScala.map(_.getKey).toSet)

      val operation = sessionMgr.operationManager.newExecuteStatementOperation(
        session,
        "select 123",
        Map.empty,
        true,
        0).asInstanceOf[ExecuteStatement]
      val operationEvent = KyuubiOperationEvent(operation)
      val operationEventDoc = EventDoc(operationEvent)
      assert(mapper.readTree(operationEvent.toJson).fields().asScala.map(_.getKey).toSet ===
        mapper.readTree(operationEventDoc.toJson).fields().asScala.map(_.getKey).toSet)
    }
  }
}
