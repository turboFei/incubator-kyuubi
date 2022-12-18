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

import org.scalatest.tags.Slow

import org.apache.kyuubi.{BatchTestHelper, WithKyuubiServer}
import org.apache.kyuubi.config.{KyuubiConf, KyuubiEbayConf}
import org.apache.kyuubi.ebay.TagBasedSessionConfAdvisor
import org.apache.kyuubi.ebay.server.events.doc.EventDoc
import org.apache.kyuubi.events.{EventBus, KyuubiOperationEvent, KyuubiServerInfoEvent, KyuubiSessionEvent}
import org.apache.kyuubi.operation.{ExecuteStatement, HiveJDBCTestHelper}
import org.apache.kyuubi.service.ServiceState
import org.apache.kyuubi.session.{KyuubiSessionImpl, KyuubiSessionManager}

/**
 * A local elastic search environment is needed.
 */
@Slow
class ElasticsearchIntegrationSuite extends WithKyuubiServer with HiveJDBCTestHelper
  with BatchTestHelper {
  System.setProperty(
    KyuubiEbayConf.ELASTIC_SEARCH_CREDENTIAL_FILE.key,
    getClass.getClassLoader.getResource("elasticsearch.md").getFile)
  override protected def jdbcUrl: String = getJdbcUrl
  override protected val conf: KyuubiConf = {
    KyuubiConf().set(KyuubiConf.ENGINE_SHARE_LEVEL, "connection")
      .set(KyuubiEbayConf.SESSION_CLUSTER_MODE_ENABLED, true)
      .set(KyuubiEbayConf.SESSION_CLUSTER, "test")
      .set(KyuubiEbayConf.OPERATION_INTERCEPT_ENABLED, true)
      .set(KyuubiEbayConf.SESSION_CLUSTER_LIST, Seq("test"))
      .set(KyuubiConf.SESSION_CONF_ADVISOR, classOf[TagBasedSessionConfAdvisor].getName)
      .set(KyuubiConf.SERVER_EVENT_LOGGERS, Seq("CUSTOM"))
  }

  test("test server/session/operations events") {
    val sessionMgr = server.backendService.sessionManager.asInstanceOf[KyuubiSessionManager]
    withJdbcStatement() { statement =>
      val serverEvent = KyuubiServerInfoEvent(server, ServiceState.STARTED).get
      val serverEventDoc = EventDoc(serverEvent)
      val serverDoc = ElasticsearchUtils.getDoc(
        conf.get(KyuubiEbayConf.ELASTIC_SEARCH_SERVER_EVENT_INDEX),
        serverEventDoc.docId)
      assert(serverDoc.get("serverIP") === serverEvent.serverIP)
      assert(serverDoc.get("state") === serverEvent.state)

      statement.executeQuery("select 123")
      val session = sessionMgr.allSessions().head.asInstanceOf[KyuubiSessionImpl]
      var sessionId = session.handle.identifier.toString
      var sessionEvent = KyuubiSessionEvent(session)
      var sessionEventDoc = EventDoc(sessionEvent)
      var sessionDoc = ElasticsearchUtils.getDoc(
        conf.get(KyuubiEbayConf.ELASTIC_SEARCH_SESSION_EVENT_INDEX) + sessionEventDoc.indexSuffix,
        sessionId)
      assert(sessionDoc.get("sessionId") === sessionId)
      assert(sessionDoc.get("sessionType") === "SQL")
      assert(sessionDoc.get("engineId").asInstanceOf[String].startsWith("local-"))

      val operation = sessionMgr.operationManager.newExecuteStatementOperation(
        session,
        "select 123",
        Map.empty,
        true,
        0).asInstanceOf[ExecuteStatement]
      var operationEvent = KyuubiOperationEvent(operation)
      var operationEventDoc = EventDoc(operationEvent)
      var opId = operation.getHandle.identifier.toString
      var opDoc = ElasticsearchUtils.getDoc(
        conf.get(KyuubiEbayConf.ELASTIC_SEARCH_OPERATION_EVENT_INDEX) +
          operationEventDoc.indexSuffix,
        opId)
      assert(opDoc.get("sessionId") === sessionId)
      assert(opDoc.get("statementId") === opId)
      assert(opDoc.get("statement") == "select 123")
      assert(opDoc.get("operationType") === "SQL")

      val batchRequest = newSparkBatchRequest()
      val batchSessionHandle = sessionMgr.openBatchSession(
        "kyuubi",
        "kyuubi",
        "127.0.0.1",
        Map.empty,
        batchRequest)
      val batchSession = sessionMgr.getBatchSessionImpl(batchSessionHandle)
      val batchOperation = sessionMgr.operationManager.newBatchJobSubmissionOperation(
        batchSession,
        batchRequest.getBatchType,
        batchRequest.getName,
        batchRequest.getResource,
        batchRequest.getClassName,
        Map.empty,
        Seq.empty,
        None)
      operationEvent = KyuubiOperationEvent(batchOperation)
      sessionEvent = KyuubiSessionEvent(batchSession)
      EventBus.post(sessionEvent)
      EventBus.post(operationEvent)
      sessionEventDoc = EventDoc(sessionEvent)
      sessionId = batchSessionHandle.identifier.toString
      sessionDoc = ElasticsearchUtils.getDoc(
        conf.get(KyuubiEbayConf.ELASTIC_SEARCH_SESSION_EVENT_INDEX) + sessionEventDoc.indexSuffix,
        sessionId)
      assert(sessionDoc.get("sessionId") === sessionId)
      assert(sessionDoc.get("sessionType") === "BATCH")

      operationEventDoc = EventDoc(operationEvent)
      opId = batchOperation.getHandle.identifier.toString
      opDoc = ElasticsearchUtils.getDoc(
        conf.get(KyuubiEbayConf.ELASTIC_SEARCH_OPERATION_EVENT_INDEX) +
          operationEventDoc.indexSuffix,
        opId)
      assert(opDoc.get("sessionId") === batchSessionHandle.identifier.toString)
      assert(opDoc.get("statementId") === opId)
      assert(opDoc.get("statement") === "BatchJobSubmission")
      assert(opDoc.get("operationType") === "BATCH")
    }
  }
}
