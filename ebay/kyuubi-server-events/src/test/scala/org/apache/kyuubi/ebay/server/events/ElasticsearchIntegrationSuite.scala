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

import java.util.UUID

import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.tags.Slow
import org.scalatest.time.SpanSugar._

import org.apache.kyuubi.{BatchTestHelper, WithKyuubiServer}
import org.apache.kyuubi.config.{KyuubiConf, KyuubiEbayConf}
import org.apache.kyuubi.ebay.ChainedSessionConfAdvisor
import org.apache.kyuubi.ebay.server.events.doc.EventDoc
import org.apache.kyuubi.events.{EventBus, KyuubiOperationEvent, KyuubiServerInfoEvent, KyuubiSessionEvent}
import org.apache.kyuubi.operation.{ExecuteStatement, HiveJDBCTestHelper}
import org.apache.kyuubi.service.ServiceState
import org.apache.kyuubi.session.{KyuubiSessionImpl, KyuubiSessionManager, SessionType}

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
  protected val indexUUID = UUID.randomUUID().toString
  override protected val conf: KyuubiConf = {
    KyuubiConf().set(KyuubiConf.ENGINE_SHARE_LEVEL, "connection")
      .set(KyuubiEbayConf.SESSION_CLUSTER_MODE_ENABLED, true)
      .set(KyuubiEbayConf.SESSION_CLUSTER, "test")
      .set(KyuubiEbayConf.OPERATION_INTERCEPT_ENABLED, true)
      .set(KyuubiEbayConf.SESSION_CLUSTER_LIST, Seq("test"))
      .set(KyuubiConf.SESSION_CONF_ADVISOR, classOf[ChainedSessionConfAdvisor].getName)
      .set(KyuubiConf.SERVER_EVENT_LOGGERS, Seq("CUSTOM"))
      .set(KyuubiEbayConf.ELASTIC_SEARCH_SERVER_EVENT_INDEX, s"$indexUUID-server-index")
      .set(KyuubiEbayConf.ELASTIC_SEARCH_SESSION_EVENT_INDEX, s"$indexUUID-session-index")
      .set(KyuubiEbayConf.ELASTIC_SEARCH_OPERATION_EVENT_INDEX, s"$indexUUID-operation-index")
      .set(KyuubiEbayConf.ELASTIC_SEARCH_SERVER_EVENT_ALIAS, s"$indexUUID-server-alias")
      .set(KyuubiEbayConf.ELASTIC_SEARCH_SESSION_EVENT_ALIAS, s"$indexUUID-session-alias")
      .set(KyuubiEbayConf.ELASTIC_SEARCH_OPERATION_EVENT_ALIAS, s"$indexUUID-operation-alias")
      .set(KyuubiEbayConf.ELASTIC_SEARCH_AGG_ENABLED, true)
      .set(KyuubiEbayConf.ELASTIC_SEARCH_AGG_INTERVAL, 1000L)
  }

  test("test server/session/operations events") {
    val sessionMgr = server.backendService.sessionManager.asInstanceOf[KyuubiSessionManager]
    withJdbcStatement() { statement =>
      val serverEvent = KyuubiServerInfoEvent(server, ServiceState.STARTED).get
      val serverEventDoc = EventDoc(serverEvent)
      val serverDoc = ElasticsearchUtils.getDocById(
        serverEventDoc.formatPartitionIndex(
          conf.get(KyuubiEbayConf.ELASTIC_SEARCH_SERVER_EVENT_INDEX)),
        serverEventDoc.docId)
      assert(serverDoc.get("serverIP") === serverEvent.serverIP)
      assert(serverDoc.get("state") === serverEvent.state)

      statement.executeQuery("select 123")
      val session = sessionMgr.allSessions().head.asInstanceOf[KyuubiSessionImpl]
      var sessionId = session.handle.identifier.toString
      var sessionEvent = KyuubiSessionEvent(session)
      var sessionEventDoc = EventDoc(sessionEvent)
      var sessionDoc = ElasticsearchUtils.getDocById(
        serverEventDoc.formatPartitionIndex(
          conf.get(KyuubiEbayConf.ELASTIC_SEARCH_SESSION_EVENT_INDEX)),
        sessionId)
      assert(sessionDoc.get("sessionId") === sessionId)
      assert(sessionDoc.get("sessionType") === SessionType.INTERACTIVE.toString)
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
      var opDoc = ElasticsearchUtils.getDocById(
        operationEventDoc.formatPartitionIndex(conf.get(
          KyuubiEbayConf.ELASTIC_SEARCH_OPERATION_EVENT_INDEX)),
        opId)
      assert(opDoc.get("sessionId") === sessionId)
      assert(opDoc.get("statementId") === opId)
      assert(opDoc.get("statement") === "select 123")
      assert(opDoc.get("sessionType") === SessionType.INTERACTIVE.toString)

      val batchRequest = newSparkBatchRequest()
      val batchSessionHandle = sessionMgr.openBatchSession(
        "kyuubi",
        "kyuubi",
        "127.0.0.1",
        batchRequest)
      val batchSession = sessionMgr.getBatchSession(batchSessionHandle).get
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
      sessionDoc =
        ElasticsearchUtils.getDocById(
          sessionEventDoc.formatPartitionIndex(
            conf.get(KyuubiEbayConf.ELASTIC_SEARCH_SESSION_EVENT_INDEX)),
          sessionId)
      assert(sessionDoc.get("sessionId") === sessionId)
      assert(sessionDoc.get("sessionType") === "BATCH")

      operationEventDoc = EventDoc(operationEvent)
      opId = batchOperation.getHandle.identifier.toString
      opDoc = ElasticsearchUtils.getDocById(
        operationEventDoc.formatPartitionIndex(
          conf.get(KyuubiEbayConf.ELASTIC_SEARCH_OPERATION_EVENT_INDEX)),
        opId)
      assert(opDoc.get("sessionId") === batchSessionHandle.identifier.toString)
      assert(opDoc.get("statementId") === opId)
      assert(opDoc.get("statement") === "BatchJobSubmission")
      assert(opDoc.get("sessionType") === SessionType.BATCH.toString)
    }

    // test aggregation
    val sessionEventIndex = ElasticsearchUtils.getAliasIndexes(
      conf.get(KyuubiEbayConf.ELASTIC_SEARCH_SESSION_EVENT_ALIAS)).head
    val clusterSessionAggRes =
      ElasticsearchUtils.getCountAggregation(sessionEventIndex, "user", false)
    val userSessionAggRes =
      ElasticsearchUtils.getCountAggregation(sessionEventIndex, "user", true)
    val operationEventIndex = ElasticsearchUtils.getAliasIndexes(
      conf.get(KyuubiEbayConf.ELASTIC_SEARCH_OPERATION_EVENT_ALIAS)).head
    val clusterOpAggRes =
      ElasticsearchUtils.getCountAggregation(operationEventIndex, "sessionUser", false)
    val userOpAggRes =
      ElasticsearchUtils.getCountAggregation(operationEventIndex, "sessionUser", true)

    assert(clusterSessionAggRes.nonEmpty)
    assert(clusterOpAggRes.nonEmpty)
    assert(userSessionAggRes.nonEmpty)
    assert(userOpAggRes.nonEmpty)

    clusterSessionAggRes.forall(_.cluster.nonEmpty)
    userSessionAggRes.forall(_.user.nonEmpty)
    clusterSessionAggRes.exists { res =>
      res.cluster.nonEmpty && res.count > 0 && res.sessionTypeCounts.forall(
        _._2 > 0) && res.clusterUserCount > 0
    }
    userSessionAggRes.exists { res =>
      res.cluster.nonEmpty && res.count > 0 && res.sessionTypeCounts.forall(_._2 > 0) &&
      res.user.nonEmpty
    }

    clusterOpAggRes.forall(_.cluster.nonEmpty)
    userOpAggRes.forall(_.user.nonEmpty)
    clusterOpAggRes.exists { res =>
      res.cluster.nonEmpty && res.count > 0 && res.sessionTypeCounts.forall(_._2 > 0) &&
      res.clusterUserCount > 0
    }
    userOpAggRes.exists { res =>
      res.cluster.nonEmpty && res.count > 0 && res.sessionTypeCounts.forall(_._2 > 0) &&
      res.user.nonEmpty
    }
    // scalastyle:off println
    println(sessionEventIndex)
    println(clusterSessionAggRes.mkString("", "\n", "\n"))
    println(s"$sessionEventIndex/user")
    println(userSessionAggRes.mkString("", "\n", "\n"))
    println(operationEventIndex)
    println(clusterOpAggRes.mkString("", "\n", "\n"))
    println(s"$operationEventIndex/user")
    println(userOpAggRes.mkString("", "\n", "\n"))
    // scalastyle:on println

    val dailySession = conf.get(KyuubiEbayConf.ELASTIC_SEARCH_DAILY_SESSION_INDEX)
    val userDailySession = conf.get(KyuubiEbayConf.ELASTIC_SEARCH_USER_DAILY_SESSION_INDEX)
    val dailyOp = conf.get(KyuubiEbayConf.ELASTIC_SEARCH_DAILY_OPERATION_INDEX)
    val userDailyOp = conf.get(KyuubiEbayConf.ELASTIC_SEARCH_USER_DAILY_OPERATION_INDEX)
    eventually(Timeout(1.minutes)) {
      val dailySessionTrend = ElasticsearchUtils.getAllDocs(dailySession)
      val userDailySessionTrend = ElasticsearchUtils.getAllDocs(userDailySession)
      val dailyOpTrend = ElasticsearchUtils.getAllDocs(dailyOp)
      val userDailyOpTrend = ElasticsearchUtils.getAllDocs(userDailyOp)

      assert(dailySessionTrend.nonEmpty)
      assert(userDailySessionTrend.nonEmpty)
      assert(dailyOpTrend.nonEmpty)
      assert(userDailyOpTrend.nonEmpty)

      // scalastyle:off println
      println(dailySession)
      println(dailySessionTrend.mkString("", "\n", "\n"))
      println(userDailySession)
      println(userDailySessionTrend.mkString("", "\n", "\n"))
      println(dailyOp)
      println(dailyOpTrend.mkString("", "\n", "\n"))
      println(userDailyOp)
      println(userDailyOpTrend.mkString("", "\n", "\n"))
      // scalastyle:on println
    }

    assert(ElasticsearchUtils.getAliasIndexes("non_exists_" + UUID.randomUUID()).isEmpty)

    val serverEventIndexes = ElasticsearchUtils.getAliasIndexes(
      conf.get(KyuubiEbayConf.ELASTIC_SEARCH_SERVER_EVENT_ALIAS))
    val sessionEventIndexes = ElasticsearchUtils.getAliasIndexes(
      conf.get(KyuubiEbayConf.ELASTIC_SEARCH_SESSION_EVENT_ALIAS))
    val operationEventIndexes = ElasticsearchUtils.getAliasIndexes(
      conf.get(KyuubiEbayConf.ELASTIC_SEARCH_OPERATION_EVENT_ALIAS))

    assert(serverEventIndexes.nonEmpty)
    assert(sessionEventIndexes.nonEmpty)
    assert(operationEventIndexes.nonEmpty)

    serverEventIndexes.foreach(showAndCleanup)
    sessionEventIndexes.foreach(showAndCleanup)
    operationEventIndexes.foreach(showAndCleanup)

    showAndCleanup(conf.get(KyuubiEbayConf.ELASTIC_SEARCH_DAILY_SESSION_INDEX))
    showAndCleanup(conf.get(KyuubiEbayConf.ELASTIC_SEARCH_USER_DAILY_SESSION_INDEX))
    showAndCleanup(conf.get(KyuubiEbayConf.ELASTIC_SEARCH_DAILY_OPERATION_INDEX))
    showAndCleanup(conf.get(KyuubiEbayConf.ELASTIC_SEARCH_USER_DAILY_OPERATION_INDEX))
  }

  def showAndCleanup(index: String): Unit = {
    // scalastyle:off println
    println(ElasticsearchUtils.getAllDocs(index).mkString(s"$index:\n", "\n", "\n"))
    // scalastyle:on println
    ElasticsearchUtils.deleteIndex(index)
    assert(!ElasticsearchUtils.indexExists(index))
  }
}

class ElasticsearchIntegrationNonClusterModeSuite extends ElasticsearchIntegrationSuite {
  override protected val conf: KyuubiConf = {
    KyuubiConf().set(KyuubiConf.ENGINE_SHARE_LEVEL, "connection")
      .set(KyuubiEbayConf.SESSION_CLUSTER_MODE_ENABLED, false)
      .set(KyuubiEbayConf.SESSION_CLUSTER, "test")
      .set(KyuubiEbayConf.OPERATION_INTERCEPT_ENABLED, true)
      .set(KyuubiConf.SESSION_CONF_ADVISOR, classOf[ChainedSessionConfAdvisor].getName)
      .set(KyuubiConf.SERVER_EVENT_LOGGERS, Seq("CUSTOM"))
      .set(KyuubiEbayConf.ELASTIC_SEARCH_SERVER_EVENT_INDEX, s"$indexUUID-server-index")
      .set(KyuubiEbayConf.ELASTIC_SEARCH_SESSION_EVENT_INDEX, s"$indexUUID-session-index")
      .set(KyuubiEbayConf.ELASTIC_SEARCH_OPERATION_EVENT_INDEX, s"$indexUUID-operation-index")
      .set(KyuubiEbayConf.ELASTIC_SEARCH_SERVER_EVENT_ALIAS, s"$indexUUID-server-alias")
      .set(KyuubiEbayConf.ELASTIC_SEARCH_SESSION_EVENT_ALIAS, s"$indexUUID-session-alias")
      .set(KyuubiEbayConf.ELASTIC_SEARCH_OPERATION_EVENT_ALIAS, s"$indexUUID-operation-alias")
      .set(KyuubiEbayConf.ELASTIC_SEARCH_AGG_ENABLED, true)
      .set(KyuubiEbayConf.ELASTIC_SEARCH_AGG_INTERVAL, 1000L)
  }
}
