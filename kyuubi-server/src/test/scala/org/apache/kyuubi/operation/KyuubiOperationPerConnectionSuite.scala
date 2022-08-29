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

package org.apache.kyuubi.operation

import java.sql.SQLException
import java.util
import java.util.Properties

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import org.apache.hive.service.rpc.thrift._
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

import org.apache.kyuubi.{Utils, WithKyuubiServer}
import org.apache.kyuubi.config.{KyuubiConf, KyuubiEbayConf}
import org.apache.kyuubi.config.KyuubiConf.SESSION_CONF_ADVISOR
import org.apache.kyuubi.jdbc.KyuubiHiveDriver
import org.apache.kyuubi.jdbc.hive.KyuubiConnection
import org.apache.kyuubi.jdbc.hive.logs.KyuubiEngineLogListener
import org.apache.kyuubi.plugin.SessionConfAdvisor
import org.apache.kyuubi.service.authentication.KyuubiAuthenticationFactory

/**
 * UT with Connection level engine shared cost much time, only run basic jdbc tests.
 */
class KyuubiOperationPerConnectionSuite extends WithKyuubiServer with HiveJDBCTestHelper {

  override protected def jdbcUrl: String =
    s"jdbc:kyuubi://${server.frontendServices.head.connectionUrl}/;"
  override protected val URL_PREFIX: String = "jdbc:kyuubi://"

  override protected val conf: KyuubiConf = {
    KyuubiConf().set(KyuubiConf.ENGINE_SHARE_LEVEL, "connection")
      .set(SESSION_CONF_ADVISOR.key, classOf[TestSessionConfAdvisor].getName)
  }

  test("KYUUBI #647 - async query causes engine crash") {
    withSessionHandle { (client, handle) =>
      val executeStmtReq = new TExecuteStatementReq()
      executeStmtReq.setStatement("select java_method('java.lang.System', 'exit', 1)")
      executeStmtReq.setSessionHandle(handle)
      executeStmtReq.setRunAsync(true)
      val executeStmtResp = client.ExecuteStatement(executeStmtReq)

      // TODO KYUUBI #745
      eventually(timeout(60.seconds), interval(500.milliseconds)) {
        val getOpStatusReq = new TGetOperationStatusReq(executeStmtResp.getOperationHandle)
        val getOpStatusResp = client.GetOperationStatus(getOpStatusReq)
        assert(getOpStatusResp.getStatus.getStatusCode === TStatusCode.SUCCESS_STATUS)
        assert(getOpStatusResp.getOperationState === TOperationState.ERROR_STATE)
      }
    }
  }

  test("sync query causes engine crash") {
    withSessionHandle { (client, handle) =>
      val executeStmtReq = new TExecuteStatementReq()
      executeStmtReq.setStatement("select java_method('java.lang.System', 'exit', 1)")
      executeStmtReq.setSessionHandle(handle)
      executeStmtReq.setRunAsync(false)
      val executeStmtResp = client.ExecuteStatement(executeStmtReq)
      assert(executeStmtResp.getStatus.getStatusCode === TStatusCode.ERROR_STATUS)
      assert(executeStmtResp.getOperationHandle === null)
      assert(executeStmtResp.getStatus.getErrorMessage contains
        "Caused by: java.net.SocketException: Connection reset")
    }
  }

  test("test asynchronous open kyuubi session") {
    withSessionConf(Map(
      KyuubiConf.SESSION_ENGINE_LAUNCH_ASYNC.key -> "true"))(Map.empty)(Map.empty) {
      withSessionAndLaunchEngineHandle { (client, handle, launchOpHandleOpt) =>
        assert(launchOpHandleOpt.isDefined)
        val launchOpHandle = launchOpHandleOpt.get
        val executeStmtReq = new TExecuteStatementReq
        executeStmtReq.setStatement("select engine_name()")
        executeStmtReq.setSessionHandle(handle)
        executeStmtReq.setRunAsync(false)
        val executeStmtResp = client.ExecuteStatement(executeStmtReq)
        val getOpStatusReq = new TGetOperationStatusReq(executeStmtResp.getOperationHandle)
        val getOpStatusResp = client.GetOperationStatus(getOpStatusReq)
        assert(getOpStatusResp.getStatus.getStatusCode === TStatusCode.SUCCESS_STATUS)
        assert(getOpStatusResp.getOperationState === TOperationState.FINISHED_STATE)

        val launchEngineResp = client.GetOperationStatus(new TGetOperationStatusReq(launchOpHandle))
        assert(launchEngineResp.getStatus.getStatusCode == TStatusCode.SUCCESS_STATUS)
        assert(getOpStatusResp.getOperationState === TOperationState.FINISHED_STATE)
      }
    }
  }

  test("test asynchronous open kyuubi session failure") {
    withSessionConf(Map(
      KyuubiConf.SESSION_ENGINE_LAUNCH_ASYNC.key -> "true",
      "spark.master" -> "invalid"))(Map.empty)(Map.empty) {
      withSessionAndLaunchEngineHandle { (client, handle, launchOpHandleOpt) =>
        assert(launchOpHandleOpt.isDefined)
        val launchOpHandle = launchOpHandleOpt.get
        val executeStmtReq = new TExecuteStatementReq
        executeStmtReq.setStatement("select engine_name()")
        executeStmtReq.setSessionHandle(handle)
        executeStmtReq.setRunAsync(false)
        val executeStmtResp = client.ExecuteStatement(executeStmtReq)
        assert(executeStmtResp.getStatus.getStatusCode == TStatusCode.ERROR_STATUS)
        assert(executeStmtResp.getStatus.getErrorMessage.contains("kyuubi-spark-sql-engine.log"))

        val launchEngineResp = client.GetOperationStatus(new TGetOperationStatusReq(launchOpHandle))
        assert(launchEngineResp.getStatus.getStatusCode == TStatusCode.SUCCESS_STATUS)
        assert(launchEngineResp.getOperationState == TOperationState.ERROR_STATE)
      }
    }
  }

  test("open session with KyuubiConnection") {
    withSessionConf(Map.empty)(Map.empty)(Map(
      KyuubiConf.SESSION_ENGINE_LAUNCH_ASYNC.key -> "true")) {
      val driver = new KyuubiHiveDriver()
      val connection = driver.connect(jdbcUrlWithConf, new Properties())

      val stmt = connection.createStatement()
      try {
        stmt.execute("select engine_name()")
        val resultSet = stmt.getResultSet
        assert(resultSet.next())
        assert(resultSet.getString(1).nonEmpty)
      } finally {
        stmt.close()
        connection.close()
      }
    }

    withSessionConf(Map.empty)(Map.empty)(Map(
      KyuubiConf.SESSION_ENGINE_LAUNCH_ASYNC.key -> "false")) {
      val driver = new KyuubiHiveDriver()
      val connection = driver.connect(jdbcUrlWithConf, new Properties())

      val stmt = connection.createStatement()
      try {
        stmt.execute("select engine_name()")
        val resultSet = stmt.getResultSet
        assert(resultSet.next())
        assert(resultSet.getString(1).nonEmpty)
      } finally {
        stmt.close()
        connection.close()
      }
    }
  }

  test("support to specify OPERATION_LANGUAGE with confOverlay") {
    withSessionHandle { (client, handle) =>
      val executeStmtReq = new TExecuteStatementReq()
      executeStmtReq.setStatement("""spark.sql("SET kyuubi.operation.language").show(false)""")
      executeStmtReq.setSessionHandle(handle)
      executeStmtReq.setRunAsync(false)
      executeStmtReq.setConfOverlay(Map(KyuubiConf.OPERATION_LANGUAGE.key -> "SCALA").asJava)
      val executeStmtResp = client.ExecuteStatement(executeStmtReq)
      assert(executeStmtResp.getStatus.getStatusCode == TStatusCode.SUCCESS_STATUS)

      val tFetchResultsReq = new TFetchResultsReq()
      tFetchResultsReq.setOperationHandle(executeStmtResp.getOperationHandle)
      tFetchResultsReq.setFetchType(0)
      tFetchResultsReq.setMaxRows(10)
      val tFetchResultsResp = client.FetchResults(tFetchResultsReq)
      val resultSet = tFetchResultsResp.getResults.getColumns.asScala
      assert(resultSet.size == 1)
      assert(resultSet.head.getStringVal.getValues.get(0).contains("kyuubi.operation.language"))
    }
  }

  test("test session conf plugin") {
    withSessionConf()(Map())(Map("spark.k1" -> "v0", "spark.k3" -> "v4")) {
      withJdbcStatement() { statement =>
        val r1 = statement.executeQuery("set spark.k1")
        assert(r1.next())
        assert(r1.getString(2) == "v0")

        val r2 = statement.executeQuery("set spark.k3")
        assert(r2.next())
        assert(r2.getString(2) == "v3")

        val r3 = statement.executeQuery("set spark.k4")
        assert(r3.next())
        assert(r3.getString(2) == "v4")
      }
    }
  }

  test("close kyuubi connection on launch engine operation failure") {
    withSessionConf(Map.empty)(Map.empty)(Map(
      KyuubiConf.SESSION_ENGINE_LAUNCH_ASYNC.key -> "true",
      "spark.master" -> "invalid")) {
      val prop = new Properties()
      prop.setProperty(KyuubiConnection.BEELINE_MODE_PROPERTY, "true")
      val kyuubiConnection = new KyuubiConnection(jdbcUrlWithConf, prop, null)
      intercept[SQLException](kyuubiConnection.waitLaunchEngineToComplete())
      assert(kyuubiConnection.isClosed)
    }
  }

  test("without session cluster mode enabled, the session cluster doest not valid") {
    withSessionConf(Map.empty)(Map.empty)(Map(
      KyuubiEbayConf.SESSION_CLUSTER.key -> "test")) {
      withJdbcStatement() { statement =>
        val rs = statement.executeQuery("set spark.sql.kyuubi.session.cluster.test")
        assert(rs.next())
        assert(rs.getString(2).equals("<undefined>"))
      }
    }
  }

  test("test proxy batch account with unsupported exception") {
    withSessionConf(Map(
      KyuubiAuthenticationFactory.KYUUBI_PROXY_BATCH_ACCOUNT -> "b_stf"))(Map.empty)(Map.empty) {
      val exception = intercept[SQLException] {
        withJdbcStatement() { _ => // no-op
        }
      }
      val verboseMessage = Utils.stringifyException(exception)
      assert(verboseMessage.contains("batch account proxy is not supported"))
    }
  }

  test("HADP-44732: kyuubi engine log listener") {
    val engineLogs = ListBuffer[String]()

    val engineLogListener = new KyuubiEngineLogListener {

      override def onListenerRegistered(kyuubiConnection: KyuubiConnection): Unit = {}

      override def onLogFetchSuccess(list: util.List[String]): Unit = {
        engineLogs ++= list.asScala
      }

      override def onLogFetchFailure(throwable: Throwable): Unit = {}
    }
    withSessionConf()(Map.empty)(Map(
      KyuubiConf.SESSION_ENGINE_LAUNCH_ASYNC.key -> "true")) {
      val conn = new KyuubiConnection(jdbcUrlWithConf, new Properties(), engineLogListener)
      assert(engineLogs.nonEmpty)
      conn.close()
    }
  }

  test("HADP-44631: get full spark url") {
    withSessionConf()(Map.empty)(Map(
      "spark.ui.enabled" -> "true")) {
      withJdbcStatement() { statement =>
        val conn = statement.getConnection.asInstanceOf[KyuubiConnection]
        val sparkUrl = conn.getSparkURL
        assert(sparkUrl.nonEmpty)
      }
    }
  }
}

class TestSessionConfAdvisor extends SessionConfAdvisor {
  override def getConfOverlay(
      user: String,
      sessionConf: util.Map[String, String]): util.Map[String, String] = {
    Map("spark.k3" -> "v3", "spark.k4" -> "v4").asJava
  }
}
