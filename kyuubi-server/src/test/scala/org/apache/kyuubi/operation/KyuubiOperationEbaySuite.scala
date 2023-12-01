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

import java.io.File
import java.net.URI
import java.nio.file.Files
import java.sql.{SQLException, Statement}
import java.util
import java.util.{Base64, Collections, Properties, UUID}
import javax.ws.rs.client.Entity
import javax.ws.rs.core.{GenericType, MediaType}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import org.apache.commons.io.FileUtils
import org.apache.hadoop.shaded.com.nimbusds.jose.util.StandardCharset
import org.apache.hive.service.rpc.thrift.{TDownloadDataReq, TExecuteStatementReq, TFetchResultsReq, TTransferDataReq}
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

import org.apache.kyuubi.{BatchTestHelper, RestFrontendTestHelper, Utils, WithKyuubiServer}
import org.apache.kyuubi.client.api.v1.dto.{SessionData, SessionHandle, SessionOpenRequest}
import org.apache.kyuubi.client.util.BatchUtils
import org.apache.kyuubi.config.{KyuubiConf, KyuubiEbayConf}
import org.apache.kyuubi.config.KyuubiConf.FrontendProtocols
import org.apache.kyuubi.config.KyuubiConf.FrontendProtocols.FrontendProtocol
import org.apache.kyuubi.ebay.{ChainedSessionConfAdvisor, FakeApiKeyAuthenticationProviderImpl}
import org.apache.kyuubi.jdbc.hive.KyuubiConnection
import org.apache.kyuubi.jdbc.hive.logs.KyuubiEngineLogListener
import org.apache.kyuubi.server.http.util.HttpAuthUtils.AUTHORIZATION_HEADER
import org.apache.kyuubi.server.metadata.api.MetadataFilter
import org.apache.kyuubi.server.metadata.jdbc.JDBCMetadataStore
import org.apache.kyuubi.service.authentication.{AuthTypes, KyuubiAuthenticationFactory}
import org.apache.kyuubi.session.{KyuubiBatchSession, KyuubiSessionImpl, KyuubiSessionManager}
import org.apache.kyuubi.util.command.CommandLineUtils

class KyuubiOperationEbaySuite extends WithKyuubiServer with HiveJDBCTestHelper
  with BatchTestHelper with RestFrontendTestHelper {
  override protected val frontendProtocols: Seq[FrontendProtocol] =
    FrontendProtocols.REST :: FrontendProtocols.THRIFT_BINARY :: Nil

  override protected def getJdbcUrl: String = {
    s"jdbc:hive2://${server.frontendServices.last.connectionUrl}/;"
  }
  override protected def jdbcUrl: String = getJdbcUrl

  override protected lazy val conf: KyuubiConf = {
    KyuubiConf().set(KyuubiConf.ENGINE_SHARE_LEVEL, "connection")
      .set(KyuubiEbayConf.SESSION_CLUSTER_MODE_ENABLED, true)
      .set(KyuubiEbayConf.SESSION_CLUSTER, "invalid-cluster")
      .set(KyuubiEbayConf.OPERATION_INTERCEPT_ENABLED, true)
      .set(KyuubiEbayConf.SESSION_CLUSTER_LIST, Seq("test", "cluster_limit"))
      .set(KyuubiConf.SESSION_CONF_ADVISOR.key, classOf[ChainedSessionConfAdvisor].getName)
      .set(KyuubiConf.AUTHENTICATION_METHOD, Set(AuthTypes.CUSTOM.toString))
      .set(
        KyuubiConf.AUTHENTICATION_CUSTOM_CLASS,
        classOf[FakeApiKeyAuthenticationProviderImpl].getName)
      .set("kyuubi.server.redaction.regex", "(?i)secret|pass|token|access[.]key")
  }
  private lazy val jdbcMetadataStore = new JDBCMetadataStore(conf)

  override def beforeAll(): Unit = {
    super.beforeAll()
    System.setProperty(KyuubiEbayConf.SESSION_TESS_SPARK_DRIVER_CORES_DEFAULT.key, "2")
    System.setProperty(KyuubiEbayConf.SESSION_TESS_SPARK_EXECUTOR_CORES_DEFAULT.key, "2")
  }

  override protected def beforeEach(): Unit = {
    FakeApiKeyAuthenticationProviderImpl.reset()
    super.beforeEach()
  }

  override protected def afterEach(): Unit = {
    super.afterEach()
    FakeApiKeyAuthenticationProviderImpl.reset()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    jdbcMetadataStore.getMetadataList(MetadataFilter(), 0, Int.MaxValue).foreach {
      batch =>
        jdbcMetadataStore.cleanupMetadataByIdentifier(batch.identifier)
    }
    jdbcMetadataStore.close()
  }

  test("open session with cluster selector") {
    withSessionConf(Map.empty)(Map.empty)(Map(
      KyuubiEbayConf.SESSION_CLUSTER.key -> "test")) {
      withJdbcStatement() { statement =>
        checkConfigValue(statement, "spark.sql.kyuubi.session.cluster.test", "yes")
      }
    }
  }

  test("open session without cluster selector") {
    withSessionConf(Map.empty)(Map.empty)(Map.empty) {
      val sqlException = intercept[SQLException] {
        withJdbcStatement() { _ =>
        }
      }
      assert(sqlException.getMessage.contains("Please specify the cluster to access"))
      assert(sqlException.getMessage.contains("should be one of"))
    }
  }

  test("open session with invalid cluster selector & proxy user") {
    withSessionConf(Map("hive.server2.proxy.user" -> "proxy_user"))(Map.empty)(Map(
      KyuubiEbayConf.SESSION_CLUSTER.key -> "invalid")) {
      val sqlException = intercept[SQLException] {
        withJdbcStatement() { _ =>
        }
      }
      assert(sqlException.getMessage.contains("should be one of"))
    }
  }

  test("open session with invalid cluster selector") {
    withSessionConf(Map.empty)(Map.empty)(Map(
      KyuubiEbayConf.SESSION_CLUSTER.key -> "invalid")) {
      val sqlException = intercept[SQLException] {
        withJdbcStatement() { _ =>
        }
      }
      assert(sqlException.getMessage.contains("should be one of"))
    }
  }

  test("HADP-44681: The client conf should overwrite the server defined configuration") {
    withSessionConf(Map.empty)(Map.empty)(Map(
      KyuubiEbayConf.SESSION_CLUSTER.key -> "test",
      "spark.sql.kyuubi.session.cluster.test" -> "false")) {
      withJdbcStatement() { statement =>
        checkConfigValue(statement, "spark.sql.kyuubi.session.cluster.test", "false")
      }
    }
  }

  test("HADP-44681: Support cluster user level conf") {
    withSessionConf(Map.empty)(Map.empty)(Map(
      KyuubiEbayConf.SESSION_CLUSTER.key -> "test")) {
      withJdbcStatement() { statement =>
        val rs = statement.executeQuery("set `spark.user.test`")
        assert(rs.next())
        assert(!rs.getString(2).equals("b"))
      }
      withJdbcStatement() { statement =>
        val rs = statement.executeQuery("set `___userb___.spark.user.test`")
        assert(rs.next())
        assert(!rs.getString(2).equals("b"))
      }
    }
  }

  test("HADP-47118: Do not get session cluster from session conf") {
    withSessionConf(Map.empty)(Map.empty)(Map(
      KyuubiEbayConf.SESSION_CLUSTER.key -> "test")) {
      withJdbcStatement() { _ =>
        val session =
          server.backendService.sessionManager.allSessions().head.asInstanceOf[KyuubiSessionImpl]
        assert(session.sessionCluster === Some("test"))
      }
    }
  }

  test("HADP-47172: Intercept the select 1 query in gateway") {
    withSessionConf(Map.empty)(Map.empty)(Map(KyuubiEbayConf.SESSION_CLUSTER.key -> "test")) {
      withJdbcStatement() { statement =>
        val rs = statement.executeQuery("select 1")
        assert(rs.next())
        assert(rs.getInt(1) === 1)
        assert(!rs.next())
      }
    }
  }

  test("support queue variable in jdbc url") {
    withSessionConf(Map.empty)(Map.empty)(Map(
      "queue" -> "yarn_queue",
      KyuubiEbayConf.SESSION_CLUSTER.key -> "test")) {
      withJdbcStatement() { statement =>
        checkConfigValue(statement, "spark.yarn.queue", "yarn_queue")
      }
    }

    withSessionConf(Map.empty)(Map.empty)(Map(
      "queue" -> "yarn_queue",
      "spark.yarn.queue" -> "queue_2",
      KyuubiEbayConf.SESSION_CLUSTER.key -> "test")) {
      withJdbcStatement() { statement =>
        checkConfigValue(statement, "spark.yarn.queue", "queue_2")
      }
    }
  }

  Seq(
    "WITH TEMP_WITH_VIEW AS (SELECT system_user(), session_user()) SELECT * FROM TEMP_WITH_VIEW",
    "SELECT system_user(), session_user()").foreach { stmt =>
    Seq(0, 10 * 1024 * 1024).foreach { minSize =>
      Seq("true", "false").foreach { sortLimitEnabled =>
        test(s"HADP-46611: support to save the result into temp table-" +
          s" $stmt/$minSize/$sortLimitEnabled") {
          withSessionConf(Map.empty)(Map.empty)(Map(
            KyuubiEbayConf.SESSION_CLUSTER.key -> "test",
            KyuubiEbayConf.OPERATION_TEMP_TABLE_DATABASE.key -> "kyuubi",
            KyuubiEbayConf.OPERATION_TEMP_TABLE_COLLECT.key -> "true",
            KyuubiConf.OPERATION_INCREMENTAL_COLLECT.key -> "true",
            KyuubiEbayConf.OPERATION_TEMP_TABLE_COLLECT_FILE_COALESCE_NUM_THRESHOLD.key -> "1",
            KyuubiEbayConf.OPERATION_TEMP_TABLE_COLLECT_SORT_LIMIT_ENABLED.key -> sortLimitEnabled,
            KyuubiEbayConf.OPERATION_TEMP_TABLE_COLLECT_MIN_FILE_SIZE.key -> minSize.toString)) {
            withJdbcStatement() { statement =>
              statement.executeQuery("create database if not exists kyuubi")
              var rs = statement.executeQuery(stmt)
              assert(rs.next())
              assert(rs.getString(1) === Utils.currentUser)
              assert(rs.getString(2) === Utils.currentUser)
              assert(!rs.next())
              // use a new statement to prevent the temp table cleanup
              val statement2 = statement.getConnection.createStatement()
              rs = statement2.executeQuery("show tables in kyuubi")
              assert(rs.next())
              val db = rs.getString(1)
              val tempTableName = rs.getString(2)
              assert(db === "kyuubi")
              assert(tempTableName.startsWith("kyuubi_temp_"))
              assert(!rs.next())
              rs = statement2.executeQuery(s"select * from $db.$tempTableName")
              assert(rs.next())
              assert(rs.getString(1) === Utils.currentUser)
              assert(rs.getString(2) === Utils.currentUser)
              assert(UUID.fromString(rs.getString(3)).isInstanceOf[UUID])
              // cleanup the temp table
              statement.close()
              rs = statement2.executeQuery("show tables in kyuubi")
              assert(!rs.next())
              statement2.executeQuery("create table ta(id int) using parquet")
              statement2.executeQuery("insert into ta(id) values(1),(2)")
              rs = statement2.executeQuery("SELECT * from ta order by id")
              assert(rs.next())
              assert(rs.getInt(1) === 1)
              assert(rs.next())
              assert(rs.getInt(1) === 2)
              assert(!rs.next())
              statement2.executeQuery("drop table ta")
              statement2.executeQuery("drop database kyuubi")
            }
          }
        }
      }
    }
  }

  test("transfer data and download data and upload data command") {
    withSessionConf(Map(KyuubiEbayConf.SESSION_CLUSTER.key -> "test"))(Map())(Map()) {
      withSessionHandle { (client, handle) =>
        val transferDataReq = new TTransferDataReq()
        transferDataReq.setSessionHandle(handle)
        transferDataReq.setValues("test".getBytes("UTF-8"))
        transferDataReq.setPath("test")
        val transferResp = client.TransferData(transferDataReq)
        val fetchResultReq = new TFetchResultsReq()
        fetchResultReq.setOperationHandle(transferResp.getOperationHandle)
        fetchResultReq.setFetchType(0)
        fetchResultReq.setMaxRows(10)
        var fetchResultResp = client.FetchResults(fetchResultReq)
        var results = fetchResultResp.getResults
        val transferredFile = new File(
          new URI(results.getColumns.get(0).getStringVal.getValues.get(0)).getPath)
        assert(transferredFile.exists())
        assert(FileUtils.readFileToString(transferredFile, "UTF-8") === "test")

        val downloadDataReq = new TDownloadDataReq()
        downloadDataReq.setSessionHandle(handle)
        downloadDataReq.setQuery("select 'test' as kyuubi")
        downloadDataReq.setFormat("parquet")
        downloadDataReq.setDownloadOptions(Collections.emptyMap[String, String]())
        val downloadResp = client.DownloadData(downloadDataReq)
        // wait the operation finished, because it is async
        waitForOperationToComplete(client, downloadResp.getOperationHandle)
        fetchResultReq.setOperationHandle(downloadResp.getOperationHandle)
        fetchResultReq.setFetchType(0)
        fetchResultReq.setMaxRows(10)
        fetchResultResp = client.FetchResults(fetchResultReq)
        results = fetchResultResp.getResults

        val col1 = results.getColumns.get(0).getStringVal.getValues // FILE_NAME
        val col2 = results.getColumns.get(1).getBinaryVal.getValues // DATA
        val col3 = results.getColumns.get(2).getStringVal.getValues // SCHEMA
        val col4 = results.getColumns.get(3).getI64Val.getValues // SIZE

        // the first row is schema
        assert(col1.get(0).isEmpty)
        assert(col2.get(0).capacity() === 0)
        assert(col3.get(0) === "kyuubi")
        val dataSize = col4.get(0)

        // the second row is the content
        assert(col1.get(1).endsWith("parquet"))
        assert(col2.get(1).capacity() === dataSize)
        val parquetContent = new String(col2.get(1).array(), "UTF-8")
        assert(parquetContent.startsWith("PAR1") && parquetContent.endsWith("PAR1"))
        assert(col3.get(1).isEmpty)
        assert(col4.get(1) === dataSize)

        // the last row is the EOF
        assert(col1.get(2).endsWith("parquet"))
        assert(col2.get(2).capacity() === 0)
        assert(col3.get(2).isEmpty)
        assert(col4.get(2) === -1)

        // download op metrics
        val metrics = server.backendService.sessionManager.operationManager.allOperations()
          .filter(_.isInstanceOf[DownloadDataOperation])
          .head.asInstanceOf[DownloadDataOperation].metrics
        assert(metrics.get("downloadDataSize").isDefined)
        assert(metrics.get("downloadDataSize").forall(_.toInt > 0))

        // create table ta
        val executeStmtReq = new TExecuteStatementReq()
        executeStmtReq.setStatement("create table ta(c1 string) using parquet")
        executeStmtReq.setSessionHandle(handle)
        executeStmtReq.setRunAsync(false)
        client.ExecuteStatement(executeStmtReq)

        // upload data
        executeStmtReq.setStatement("UPLOAD DATA INPATH 'test' OVERWRITE INTO TABLE ta")
        client.ExecuteStatement(executeStmtReq)

        // check upload result
        executeStmtReq.setStatement("SELECT * FROM ta")
        val executeStatementResp = client.ExecuteStatement(executeStmtReq)

        fetchResultReq.setOperationHandle(executeStatementResp.getOperationHandle)
        fetchResultResp = client.FetchResults(fetchResultReq)
        val resultSet = fetchResultResp.getResults.getColumns.asScala
        assert(resultSet.size == 1)
        assert(resultSet.head.getStringVal.getValues.get(0) === "test")
      }
    }
  }

  test("move data command") {
    withSessionConf(Map(KyuubiEbayConf.SESSION_CLUSTER.key -> "test"))(Map.empty)(Map()) {
      withSessionHandle { (client, handle) =>
        val tempDir = Utils.createTempDir()

        val transferDataReq = new TTransferDataReq()
        transferDataReq.setSessionHandle(handle)
        transferDataReq.setValues("test".getBytes("UTF-8"))
        transferDataReq.setPath("test")
        client.TransferData(transferDataReq)

        val executeStmtReq = new TExecuteStatementReq()
        executeStmtReq.setSessionHandle(handle)
        // upload data
        executeStmtReq.setStatement(
          s"MOVE DATA INPATH 'test' OVERWRITE INTO '${tempDir.toFile.getAbsolutePath}' 'dest.csv'")
        client.ExecuteStatement(executeStmtReq)

        val destFile = new File(tempDir.toFile, "dest.csv")
        assert(destFile.isFile)
        assert(FileUtils.readFileToString(destFile, "UTF-8") === "test")
        Utils.deleteDirectoryRecursively(tempDir.toFile)
      }
    }
  }

  test("kyuubi connection upload file") {
    withSessionConf(Map.empty)(Map.empty)(Map(
      KyuubiEbayConf.SESSION_CLUSTER.key -> "test")) {
      withJdbcStatement() { statement =>
        val connection = statement.getConnection.asInstanceOf[KyuubiConnection]
        val tempDir = Utils.createTempDir()
        val remoteDir = Utils.createTempDir()
        val localFile = new File(tempDir.toFile, "local.txt")
        Files.write(localFile.toPath, "test".getBytes(StandardCharset.UTF_8))
        val remoteFile = new File(remoteDir.toFile, "local.txt")
        assert(!remoteFile.exists())
        var result =
          connection.uploadFile(localFile.getAbsolutePath, remoteDir.toFile.getAbsolutePath, true)
        assert(remoteFile.isFile)
        assert(result === remoteFile.getAbsolutePath)
        connection.uploadFile(localFile.getAbsolutePath, remoteDir.toFile.getAbsolutePath, true)
        val e = intercept[SQLException] {
          connection.uploadFile(localFile.getAbsolutePath, remoteDir.toFile.getAbsolutePath, false)
        }
        assert(e.getMessage.contains("Dest path already exists"))
        val remoteFile2 = new File(remoteDir.toFile, "local2.txt")
        assert(!remoteFile2.exists())
        result = connection.uploadFile(
          localFile.getAbsolutePath,
          remoteDir.toFile.getAbsolutePath,
          "local2.txt",
          false)
        assert(remoteFile2.isFile)
        assert(result === remoteFile2.getAbsolutePath)
      }
    }
  }

  ignore("without session cluster mode enabled, the session cluster doest not valid") {
    withSessionConf(Map.empty)(Map.empty)(Map(
      KyuubiEbayConf.SESSION_CLUSTER.key -> "test")) {
      withJdbcStatement() { statement =>
        checkConfigValue(statement, "spark.sql.kyuubi.session.cluster.test", "<undefined>")
      }
    }
  }

  test("test proxy batch account with unsupported exception") {
    withSessionConf(Map(
      KyuubiAuthenticationFactory.KYUUBI_PROXY_BATCH_ACCOUNT -> "b_stf",
      KyuubiEbayConf.SESSION_CLUSTER.key -> "test"))(Map.empty)(Map.empty) {
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
      KyuubiConf.SESSION_ENGINE_LAUNCH_ASYNC.key -> "true",
      KyuubiEbayConf.SESSION_CLUSTER.key -> "test")) {
      val conn = new KyuubiConnection(jdbcUrlWithConf, new Properties(), engineLogListener)
      eventually(Timeout(20.seconds)) {
        assert(engineLogs.nonEmpty)
      }
      conn.close()
    }
  }

  test("HADP-44631: get full spark url") {
    withSessionConf()(Map.empty)(Map(
      "spark.ui.enabled" -> "true",
      KyuubiEbayConf.SESSION_CLUSTER.key -> "test")) {
      withJdbcStatement() { statement =>
        val conn = statement.getConnection.asInstanceOf[KyuubiConnection]
        val sparkUrl = conn.getSparkURL
        assert(sparkUrl.nonEmpty)
      }
    }
  }

  test("HADP-44628: Enable the timeout for KyuubiConnection::isValid") {
    withSessionConf(Map.empty)(Map.empty)(
      Map(
        KyuubiConf.SESSION_ENGINE_LAUNCH_ASYNC.key -> "false",
        KyuubiEbayConf.SESSION_CLUSTER.key -> "test")) {
      withJdbcStatement() { statement =>
        val conn = statement.getConnection
        assert(conn.isInstanceOf[KyuubiConnection])
        assert(conn.isValid(3))
      }
    }
  }

  test("HADP-44779: Support to get update count") {
    withSessionConf(Map.empty)(Map.empty)(Map(KyuubiEbayConf.SESSION_CLUSTER.key -> "test")) {
      withJdbcStatement("test_update_count") { statement =>
        statement.executeQuery("create table test_update_count(id int) using parquet")
        statement.executeQuery("insert into test_update_count values(1), (2)")
        assert(statement.getUpdateCount === 2)
      }
    }
  }

  test("test engine spark result max rows") {
    withSessionConf()(Map.empty)(Map(
      KyuubiConf.OPERATION_RESULT_MAX_ROWS.key -> "1",
      KyuubiEbayConf.SESSION_CLUSTER.key -> "test")) {
      withJdbcStatement("va") { statement =>
        statement.executeQuery("create temporary view va as select * from values(1),(2)")

        val resultLimit1 = statement.executeQuery("select * from va")
        assert(resultLimit1.next())
        assert(!resultLimit1.next())

        statement.executeQuery(s"set ${KyuubiConf.OPERATION_RESULT_MAX_ROWS.key}=0")
        statement.executeQuery(s"set ${KyuubiEbayConf.EBAY_OPERATION_MAX_RESULT_COUNT.key}=1")
        val resultUnLimit = statement.executeQuery("select * from va")
        assert(resultUnLimit.next())
        assert(resultUnLimit.next())
      }
    }
    withSessionConf()(Map.empty)(Map(
      KyuubiEbayConf.EBAY_OPERATION_MAX_RESULT_COUNT.key -> "1",
      KyuubiEbayConf.SESSION_CLUSTER.key -> "test")) {
      withJdbcStatement("va") { statement =>
        statement.executeQuery("create temporary view va as select * from values(1),(2)")

        val resultLimit1 = statement.executeQuery("select * from va")
        assert(resultLimit1.next())
        assert(!resultLimit1.next())

        statement.executeQuery(s"set ${KyuubiEbayConf.EBAY_OPERATION_MAX_RESULT_COUNT.key}=0")
        val resultUnLimit = statement.executeQuery("select * from va")
        assert(resultUnLimit.next())
        assert(resultUnLimit.next())
      }
    }
  }

  test("[HADP-44876][FOLLOWUP]: support to disable move queue for zeta connection") {
    Seq("zeta", "non-zeta").foreach { tag =>
      withSessionConf()(Map.empty)(Map(
        KyuubiEbayConf.SESSION_CLUSTER.key -> "test",
        KyuubiEbayConf.SESSION_TAG.key -> tag,
        KyuubiEbayConf.SESSION_ENGINE_LAUNCH_MOVE_QUEUE_ENABLED.key -> "true")) {
        withJdbcStatement() { _ =>
          val session =
            server.backendService.sessionManager.allSessions().head.asInstanceOf[KyuubiSessionImpl]
          assert(!KyuubiEbayConf.moveQueueEnabled(session.sessionConf))
        }
      }
    }
  }

  test("HADP-48118: cleanup the temp tables") {
    withSessionConf()(Map.empty)(Map(KyuubiEbayConf.SESSION_CLUSTER.key -> "test")) {
      withJdbcStatement() { statement =>
        statement.executeQuery("create temporary table ta(a int) using parquet")
        withJdbcStatement() { statement2 =>
          val result = statement2.executeQuery("show tables")
          assert(!result.next())
        }
      }
    }

    withSessionConf()(Map.empty)(Map(
      KyuubiEbayConf.SESSION_CLUSTER.key -> "test",
      KyuubiConf.ENGINE_SINGLE_SPARK_SESSION.key -> "true")) {
      withJdbcStatement() { statement =>
        statement.executeQuery("create temporary table ta(a int) using parquet")
        withJdbcStatement() { statement2 =>
          val result = statement2.executeQuery("show tables")
          assert(!result.next())
        }
      }

      withJdbcStatement() { statement =>
        statement.executeQuery("create temporary table ta(a int) using parquet")
      }
      withJdbcStatement() { statement =>
        val result = statement.executeQuery("show tables")
        assert(!result.next())
      }
    }
  }

  test("HADP-47085: tSupport to limit max connections for different clusters") {
    Seq("test", "cluster_limit").foreach { cluster =>
      withSessionConf()(Map.empty)(Map(KyuubiEbayConf.SESSION_CLUSTER.key -> cluster)) {
        // session 1
        withJdbcStatement() { _ =>
          withSessionConf()(Map.empty)(Map(KyuubiEbayConf.SESSION_CLUSTER.key -> cluster)) {
            // session 2
            if (cluster == "cluster_limit") {
              val e = intercept[Exception](withJdbcStatement() { _ => })
              assert(e.getMessage.contains("Connection limit per user reached"))
            } else {
              withJdbcStatement() { _ => }
            }
          }
        }
      }
    }
  }

  test("HADP-47085: batch is not limited with out batch limit key") {
    val sessionMgr = server.backendService.sessionManager.asInstanceOf[KyuubiSessionManager]
    val sessionNum = 5
    Seq("test", "cluster_limit").foreach { cluster =>
      (0 until sessionNum).foreach { _ =>
        val batchConf = Map(
          "kyuubi.session.cluster" -> cluster,
          BatchUtils.KYUUBI_BATCH_ID_KEY -> UUID.randomUUID().toString)
        val batchRequest = newSparkBatchRequest(batchConf)
        sessionMgr.openBatchSession(
          "user",
          "password",
          "127.0.0.1",
          batchRequest)
      }
      assert(sessionMgr.allSessions().filter(_.isInstanceOf[KyuubiBatchSession])
        .map(_.asInstanceOf[KyuubiBatchSession])
        .filter(_.sessionCluster == Some(cluster)).size == sessionNum)
    }
    sessionMgr.allSessions().foreach(_.close())
  }

  test("HADP-49143: support api key and api secret authentication") {
    Seq("serviceAccount1", "serviceAccount2").foreach { serviceAccount =>
      FakeApiKeyAuthenticationProviderImpl.setServiceAccount(serviceAccount)
      withSessionConf(Map.empty)(Map.empty)(Map(KyuubiEbayConf.SESSION_CLUSTER.key -> "test")) {
        withJdbcStatement() { statement =>
          val rs = statement.executeQuery("SELECT session_user()")
          assert(rs.next())
          assert(rs.getString(1) === serviceAccount)
          assert(!rs.next())
        }
      }
    }
  }

  test("HADP-49286: redact the session conf in AdminResource") {
    val adminUser = Utils.currentUser
    val encodeAuthorization = new String(
      Base64.getEncoder.encode(
        s"$adminUser:pass".getBytes()),
      "UTF-8")

    val requestObj = new SessionOpenRequest(Map(
      "kyuubi.session.cluster" -> "test",
      "spark.redaction.regex" -> "ebay",
      "kyuubi.token" -> "token",
      "kyuubi.ebay" -> "engineToken").asJava)
    var response = webTarget.path("api/v1/sessions")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .header(AUTHORIZATION_HEADER, s"BASIC $encodeAuthorization")
      .post(Entity.entity(requestObj, MediaType.APPLICATION_JSON_TYPE))

    val sessionHandle = response.readEntity(classOf[SessionHandle]).getIdentifier.toString

    // get session list
    val response2 = webTarget.path("api/v1/admin/sessions").request()
      .header(AUTHORIZATION_HEADER, s"BASIC $encodeAuthorization")
      .get()
    assert(200 == response2.getStatus)
    val sessions = response2.readEntity(new GenericType[Seq[SessionData]]() {})
    val session = sessions.find(_.getIdentifier === sessionHandle).get
    assert(session.getConf.get("kyuubi.token") === CommandLineUtils.REDACTION_REPLACEMENT_TEXT)
    assert(session.getConf.get("kyuubi.ebay") === CommandLineUtils.REDACTION_REPLACEMENT_TEXT)

    // close an opened session
    response = webTarget.path(s"api/v1/admin/sessions/$sessionHandle").request()
      .header(AUTHORIZATION_HEADER, s"BASIC $encodeAuthorization")
      .delete()
    assert(200 == response.getStatus)
  }

  test("HADP-49619: Support kyuubi spark on tess conf advisor - interactive") {
    withSessionConf(Map.empty)(Map.empty)(Map(
      KyuubiEbayConf.SESSION_CLUSTER.key -> "test",
      KyuubiEbayConf.ENGINE_SPARK_TESS_ENABLED.key -> "true",
      "kyuubi.hadoop.adlc.app" -> "adlc",
      "kyuubi.hadoop.adlc.ai" -> "adlc-ai",
      "kyuubi.hadoop.adlc.image" -> "spark:test",
      "spark.driver.cores" -> "3")) {
      withJdbcStatement() { statement =>
        checkConfigValue(statement, "spark.tess.key1", "value1")
        checkConfigValue(statement, "spark.tess.key2", "value2")
        checkConfigValue(statement, "spark.tess.overwrite", "true")

        checkConfigValue(
          statement,
          "spark.kubernetes.driver.annotation.application.tess.io/name",
          "adlc")
        checkConfigValue(
          statement,
          "spark.kubernetes.driver.annotation.io.sherlock.logs/namespace",
          "adlc")
        checkConfigValue(
          statement,
          "spark.kubernetes.driver.label.applicationinstance.tess.io/name",
          "adlc-ai")
        checkConfigValue(
          statement,
          "spark.kubernetes.executor.annotation.application.tess.io/name",
          "adlc")
        checkConfigValue(
          statement,
          "spark.kubernetes.executor.annotation.io.sherlock.logs/namespace",
          "adlc")
        checkConfigValue(
          statement,
          "spark.kubernetes.executor.label.applicationinstance.tess.io/name",
          "adlc-ai")
        checkConfigValue(
          statement,
          "spark.kubernetes.driver.request.cores",
          "3")
        checkConfigValue(
          statement,
          "spark.kubernetes.driver.limit.cores",
          "3")
        checkConfigValue(
          statement,
          "spark.kubernetes.executor.request.cores",
          "2")
        checkConfigValue(
          statement,
          "spark.kubernetes.executor.limit.cores",
          "2")
        checkConfigValue(statement, "spark.kubernetes.container.image", "spark:test")
      }
    }
  }

  test("HADP-49619: Support kyuubi spark on tess conf advisor - batch") {
    val sessionMgr = server.backendService.sessionManager.asInstanceOf[KyuubiSessionManager]
    val batchConf = Map(
      "kyuubi.session.cluster" -> "test",
      KyuubiEbayConf.ENGINE_SPARK_TESS_ENABLED.key -> "true",
      "kyuubi.hadoop.adlc.app" -> "adlc",
      "kyuubi.hadoop.adlc.ai" -> "adlc-ai",
      "kyuubi.hadoop.adlc.image" -> "spark:test",
      "spark.driver.cores" -> "3",
      BatchUtils.KYUUBI_BATCH_ID_KEY -> UUID.randomUUID().toString)
    val batchRequest = newSparkBatchRequest(batchConf)
    val sessionHandle = sessionMgr.openBatchSession(
      "user",
      "password",
      "127.0.0.1",
      batchRequest)
    val batchSession = sessionMgr.getBatchSession(sessionHandle).get
    val conf = batchSession.optimizedConf

    assert(conf("spark.tess.key1") == "value1")
    assert(conf("spark.tess.key2") == "value2")
    assert(conf("spark.tess.overwrite") == "true")

    assert(
      conf("spark.kubernetes.driver.annotation.application.tess.io/name") == "adlc")
    assert(conf("spark.kubernetes.driver.annotation.io.sherlock.logs/namespace") == "adlc")
    assert(conf("spark.kubernetes.driver.label.applicationinstance.tess.io/name") == "adlc-ai")
    assert(conf("spark.kubernetes.executor.annotation.application.tess.io/name") == "adlc")
    assert(conf("spark.kubernetes.executor.annotation.io.sherlock.logs/namespace") == "adlc")
    assert(conf("spark.kubernetes.executor.label.applicationinstance.tess.io/name") == "adlc-ai")
    assert(conf("spark.kubernetes.container.image") == "spark:test")
    assert(conf("spark.kubernetes.driver.request.cores") == "3")
    assert(conf("spark.kubernetes.driver.limit.cores") == "3")
    assert(conf("spark.kubernetes.executor.request.cores") == "2")
    assert(conf("spark.kubernetes.executor.limit.cores") == "2")
    batchSession.close()
  }

  def checkConfigValue(statement: Statement, config: String, expectedValue: String): Unit = {
    val rs = statement.executeQuery(s"SET `$config`")
    assert(rs.next())
    try {
      assert(
        rs.getString(2) == expectedValue,
        s"${rs.getString(1)}=${rs.getString(2)}/!=$expectedValue")
    } finally {
      rs.close()
    }
  }
}
