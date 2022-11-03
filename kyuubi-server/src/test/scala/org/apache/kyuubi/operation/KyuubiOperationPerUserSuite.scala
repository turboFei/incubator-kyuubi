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

import java.util.UUID

import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.hive.service.rpc.thrift.{TExecuteStatementReq, TGetInfoReq, TGetInfoType, TStatusCode}
import org.scalatest.time.SpanSugar._

import org.apache.kyuubi.{Utils, WithKyuubiServer, WithSimpleDFSService}
import org.apache.kyuubi.KYUUBI_VERSION
import org.apache.kyuubi.config.{KyuubiConf, KyuubiEbayConf}
import org.apache.kyuubi.config.KyuubiConf.KYUUBI_ENGINE_ENV_PREFIX
import org.apache.kyuubi.engine.SemanticVersion
import org.apache.kyuubi.jdbc.hive.KyuubiConnection
import org.apache.kyuubi.jdbc.hive.KyuubiStatement
import org.apache.kyuubi.session.{KyuubiSessionImpl, KyuubiSessionManager, SessionHandle}
import org.apache.kyuubi.zookeeper.ZookeeperConf

class KyuubiOperationPerUserSuite
  extends WithKyuubiServer with SparkQueryTests with WithSimpleDFSService {

  override protected def jdbcUrl: String = getJdbcUrl

  override protected val conf: KyuubiConf = {
    KyuubiConf().set(KyuubiConf.ENGINE_SHARE_LEVEL, "user")
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    conf.set(s"$KYUUBI_ENGINE_ENV_PREFIX.HADOOP_CONF_DIR", getHadoopConfDir)
  }

  test("audit Kyuubi server MetaData") {
    withSessionConf()(Map(KyuubiConf.SERVER_INFO_PROVIDER.key -> "SERVER"))(Map.empty) {
      withJdbcStatement() { statement =>
        val metaData = statement.getConnection.getMetaData
        assert(metaData.getDatabaseProductName === "Apache Kyuubi (Incubating)")
        assert(metaData.getDatabaseProductVersion === KYUUBI_VERSION)
        val ver = SemanticVersion(KYUUBI_VERSION)
        assert(metaData.getDatabaseMajorVersion === ver.majorVersion)
        assert(metaData.getDatabaseMinorVersion === ver.minorVersion)
      }
    }
  }

  test("kyuubi defined function - system_user/session_user") {
    withJdbcStatement() { statement =>
      val rs = statement.executeQuery("SELECT system_user(), session_user()")
      assert(rs.next())
      assert(rs.getString(1) === Utils.currentUser)
      assert(rs.getString(2) === Utils.currentUser)
    }
  }

  test("ensure two connections in user mode share the same engine") {
    var r1: String = null
    var r2: String = null
    new Thread {
      override def run(): Unit = withJdbcStatement() { statement =>
        val res = statement.executeQuery("set spark.app.name")
        assert(res.next())
        r1 = res.getString("value")
      }
    }.start()

    new Thread {
      override def run(): Unit = withJdbcStatement() { statement =>
        val res = statement.executeQuery("set spark.app.name")
        assert(res.next())
        r2 = res.getString("value")
      }
    }.start()

    eventually(timeout(120.seconds), interval(100.milliseconds)) {
      assert(r1 != null && r2 != null)
    }

    assert(r1 === r2)
  }

  test("ensure open session asynchronously for USER mode still share the same engine") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("SELECT engine_id()")
      assert(resultSet.next())
      val engineId = resultSet.getString(1)

      withSessionConf(Map(
        KyuubiConf.SESSION_ENGINE_LAUNCH_ASYNC.key -> "true"))(Map.empty)(Map.empty) {
        withJdbcStatement() { stmt =>
          val rs = stmt.executeQuery("SELECT engine_id()")
          assert(rs.next())
          assert(rs.getString(1) == engineId)
        }
      }
    }
  }

  test("ensure two connections share the same engine when specifying subdomain.") {
    withSessionConf()(
      Map(
        KyuubiConf.ENGINE_SHARE_LEVEL_SUBDOMAIN.key -> "abc"))(Map.empty) {

      var r1: String = null
      var r2: String = null
      new Thread {
        override def run(): Unit = withJdbcStatement() { statement =>
          val res = statement.executeQuery("set spark.app.name")
          assert(res.next())
          r1 = res.getString("value")
        }
      }.start()

      new Thread {
        override def run(): Unit = withJdbcStatement() { statement =>
          val res = statement.executeQuery("set spark.app.name")
          assert(res.next())
          r2 = res.getString("value")
        }
      }.start()

      eventually(timeout(120.seconds), interval(100.milliseconds)) {
        assert(r1 != null && r2 != null)
      }

      assert(r1 === r2)
    }
  }

  test("ensure engine discovery works when mixed use subdomain") {
    var r1: String = null
    var r2: String = null
    withSessionConf()(Map.empty)(Map.empty) {
      withJdbcStatement() { statement =>
        val res = statement.executeQuery("set spark.app.name")
        assert(res.next())
        r1 = res.getString("value")
      }
    }
    assert(r1 contains "default")

    withSessionConf()(Map(KyuubiConf.ENGINE_SHARE_LEVEL_SUBDOMAIN.key -> "abc"))(Map.empty) {
      withJdbcStatement() { statement =>
        val res = statement.executeQuery("set spark.app.name")
        assert(res.next())
        r2 = res.getString("value")
      }
    }
    assert(r2 contains "abc")

    assert(r1 !== r2)
  }

  test("HADP-44628: Enable the timeout for KyuubiConnection::isValid") {
    withSessionConf(Map.empty)(Map.empty)(
      Map(KyuubiConf.SESSION_ENGINE_LAUNCH_ASYNC.key -> "false")) {
      withJdbcStatement() { statement =>
        val conn = statement.getConnection
        assert(conn.isInstanceOf[KyuubiConnection])
        assert(conn.isValid(3))
      }
    }
  }

  test("HADP-44779: Support to get update count") {
    withJdbcStatement("test_update_count") { statement =>
      statement.executeQuery("create table test_update_count(id int) using parquet")
      withJdbcStatement() { statement2 =>
        statement2.executeQuery("insert into test_update_count values(1), (2)")
        assert(statement2.getUpdateCount == 2)
      }
    }
  }

  test("test engine spark result max rows") {
    withSessionConf()(Map.empty)(Map(KyuubiConf.OPERATION_RESULT_MAX_ROWS.key -> "1")) {
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
    withSessionConf()(Map.empty)(Map(KyuubiEbayConf.EBAY_OPERATION_MAX_RESULT_COUNT.key -> "1")) {
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

  test("support to interrupt the thrift request if remote engine is broken") {
    assume(!httpMode)
    withSessionConf(Map(
      KyuubiConf.ENGINE_ALIVE_PROBE_ENABLED.key -> "true",
      KyuubiConf.ENGINE_ALIVE_PROBE_INTERVAL.key -> "1000",
      KyuubiConf.ENGINE_ALIVE_TIMEOUT.key -> "1000"))(Map.empty)(
      Map.empty) {
      withSessionHandle { (client, handle) =>
        val preReq = new TExecuteStatementReq()
        preReq.setStatement("select engine_name()")
        preReq.setSessionHandle(handle)
        preReq.setRunAsync(false)
        client.ExecuteStatement(preReq)

        val sessionHandle = SessionHandle(handle)
        val session = server.backendService.sessionManager.asInstanceOf[KyuubiSessionManager]
          .getSession(sessionHandle).asInstanceOf[KyuubiSessionImpl]
        session.client.getEngineAliveProbeProtocol.foreach(_.getTransport.close())

        val exitReq = new TExecuteStatementReq()
        exitReq.setStatement("SELECT java_method('java.lang.Thread', 'sleep', 1000L)," +
          "java_method('java.lang.System', 'exit', 1)")
        exitReq.setSessionHandle(handle)
        exitReq.setRunAsync(true)
        client.ExecuteStatement(exitReq)

        val executeStmtReq = new TExecuteStatementReq()
        executeStmtReq.setStatement("SELECT java_method('java.lang.Thread', 'sleep', 30000l)")
        executeStmtReq.setSessionHandle(handle)
        executeStmtReq.setRunAsync(false)
        val startTime = System.currentTimeMillis()
        val executeStmtResp = client.ExecuteStatement(executeStmtReq)
        assert(executeStmtResp.getStatus.getStatusCode === TStatusCode.ERROR_STATUS)
        assert(executeStmtResp.getStatus.getErrorMessage.contains(
          "java.net.SocketException: Connection reset") ||
          executeStmtResp.getStatus.getErrorMessage.contains(
            "Caused by: java.net.SocketException: Broken pipe (Write failed)"))
        val elapsedTime = System.currentTimeMillis() - startTime
        assert(elapsedTime < 20 * 1000)
        assert(session.client.asyncRequestInterrupted)
      }
    }
  }

  test("HADP-46611: support to save the result into temp table") {
    withDatabases("kyuubi") { _ =>
      withSessionConf(Map.empty)(Map(
        KyuubiConf.ENGINE_SINGLE_SPARK_SESSION.key -> "true",
        KyuubiEbayConf.OPERATION_TEMP_TABLE_COLLECT.key -> "true",
        KyuubiConf.OPERATION_INCREMENTAL_COLLECT.key -> "true"))(Map.empty) {
        withJdbcStatement() { statement =>
          var rs = statement.executeQuery("SELECT system_user(), session_user()")
          assert(rs.next())
          assert(rs.getString(1) === Utils.currentUser)
          assert(rs.getString(2) === Utils.currentUser)
          assert(!rs.next())
          // use a new statement to prevent the temp table cleanup
          val statement2 = statement.getConnection.createStatement()
          rs = statement2.executeQuery("show tables")
          // before failed to create temporary view due to duplicated udf columns
          assert(!rs.next())
          // cleanup the temp table
          statement.close()
          rs = statement2.executeQuery("show tables")
          assert(!rs.next())
          statement2.executeQuery("create table ta(id int) using parquet")
          statement2.executeQuery("insert into ta(id) values(1),(2)")
          rs = statement2.executeQuery("SELECT * from ta order by id")
          assert(rs.next())
          assert(rs.getInt(1) === 1)
          assert(rs.next())
          assert(rs.getInt(1) === 2)
          assert(!rs.next())
          // use a new statement to prevent the temp table cleanup
          val statement3 = statement2.getConnection.createStatement()
          rs = statement3.executeQuery("show tables")
          assert(rs.next())
          assert(rs.next())
          val tempTableName = rs.getString(2)
          assert(tempTableName.startsWith("kyuubi_cache_"))
          assert(!rs.next())
          rs = statement3.executeQuery(s"select * from $tempTableName")
          assert(rs.next())
          assert(rs.getInt(1) === 1)
          assert(rs.next())
          assert(rs.getInt(1) === 2)
          assert(!rs.next())
          statement3.executeQuery("drop table ta")
        }
      }
    }
  }

  test("scala NPE issue with hdfs jar") {
    val jarDir = Utils.createTempDir().toFile
    val udfCode =
      """
        |package test.utils
        |
        |object Math {
        |def add(x: Int, y: Int): Int = x + y
        |}
        |
        |""".stripMargin
    val jarFile = UserJarTestUtils.createJarFile(
      udfCode,
      "test",
      s"test-function-${UUID.randomUUID}.jar",
      jarDir.toString)
    val hadoopConf = getHadoopConf
    val dfs = FileSystem.get(hadoopConf)
    val dfsJarDir = dfs.makeQualified(new Path(s"jars-${UUID.randomUUID()}"))
    val localFs = FileSystem.getLocal(hadoopConf)
    val localPath = new Path(jarFile.getAbsolutePath)
    val dfsJarPath = new Path(dfsJarDir, "test-function.jar")
    FileUtil.copy(localFs, localPath, dfs, dfsJarPath, false, false, hadoopConf)
    withJdbcStatement() { statement =>
      val kyuubiStatement = statement.asInstanceOf[KyuubiStatement]
      statement.executeQuery(s"add jar $dfsJarPath")
      val rs = kyuubiStatement.executeScala("println(test.utils.Math.add(1,2))")
      rs.next()
      assert(rs.getString(1) === "3")
    }
  }

  test("server info provider - server") {
    assume(!httpMode)
    withSessionConf(Map(KyuubiConf.SERVER_INFO_PROVIDER.key -> "SERVER"))()() {
      withSessionHandle { (client, handle) =>
        val req = new TGetInfoReq()
        req.setSessionHandle(handle)
        req.setInfoType(TGetInfoType.CLI_DBMS_NAME)
        assert(client.GetInfo(req).getInfoValue.getStringValue === "Apache Kyuubi (Incubating)")
      }
    }
  }

  test("server info provider - engine") {
    assume(!httpMode)
    withSessionConf(Map(KyuubiConf.SERVER_INFO_PROVIDER.key -> "ENGINE"))()() {
      withSessionHandle { (client, handle) =>
        val req = new TGetInfoReq()
        req.setSessionHandle(handle)
        req.setInfoType(TGetInfoType.CLI_DBMS_NAME)
        assert(client.GetInfo(req).getInfoValue.getStringValue === "Spark SQL")
      }
    }
  }

  test("the new client should work properly when the engine exits unexpectedly") {
    assume(!httpMode)
    withSessionConf(Map(
      ZookeeperConf.ZK_MAX_SESSION_TIMEOUT.key -> "10000"))(Map.empty)(
      Map.empty) {
      withSessionHandle { (client, handle) =>
        val preReq = new TExecuteStatementReq()
        preReq.setStatement("SET kyuubi.operation.language=scala")
        preReq.setSessionHandle(handle)
        preReq.setRunAsync(false)
        client.ExecuteStatement(preReq)

        val exitReq = new TExecuteStatementReq()
        // force kill engine without shutdown hook
        exitReq.setStatement("java.lang.Runtime.getRuntime().halt(-1)")
        exitReq.setSessionHandle(handle)
        exitReq.setRunAsync(true)
        client.ExecuteStatement(exitReq)
      }
      withSessionHandle { (client, handle) =>
        val preReq = new TExecuteStatementReq()
        preReq.setStatement("select engine_name()")
        preReq.setSessionHandle(handle)
        preReq.setRunAsync(false)
        val tExecuteStatementResp = client.ExecuteStatement(preReq)
        val opHandle = tExecuteStatementResp.getOperationHandle
        waitForOperationToComplete(client, opHandle)
        assert(tExecuteStatementResp.getStatus.getStatusCode === TStatusCode.SUCCESS_STATUS)
      }
    }
  }

}
