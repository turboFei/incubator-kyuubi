/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package yaooqinn.kyuubi.session

import org.apache.hive.service.cli.thrift.TProtocolVersion
import org.apache.spark.KyuubiSparkUtil
import org.apache.spark.sql.SparkSession
import org.scalatest.mock.MockitoSugar
import yaooqinn.kyuubi.KyuubiSQLException
import yaooqinn.kyuubi.cli.{FetchOrientation, FetchType, GetInfoType}
import yaooqinn.kyuubi.operation.{CLOSED, KyuubiOperation, OperationManager}
import yaooqinn.kyuubi.schema.ColumnBasedSet
import yaooqinn.kyuubi.ui.KyuubiServerMonitor
import yaooqinn.kyuubi.utils.ReflectUtils

class KyuubiSessionSuite extends SessionSuite with MockitoSugar {

  var spark: SparkSession = _

  override def beforeAll(): Unit = {
    System.setProperty("spark.master", "local")
    super.beforeAll()
    val be = server.beService
    val sessionMgr = be.getSessionManager
    val operationMgr = sessionMgr.getOperationMgr
    val user = KyuubiSparkUtil.getCurrentUserName
    val passwd = ""
    val ip = ""
    val imper = true
    val proto = TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V8
    session =
      new KyuubiSession(proto, user, passwd, server.getConf, ip, imper, sessionMgr, operationMgr)
    session.open(Map.empty)
    KyuubiServerMonitor.getListener(user)
      .foreach(_.onSessionCreated(
        session.getIpAddress, session.getSessionHandle.getSessionId.toString, user))
    spark = session.asInstanceOf[KyuubiSession].sparkSession
  }

  override def afterAll(): Unit = {
    System.clearProperty("spark.master")
    if (spark != null) spark.stop()
    super.afterAll()
  }

  test("spark session") {
    assert(!spark.sparkContext.isStopped)
  }

  test("get info") {
    assert(
      session.getInfo(GetInfoType.SERVER_NAME).toTGetInfoValue.getStringValue === "Kyuubi Server")
    assert(session.getInfo(GetInfoType.DBMS_NAME).toTGetInfoValue.getStringValue === "Spark SQL")
    assert(
      session.getInfo(GetInfoType.DBMS_VERSION).toTGetInfoValue.getStringValue === spark.version)
    val e = intercept[KyuubiSQLException](session.getInfo(new GetInfoType {}))
    assert(e.getMessage.startsWith("Unrecognized GetInfoType value"))
  }

  test("test new ExecuteStatementOperation") {
    val statement = "show databases"
    val operationMgr = new OperationManager()
    assert(operationMgr.newExecuteStatementOperation(session, statement)
      .isInstanceOf[KyuubiOperation])
  }

  test("test executeStatement") {
    if (session != null) {
      session.close()
    }
    val sessionHadnle = server.beService.getSessionManager.openSession(
      session.getProtocolVersion,
      session.getUserName,
      "",
      session.getIpAddress,
      Map.empty,
      true)
    session = server.beService.getSessionManager.getSession(sessionHadnle)
    session.getSessionMgr.getCacheMgr.set(session.getUserName, spark)

    var opHandle = session.executeStatement("wrong statement")
    Thread.sleep(5000)
    var opException = session.getSessionMgr.getOperationMgr.getOperation(opHandle)
      .getStatus.getOperationException
    assert(opException.getSQLState === "ParseException")

    opHandle = session.executeStatement("select * from tablea")
    Thread.sleep(5000)
    opException = session.getSessionMgr.getOperationMgr.getOperation(opHandle)
      .getStatus.getOperationException
    assert(opException.getSQLState === "AnalysisException")

    opHandle = session.executeStatement("show tables")
    Thread.sleep(10000)
    val results = session.fetchResults(opHandle, FetchOrientation.FETCH_FIRST,
      10, FetchType.QUERY_OUTPUT)
    val logs = session.fetchResults(opHandle, FetchOrientation.FETCH_FIRST,
      10, FetchType.LOG)
    assert(results.isInstanceOf[ColumnBasedSet] && logs.isInstanceOf[ColumnBasedSet])

    // Just let server to stop all.
    session = null
  }

  test("test getNoOperationTime") {
    val mockSession = mock[KyuubiSession]
    assert(mockSession.getNoOperationTime === 0L)
  }
}
