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

package yaooqinn.kyuubi.session

import java.io.File

import org.apache.hive.service.cli.thrift.{TGetInfoType, TProtocolVersion}
import org.apache.spark.KyuubiConf.{LOGGING_OPERATION_LOG_DIR, OPERATION_DOWNLOADED_RESOURCES_DIR}
import org.apache.spark.KyuubiSparkUtil
import org.apache.spark.sql.SparkSession

import yaooqinn.kyuubi.KyuubiSQLException
import yaooqinn.kyuubi.cli.{FetchOrientation, FetchType, GetInfoType}
import yaooqinn.kyuubi.schema.ColumnBasedSet
import yaooqinn.kyuubi.ui.KyuubiServerMonitor

class KyuubiClientSessionSuite extends AbstractKyuubiSessionSuite {

  var spark: SparkSession = _
  var kyuubiClientSession: KyuubiClientSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    val be = server.beService
    val sessionMgr = be.getSessionManager
    val operationMgr = sessionMgr.getOperationMgr
    val user = KyuubiSparkUtil.getCurrentUserName
    val passwd = ""
    val ip = ""
    val imper = true
    val proto = TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V8
    session = new KyuubiClientSession(
      proto, user, passwd, server.getConf, ip, imper, sessionMgr, operationMgr)
    session.open(Map.empty)
    KyuubiServerMonitor.getListener(user)
      .foreach(_.onSessionCreated(
        session.getIpAddress, session.getSessionHandle.getSessionId.toString, user))
    kyuubiClientSession = session.asInstanceOf[KyuubiClientSession]
    spark = kyuubiClientSession.sparkSession
  }

  override def afterAll(): Unit = {
    if (session != null) {
      if (spark != null) {
        spark.stop()
      }
    }
    super.afterAll()
  }

  test("set operation log session dir") {
    val operationLogRootDir = new File(server.getConf.get(LOGGING_OPERATION_LOG_DIR.key))
    operationLogRootDir.mkdirs()
    kyuubiClientSession.setOperationLogSessionDir(operationLogRootDir)
    assert(kyuubiClientSession.isOperationLogEnabled)
    assert(operationLogRootDir.exists())
    assert(operationLogRootDir.listFiles().exists(_.getName == KyuubiSparkUtil.getCurrentUserName))
    assert(operationLogRootDir.listFiles().filter(_.getName == KyuubiSparkUtil.getCurrentUserName)
      .head.listFiles().exists(_.getName === session.getSessionHandle.getHandleIdentifier.toString))
    kyuubiClientSession.setOperationLogSessionDir(operationLogRootDir)
    assert(kyuubiClientSession.isOperationLogEnabled)
    operationLogRootDir.delete()
    operationLogRootDir.setExecutable(false)
    operationLogRootDir.setReadable(false)
    operationLogRootDir.setWritable(false)
    kyuubiClientSession.setOperationLogSessionDir(operationLogRootDir)
    assert(!kyuubiClientSession.isOperationLogEnabled)
    operationLogRootDir.setReadable(true)
    operationLogRootDir.setWritable(true)
    operationLogRootDir.setExecutable(true)
  }

  test("set resources session dir") {
    val resourceRoot = new File(server.getConf.get(OPERATION_DOWNLOADED_RESOURCES_DIR.key))
    resourceRoot.mkdirs()
    resourceRoot.deleteOnExit()
    assert(resourceRoot.isDirectory)
    kyuubiClientSession.setResourcesSessionDir(resourceRoot)
    val subDir = resourceRoot.listFiles().head
    assert(subDir.getName === KyuubiSparkUtil.getCurrentUserName)
    val resourceDir = subDir.listFiles().head
    assert(resourceDir.getName === session.getSessionHandle.getSessionId + "_resources")
    kyuubiClientSession.setResourcesSessionDir(resourceRoot)
    assert(subDir.listFiles().length === 1, "directory should already exists")
    assert(resourceDir.delete())
    resourceDir.createNewFile()
    assert(resourceDir.isFile)
    val e1 = intercept[RuntimeException](kyuubiClientSession.setResourcesSessionDir(resourceRoot))
    assert(e1.getMessage.startsWith("The resources directory exists but is not a directory"))
    resourceDir.delete()
    subDir.setWritable(false)
    val e2 = intercept[RuntimeException](kyuubiClientSession.setResourcesSessionDir(resourceRoot))
    assert(e2.getMessage.startsWith("Couldn't create session resources directory"))
    subDir.setWritable(true)
  }

  test("test get info") {
    assert(session.getInfo(GetInfoType.SERVER_NAME).toTGetInfoValue
      .getStringValue === "Kyuubi Server")
    assert(session.getInfo(GetInfoType.DBMS_NAME).toTGetInfoValue
      .getStringValue === "Spark SQL")
    assert(session.getInfo(GetInfoType.DBMS_VERSION).toTGetInfoValue
      .getStringValue === spark.version)

    case object UNSUPPORT_INFO extends GetInfoType {
      override val tInfoType: TGetInfoType = TGetInfoType.CLI_USER_NAME
    }
    intercept[KyuubiSQLException](session.getInfo(UNSUPPORT_INFO))
  }

  test("test getNoOperationTime") {
    val mockSession = mock[KyuubiClientSession]
    assert(mockSession.getNoOperationTime === 0L)
  }

  test("test executeStatement") {
    val sessionHadnle = server.beService.getSessionManager.openSession(
      session.getProtocolVersion,
      session.getUserName,
      "",
      session.getIpAddress,
      Map.empty,
      true)
    val kyuubiSession = server.beService.getSessionManager.getSession(sessionHadnle)
    kyuubiSession.getSessionMgr.getCacheMgr.set(session.getUserName, spark)

    var opHandle = kyuubiSession.executeStatement("wrong statement")
    Thread.sleep(5000)
    var opException = kyuubiSession.getSessionMgr.getOperationMgr.getOperation(opHandle)
      .getStatus.getOperationException
    assert(opException.getSQLState === "ParseException")

    opHandle = kyuubiSession.executeStatement("select * from tablea")
    Thread.sleep(5000)
    opException = kyuubiSession.getSessionMgr.getOperationMgr.getOperation(opHandle)
      .getStatus.getOperationException
    assert(opException.getSQLState === "AnalysisException")

    opHandle = kyuubiSession.executeStatement("show tables")
    Thread.sleep(5000)
    val results = kyuubiSession.fetchResults(opHandle, FetchOrientation.FETCH_FIRST,
      10, FetchType.QUERY_OUTPUT)
    val logs = kyuubiSession.fetchResults(opHandle, FetchOrientation.FETCH_FIRST,
      10, FetchType.LOG)
    assert(results.isInstanceOf[ColumnBasedSet] && logs.isInstanceOf[ColumnBasedSet])
  }
}
