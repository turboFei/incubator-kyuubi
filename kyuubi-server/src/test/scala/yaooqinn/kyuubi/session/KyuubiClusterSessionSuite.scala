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

import java.util.concurrent.ConcurrentHashMap

import org.apache.hadoop.security.UserGroupInformation
import org.apache.hive.service.cli.thrift._
import org.apache.spark.KyuubiSparkUtil
import org.scalatest.mock.MockitoSugar

import yaooqinn.kyuubi.KyuubiSQLException
import yaooqinn.kyuubi.cli.GetInfoType
import yaooqinn.kyuubi.ui.KyuubiServerMonitor
import yaooqinn.kyuubi.utils.ReflectUtils
import yaooqinn.kyuubi.yarn.KyuubiAppMasterWithUGI

class KyuubiClusterSessionSuite extends SessionSuite with MockitoSugar {

  var kyuubiClusterSession: KyuubiClusterSession = _

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
    session = new KyuubiClusterSession(proto, user, passwd, server.getConf, ip, imper,
      sessionMgr, operationMgr)
    kyuubiClusterSession = session.asInstanceOf[KyuubiClusterSession]
    kyuubiClusterSession.mockOpen(server)
    KyuubiServerMonitor.getListener(user)
      .foreach(_.onSessionCreated(
        session.getIpAddress, session.getSessionHandle.getSessionId.toString, user))
  }

  override def afterAll(): Unit = {
    System.clearProperty("spark.master")
    super.afterAll()
  }

  test("get info") {
    val e = intercept[KyuubiSQLException](session.getInfo(new GetInfoType {}))
    assert(e.getMessage.startsWith("Method Not Implemented!"))
  }

  test("get info resp") {
    val serverName = kyuubiClusterSession.getInfoResp(
      new TGetInfoReq(kyuubiClusterSession.thriftHandle, TGetInfoType.CLI_SERVER_NAME))
    assert(serverName.getInfoValue.getStringValue == "Kyuubi Server" )

    val dbmsName = kyuubiClusterSession.getInfoResp(
      new TGetInfoReq(kyuubiClusterSession.thriftHandle, TGetInfoType.CLI_DBMS_NAME))
    assert(dbmsName.getInfoValue.getStringValue == "Spark SQL")

    val dbmsVersion = kyuubiClusterSession.getInfoResp(
      new TGetInfoReq(kyuubiClusterSession.thriftHandle, TGetInfoType.CLI_DBMS_VER))
    assert(dbmsVersion.getInfoValue.getStringValue == server.beService.getSessionManager
      .getCacheMgr.getAndIncrease(session.getUserName).get.version)

    val client = ReflectUtils.getFieldValue(kyuubiClusterSession, "client")
    ReflectUtils.setFieldValue(kyuubiClusterSession, "client", null)
    var e = intercept[KyuubiSQLException](kyuubiClusterSession.getInfoResp(
      new TGetInfoReq(kyuubiClusterSession.thriftHandle, TGetInfoType.CLI_SERVER_NAME)))
    assert(e.getSQLState === "08S01")

    e = intercept[KyuubiSQLException](ReflectUtils.invokeMethod(kyuubiClusterSession,
      "openThriftConnection", List(classOf[Map[String, String]]), List(Map.empty)))
    assert(e.getSQLState === "08S01")
    ReflectUtils.setFieldValue(kyuubiClusterSession, "client", client)
  }

  test("test call from backendService") {
    val sessHandle = session.getSessionHandle
    val handleToSession = ReflectUtils.getFieldValue(server.beService.getSessionManager,
      "yaooqinn$kyuubi$session$SessionManager$$handleToSession")
      .asInstanceOf[ConcurrentHashMap[SessionHandle, Session]]
    handleToSession.put(sessHandle, session)

    val serverName = server.beService.getInfoResp(sessHandle,
      new TGetInfoReq(sessHandle.toTSessionHandle, TGetInfoType.CLI_SERVER_NAME))
    assert(serverName.getInfoValue.getStringValue == "Kyuubi Server")

    val dbmsName = server.beService.getInfoResp(sessHandle,
      new TGetInfoReq(sessHandle.toTSessionHandle, TGetInfoType.CLI_DBMS_NAME))
    assert(dbmsName.getInfoValue.getStringValue == "Spark SQL")

    val dbmsVersion = server.beService.getInfoResp(sessHandle,
      new TGetInfoReq(sessHandle.toTSessionHandle, TGetInfoType.CLI_DBMS_VER))
    assert(dbmsVersion.getInfoValue.getStringValue == server.beService.getSessionManager
      .getCacheMgr.getAndIncrease(session.getUserName).get.version)
  }

  test("test getThriftConnection") {
    val ugi = UserGroupInformation.getCurrentUser
    val kyuubiAmWithUGI = new KyuubiAppMasterWithUGI(ugi, server.getConf)
    val fe = server.feService
    val instance = Some(fe.getServerIPAddress.getHostName + ":" + fe.getPortNumber)
    ReflectUtils.setFieldValue(kyuubiAmWithUGI, "instance", instance)
    ReflectUtils.setFieldValue(session, "kyuubiAppMasterWithUGI", kyuubiAmWithUGI)

    val client = ReflectUtils.getFieldValue(kyuubiClusterSession, "client")
    var kqe: KyuubiSQLException = null
    // For this test throws exception in local, but not in travis ci
    try {
      ReflectUtils.invokeMethod(session,
        "yaooqinn$kyuubi$session$KyuubiClusterSession$$getThriftConnection",
        List(classOf[Map[String, String]]), List(Map.empty))
    } catch {
      case e: KyuubiSQLException =>
        kqe = e
    }
    if (kqe != null) {
      assert(kqe.getMessage === s"Get Thrift connection for [${session.getUserName}] failed")
    }

    ReflectUtils.setFieldValue(kyuubiAmWithUGI, "instance", Some("127.0.0.1"))
    ReflectUtils.setFieldValue(session, "kyuubiAppMasterWithUGI", kyuubiAmWithUGI)
    val e = intercept[KyuubiSQLException]( ReflectUtils.invokeMethod(session,
      "yaooqinn$kyuubi$session$KyuubiClusterSession$$getThriftConnection",
      List(classOf[Map[String, String]]), List(Map.empty)))
    ReflectUtils.setFieldValue(kyuubiClusterSession, "client", client)
  }

  test("test close kyuubiClusterSession with exception") {
    val client = ReflectUtils.getFieldValue(kyuubiClusterSession, "client")
    ReflectUtils.setFieldValue(kyuubiClusterSession, "client", null)
    val e = intercept[KyuubiSQLException](session.close())
    assert(e.getMessage === "Error occured when close the ThriftConnection")
    ReflectUtils.setFieldValue(kyuubiClusterSession, "client", client)
  }

  test("test verifySuccess") {
    val tStatus0 = new TStatus(TStatusCode.SUCCESS_STATUS)
    val tStatus1 = new TStatus(TStatusCode.SUCCESS_WITH_INFO_STATUS)
    val tStatus2 = new TStatus(TStatusCode.STILL_EXECUTING_STATUS)
    val tStatus3 = new TStatus(TStatusCode.ERROR_STATUS)
    val tStatus4 = new TStatus(TStatusCode.INVALID_HANDLE_STATUS)

    KyuubiClusterSession.verifySuccess(tStatus0, false)
    KyuubiClusterSession.verifySuccess(tStatus0, true)
    KyuubiClusterSession.verifySuccess(tStatus1, true)

    intercept[KyuubiSQLException](KyuubiClusterSession.verifySuccess(tStatus1))
    intercept[KyuubiSQLException](KyuubiClusterSession.verifySuccess(tStatus2))
    intercept[KyuubiSQLException](KyuubiClusterSession.verifySuccess(tStatus3))
    intercept[KyuubiSQLException](KyuubiClusterSession.verifySuccess(tStatus4))
  }
}
