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

import java.io.File
import java.util.concurrent.ConcurrentHashMap

import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
import org.apache.hive.service.cli.thrift.TProtocolVersion
import org.apache.spark.{KyuubiConf, KyuubiSparkUtil, SparkConf, SparkFunSuite}

import scala.collection.mutable.{HashSet => MHSet}
import yaooqinn.kyuubi.KyuubiSQLException
import yaooqinn.kyuubi.auth.KyuubiAuthFactory
import yaooqinn.kyuubi.cli.GetInfoType
import yaooqinn.kyuubi.operation.{CLOSED, Operation, OperationHandle}
import yaooqinn.kyuubi.server.KyuubiServer
import yaooqinn.kyuubi.utils.ReflectUtils

abstract class SessionSuite extends SparkFunSuite {

  import KyuubiConf._

  var server: KyuubiServer = _
  var session: Session = _
  val statement = "show tables"

  override def beforeAll(): Unit = {
    System.setProperty(KyuubiConf.FRONTEND_BIND_PORT.key, "0")
    server = KyuubiServer.startKyuubiServer()
    server.getConf.remove(KyuubiSparkUtil.CATALOG_IMPL)
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    System.clearProperty(KyuubiConf.FRONTEND_BIND_PORT.key)
    if (session != null) session.close()
    if (server != null) server.stop()
    super.afterAll()
  }

  test("test session ugi") {
    assert(session.ugi.getAuthenticationMethod === AuthenticationMethod.SIMPLE)
  }

  test("session handle") {
    val handle = session.getSessionHandle
    assert(handle.getProtocolVersion === TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V8)
  }

  test("get last access time") {
    session.getInfo(GetInfoType.SERVER_NAME)
    assert(session.getLastAccessTime !== 0L)
  }

  test("get password") {
    assert(session.getPassword === "")
  }

  test("set operation log session dir") {
    val operationLogRootDir = new File(server.getConf.get(LOGGING_OPERATION_LOG_DIR))
    operationLogRootDir.mkdirs()
    session.setOperationLogSessionDir(operationLogRootDir)
    assert(session.getSessionLogDir.getAbsolutePath
      .startsWith(operationLogRootDir.getAbsolutePath))
    assert(session.isOperationLogEnabled)
    assert(operationLogRootDir.exists())
    assert(operationLogRootDir.listFiles().exists(_.getName == KyuubiSparkUtil.getCurrentUserName))
    assert(operationLogRootDir.listFiles().filter(_.getName == KyuubiSparkUtil.getCurrentUserName)
      .head.listFiles().exists(_.getName === session.getSessionHandle.getHandleIdentifier.toString))
    session.setOperationLogSessionDir(operationLogRootDir)
    assert(session.isOperationLogEnabled)
    operationLogRootDir.delete()
    operationLogRootDir.setExecutable(false)
    operationLogRootDir.setReadable(false)
    operationLogRootDir.setWritable(false)
    session.setOperationLogSessionDir(operationLogRootDir)
    assert(!session.isOperationLogEnabled)
    operationLogRootDir.setReadable(true)
    operationLogRootDir.setWritable(true)
    operationLogRootDir.setExecutable(true)
  }

  test("set resources session dir") {
    val resourceRoot = new File(server.getConf.get(OPERATION_DOWNLOADED_RESOURCES_DIR))
    resourceRoot.mkdirs()
    resourceRoot.deleteOnExit()
    assert(resourceRoot.isDirectory)
    session.setResourcesSessionDir(resourceRoot)
    assert(session.getResourcesSessionDir.getAbsolutePath.startsWith(resourceRoot.getAbsolutePath))
    val subDir = resourceRoot.listFiles().head
    assert(subDir.getName === KyuubiSparkUtil.getCurrentUserName)
    val resourceDir = subDir.listFiles().head
    assert(resourceDir.getName === session.getSessionHandle.getSessionId + "_resources")
    session.setResourcesSessionDir(resourceRoot)
    assert(subDir.listFiles().length === 1, "directory should already exists")
    assert(resourceDir.delete())
    resourceDir.createNewFile()
    assert(resourceDir.isFile)
    val e1 = intercept[RuntimeException](session.setResourcesSessionDir(resourceRoot))
    assert(e1.getMessage.startsWith("The resources directory exists but is not a directory"))
    resourceDir.delete()
    subDir.setWritable(false)
    val e2 = intercept[RuntimeException](session.setResourcesSessionDir(resourceRoot))
    assert(e2.getMessage.startsWith("Couldn't create session resources directory"))
    subDir.setWritable(true)
  }

  test("get no operation time") {
    assert(session.getNoOperationTime !== 0L)
  }

  test("get delegation token for non secured") {
    val authFactory =
      ReflectUtils.getFieldValue(server.feService, "authFactory").asInstanceOf[KyuubiAuthFactory]
    val e = intercept[KyuubiSQLException](
      session.getDelegationToken(authFactory, session.getUserName, session.getUserName))
    assert(e.getMessage === "Delegation token only supported over kerberos authentication")
    assert(e.toTStatus.getSqlState === "08S01")
  }

  test("cancel delegation token for non secured") {
    val authFactory =
      ReflectUtils.getFieldValue(server.feService, "authFactory").asInstanceOf[KyuubiAuthFactory]
    val e = intercept[KyuubiSQLException](
      session.cancelDelegationToken(authFactory, ""))
    assert(e.getMessage === "Delegation token only supported over kerberos authentication")
    assert(e.toTStatus.getSqlState === "08S01")
  }

  test("renew delegation token for non secured") {
    val authFactory =
      ReflectUtils.getFieldValue(server.feService, "authFactory").asInstanceOf[KyuubiAuthFactory]
    val e = intercept[KyuubiSQLException](
      session.renewDelegationToken(authFactory, ""))
    assert(e.getMessage === "Delegation token only supported over kerberos authentication")
    assert(e.toTStatus.getSqlState === "08S01")
  }

  test("test getProtocolVersion") {
    assert(session.getProtocolVersion === TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V8)
  }

  test("test close operation") {
    val opMgr = session.getSessionMgr.getOperationMgr
    val op = opMgr.newExecuteStatementOperation(session, statement)
    val opHandle = op.getHandle
    assert(opMgr.getOperation(opHandle) != null)
    session.closeOperation(opHandle)
    val e = intercept[KyuubiSQLException](opMgr.getOperation(opHandle))
    assert(e.getMessage === "Invalid OperationHandle " + opHandle)
  }

  test("test cancel operation") {
    val opMgr = session.getSessionMgr.getOperationMgr
    val op = opMgr.newExecuteStatementOperation(session, statement)
    val opHandle = op.getHandle
    assert(!op.isClosedOrCanceled)
    session.cancelOperation(opHandle)
    assert(op.isClosedOrCanceled)
  }

  test("test closeExpiredOperations") {
    val opMgr = session.getSessionMgr.getOperationMgr
    val conf = ReflectUtils.getSuperField(session, "yaooqinn$kyuubi$session$Session$$conf")
      .asInstanceOf[SparkConf]
    conf.set(KyuubiConf.OPERATION_IDLE_TIMEOUT.key, "1ms")
    val op = opMgr.newExecuteStatementOperation(session, statement)
    val opHandle = op.getHandle
    var opHandleSet = ReflectUtils.getSuperField(session, "opHandleSet")
      .asInstanceOf[MHSet[OperationHandle]]
    opHandleSet.add(opHandle)
    ReflectUtils.setSuperField(session, "opHandleSet", opHandleSet)

    val handleToOperation = ReflectUtils.getFieldValue(opMgr,
      "yaooqinn$kyuubi$operation$OperationManager$$handleToOperation")
      .asInstanceOf[ConcurrentHashMap[OperationHandle, Operation]]
    handleToOperation.put(opHandle, op)
    ReflectUtils.setSuperField(op, "state", CLOSED)
    ReflectUtils.setSuperField(op, "lastAccessTime", Long.box(Long.MinValue))
    session.closeExpiredOperations
    val e = intercept[KyuubiSQLException](opMgr.getOperation(opHandle))
    assert(e.getMessage === "Invalid OperationHandle " + opHandle)
  }
}
