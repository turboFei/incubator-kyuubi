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

package yaooqinn.kyuubi.operation

import java.io.File

import org.apache.hadoop.security.UserGroupInformation
import org.apache.hive.service.cli.thrift.{TFetchOrientation, TProtocolVersion}
import org.apache.spark.{KyuubiSparkUtil, SparkConf, SparkFunSuite}
import org.apache.spark.KyuubiConf.LOGGING_OPERATION_LOG_DIR
import org.scalatest.mock.MockitoSugar

import yaooqinn.kyuubi.KyuubiSQLException
import yaooqinn.kyuubi.cli.FetchOrientation
import yaooqinn.kyuubi.session.{KyuubiSession, Session, SessionManager}
import yaooqinn.kyuubi.utils.ReflectUtils

abstract class OperationSuite extends SparkFunSuite with MockitoSugar {

  val conf = new SparkConf(loadDefaults = true).setAppName("operation test")
  KyuubiSparkUtil.setupCommonConfig(conf)
  conf.remove(KyuubiSparkUtil.CATALOG_IMPL)
  conf.setMaster("local")
  var sessionMgr: SessionManager = _
  val proto = TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V8
  val user = UserGroupInformation.getCurrentUser
  val userName = user.getShortUserName
  val passwd = ""
  val statement = "show tables"
  var session: Session = _

  override protected def beforeAll(): Unit = {
    sessionMgr = new SessionManager()
    sessionMgr.init(conf)
    sessionMgr.start()
  }

  protected override def afterAll(): Unit = {
    session.stopShared()
    session.close()
    session = null
    sessionMgr.stop()
  }

  test("testCancel") {
    val op = sessionMgr.getOperationMgr.newExecuteStatementOperation(session, statement)
    assert(op.getStatus.getState === INITIALIZED)
    op.cancel()
    assert(op.getStatus.getState === CANCELED)
  }

  test("testGetHandle") {
    val op = sessionMgr.getOperationMgr.newExecuteStatementOperation(session, statement)
    assert(!op.getHandle.isHasResultSet)
    assert(!op.getHandle.toTOperationHandle.isHasResultSet)
    op.getHandle.setHasResultSet(true)
    assert(op.getHandle.isHasResultSet)
    assert(op.getHandle.toTOperationHandle.isHasResultSet)
    assert(op.getHandle.getProtocolVersion === TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V8)
    assert(op.getHandle.getOperationType === EXECUTE_STATEMENT)
  }

  test("testGetStatus") {
    val op = sessionMgr.getOperationMgr.newExecuteStatementOperation(session, statement)
    assert(op.getStatus.getState === INITIALIZED)
    assert(op.getStatus.getOperationException === null)
  }

  test("testIsTimedOut") {
    val op = sessionMgr.getOperationMgr.newExecuteStatementOperation(session, statement)
    assert(!op.isTimedOut)
  }

  test("testGetProtocolVersion") {
    val op = sessionMgr.getOperationMgr.newExecuteStatementOperation(session, statement)
    assert(op.getProtocolVersion === proto)
  }

  test("testGetOperationLog") {
    // TODO
  }

  test("testClose") {
    val op = sessionMgr.getOperationMgr.newExecuteStatementOperation(session, statement)
    assert(op.getStatus.getState === INITIALIZED)
    op.close()
    assert(op.getStatus.getState === CLOSED)
  }

  test("is closed or canceled") {
    val op = sessionMgr.getOperationMgr.newExecuteStatementOperation(session, statement)
    assert(!op.isClosedOrCanceled)
    op.cancel()
    assert(op.isClosedOrCanceled)
    op.close()
    assert(op.isClosedOrCanceled)
    val op2 = sessionMgr.getOperationMgr.newExecuteStatementOperation(session, statement)
    op2.close()
    assert(op2.isClosedOrCanceled)
    val op3 = sessionMgr.getOperationMgr.newExecuteStatementOperation(session, null)
    op3.cancel()
    op3.close()
    assert(op3.isClosedOrCanceled)
  }

  test("test getSession") {
    val op = sessionMgr.getOperationMgr.newExecuteStatementOperation(session, statement)
    assert(op.getSession === session)
  }

  test("test get operation log") {
    val operationLogRootDir = new File(conf.get(LOGGING_OPERATION_LOG_DIR.key))
    operationLogRootDir.mkdirs()
    session.setOperationLogSessionDir(operationLogRootDir)
    val op = sessionMgr.getOperationMgr.newExecuteStatementOperation(session, statement)
    assert(op.getOperationLog === null)
    ReflectUtils.invokeMethod(op, "registerCurrentOperationLog")
    assert(sessionMgr.getOperationMgr.getOperationLog === null)
    ReflectUtils.invokeMethod(op, "createOperationLog")
    assert(op.getOperationLog != null)

    ReflectUtils.invokeMethod(op, "createOperationLog")
    assert(op.getOperationLog != null)

    ReflectUtils.invokeMethod(op, "unregisterOperationLog")
    assert(sessionMgr.getOperationMgr.getOperationLog === null)
    operationLogRootDir.delete()
  }

  test("test set, check and assert state") {
    val op = sessionMgr.getOperationMgr.newExecuteStatementOperation(session, statement)
    ReflectUtils.invokeMethod(op, "setState", List(classOf[OperationState]), List(RUNNING))
    assert(ReflectUtils.invokeMethod(
      op, "checkState", List(classOf[OperationState]), List(RUNNING)) === true)
    ReflectUtils.invokeMethod(op, "assertState", List(classOf[OperationState]), List(RUNNING))
  }

  test("test validateDefaultFetchOrientation") {
    case object FETCH_RELATIVE extends FetchOrientation {
      override val toTFetchOrientation: TFetchOrientation = TFetchOrientation.FETCH_RELATIVE
    }

    val op = sessionMgr.getOperationMgr.newExecuteStatementOperation(session, statement)
    val e = intercept[KyuubiSQLException](ReflectUtils.invokeMethod(op,
      "validateDefaultFetchOrientation", List(classOf[FetchOrientation]), List(FETCH_RELATIVE)))
    assert(e.getMessage === "The fetch type " + FETCH_RELATIVE.toString +
      " is not supported for this resultset")
  }
}
