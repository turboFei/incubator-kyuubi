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

import org.apache.hive.service.cli.thrift._
import org.apache.hive.service.cli.thrift.TCLIService.Iface
import org.apache.spark.{KyuubiConf, KyuubiSparkUtil}
import org.mockito.Matchers.any
import org.mockito.Mockito.when
import scala.collection.JavaConverters._

import yaooqinn.kyuubi.KyuubiSQLException
import yaooqinn.kyuubi.cli.FetchOrientation.FETCH_NEXT
import yaooqinn.kyuubi.cli.FetchType
import yaooqinn.kyuubi.server.KyuubiServer
import yaooqinn.kyuubi.session.KyuubiClusterSession
import yaooqinn.kyuubi.utils.ReflectUtils

class KyuubiClusterOperationSuite extends AbstractKyuubiOperationSuite {

  var kyuubiClusterSession: KyuubiClusterSession = _
  var server: KyuubiServer = _

  override protected def beforeAll(): Unit = {
    System.setProperty("spark.master", "local")
    System.setProperty(KyuubiConf.FRONTEND_BIND_PORT.key, "0")
    server = KyuubiServer.startKyuubiServer()
    server.getConf.remove(KyuubiSparkUtil.CATALOG_IMPL)
    val be = server.beService
    sessionMgr = be.getSessionManager
    val operationMgr = sessionMgr.getOperationMgr
    val user = KyuubiSparkUtil.getCurrentUserName
    val passwd = ""
    val ip = ""
    val imper = true
    val proto = TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V8
    session = new KyuubiClusterSession(proto, user, passwd, server.getConf, ip, imper,
      sessionMgr, operationMgr)
    ReflectUtils.invokeMethod(session, "mockOpen", List(server.getClass), List(server))
    kyuubiClusterSession = session.asInstanceOf[KyuubiClusterSession]
  }

  protected override def afterAll(): Unit = {
    System.clearProperty("spark.master")
    if (session != null) {
      session.close()
    }
    System.clearProperty(KyuubiConf.FRONTEND_BIND_PORT.key)
    if (server != null) server.stop()
  }

  test("test getResultSetSchema") {
    val op = sessionMgr.getOperationMgr.newExecuteStatementOperation(session, statement)
    val e = intercept[KyuubiSQLException](op.getResultSetSchema)
    assert(e.getMessage.startsWith("Method Not Implemented!"))
  }

  test("test getNextRowSet") {
    val op = sessionMgr.getOperationMgr.newExecuteStatementOperation(session, statement)
    val e = intercept[KyuubiSQLException](op.getNextRowSet(FETCH_NEXT, 10))
    assert(e.getMessage.startsWith("Method Not Implemented!"))
  }

  test("test getResultSetMetaDataResp and getResultResp") {
    val op = sessionMgr.getOperationMgr.newExecuteStatementOperation(session, statement)
      .asInstanceOf[KyuubiClusterOperation]
    ReflectUtils.invokeMethod(op, "execute")
    val schema = op.getResultSetMetaDataResp().getSchema
    val columnNames = schema.getColumns.asScala.map(_.getColumnName)
    assert(columnNames.mkString(";") === "database;tableName;isTemporary")
    val result = op.getResultResp(FETCH_NEXT, 10, FetchType.QUERY_OUTPUT).getResults
    assert(result.isInstanceOf[TRowSet])
    val logs = op.getResultResp(FETCH_NEXT, 10, FetchType.LOG).getResults
    assert(logs.isInstanceOf[TRowSet])
    op.close()
  }

  test("test execute kyuubiClusterOperation with exception") {
    val op = sessionMgr.getOperationMgr.newExecuteStatementOperation(session, "wrong statement")
      .asInstanceOf[KyuubiClusterOperation]
    val e = intercept[KyuubiSQLException](ReflectUtils.invokeMethod(op, "execute"))
  }

  test("test call from backendService") {
    val be = server.beService
    val op = sessionMgr.getOperationMgr.newExecuteStatementOperation(session, statement)
      .asInstanceOf[KyuubiClusterOperation]
    ReflectUtils.invokeMethod(op, "execute")
    val metaDataColums = be.getResultSetMetadataResp(op.getHandle).getSchema.getColumns.asScala
    assert(metaDataColums.map(_.getColumnName).mkString(";") ===
      "database;tableName;isTemporary")
    val result = be.fetchResultsResp(op.getHandle, FETCH_NEXT, 10, FetchType.QUERY_OUTPUT)
      .getResults
    assert(result.isInstanceOf[TRowSet])
    val logs = be.fetchResultsResp(op.getHandle, FETCH_NEXT, 10, FetchType.LOG).getResults
    assert(logs.isInstanceOf[TRowSet])
  }

  test("test kyuubiClusterOperation with exception") {
    val op = sessionMgr.getOperationMgr.newExecuteStatementOperation(session, statement)
      .asInstanceOf[KyuubiClusterOperation]
    val client = ReflectUtils.getFieldValue(op, "client")
    val mockClient = mock[Iface]
    when(mockClient.GetResultSetMetadata(any(classOf[TGetResultSetMetadataReq])))
        .thenThrow(classOf[KyuubiSQLException])
    when(mockClient.FetchResults(any(classOf[TFetchResultsReq])))
      .thenThrow(classOf[KyuubiSQLException])

    ReflectUtils.setFieldValue(op, "client", null)
    var e = intercept[KyuubiSQLException](ReflectUtils.invokeMethod(op, "execute"))
    assert(e.getSQLState === "08S01")
    val resp1 = op.getResultSetMetaDataResp()
    assert(resp1.getStatus.getStatusCode === TStatusCode.ERROR_STATUS)

    ReflectUtils.setSuperField(op, "state", FINISHED)
    val resp2 = op.getResultResp(FETCH_NEXT, 10, FetchType.QUERY_OUTPUT)
    assert(resp2.getStatus.getStatusCode === TStatusCode.ERROR_STATUS)

    val resp3 = op.getResultResp(FETCH_NEXT, 10, FetchType.LOG)
    assert(resp3.getStatus.getStatusCode === TStatusCode.ERROR_STATUS)

    ReflectUtils.setFieldValue(op, "stmtHandle", new TOperationHandle())
    e = intercept[KyuubiSQLException](op.close())
    assert(e.getSQLState === "08S01")
    ReflectUtils.setFieldValue(op, "client", client)
  }
}
