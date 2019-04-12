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

package yaooqinn.kyuubi.operation

import java.util.UUID

import org.apache.hadoop.hive.ql.session.OperationLog
import org.apache.hive.service.cli.thrift._
import org.apache.hive.service.cli.thrift.TOperationState._
import org.apache.spark.KyuubiConf.OPERATION_IDLE_TIMEOUT
import org.apache.spark.KyuubiSparkUtil
import org.apache.spark.sql.types.StructType
import scala.collection.JavaConverters._

import yaooqinn.kyuubi.KyuubiSQLException
import yaooqinn.kyuubi.cli.{FetchOrientation, FetchType}
import yaooqinn.kyuubi.schema.RowSet
import yaooqinn.kyuubi.session.KyuubiClusterSession

class KyuubiClusterOperation(session: KyuubiClusterSession, statement: String)
  extends AbstractKyuubiOperation(session, statement) {

  import KyuubiClusterSession._

  private val conf = session.getConf
  private val userName = session.getUserName
  private val client = session.thriftClient
  private val transportLock = session.thriftLock
  private val sessHandle = session.thriftHandle
  private var stmtHandle: TOperationHandle = _

  override protected val operationTimeout =
    KyuubiSparkUtil.timeStringAsMs(conf.get(OPERATION_IDLE_TIMEOUT.key))

  override def close(): Unit = {
    super.close()
    transportLock.lock()
    try {
      if (stmtHandle != null) {
        val closeReq = new TCloseOperationReq(stmtHandle)
        val closeResp = client.CloseOperation(closeReq)
        verifySuccess(closeResp.getStatus)
      }
    } catch {
      case e: Exception =>
        throw new KyuubiSQLException(e.toString, "08S01", e)
    } finally {
      transportLock.unlock()
    }
    stmtHandle = null
  }

  override def getOperationLog: OperationLog = null

  override def getResultSetSchema: StructType = {
    throw new KyuubiSQLException("Method Not Implemented!")
  }

  override def getNextRowSet(order: FetchOrientation, rowSetSize: Long): RowSet = {
    throw new KyuubiSQLException("Method Not Implemented!")
  }

  def getResultSetMetaDataResp(): TGetResultSetMetadataResp = {
    val fetchReq = new TGetResultSetMetadataReq(stmtHandle)
    transportLock.lock()
    try {
      val fetchResp = client.GetResultSetMetadata(fetchReq)
      verifySuccess(fetchResp.getStatus)
      fetchResp
    } catch {
      case e: Exception =>
        warn("Error getting result set metadata: ", e)
        val resp = new TGetResultSetMetadataResp()
        resp.setStatus(KyuubiSQLException.toTStatus(e))
        resp
    } finally {
      transportLock.unlock()
    }
  }

  def getResultResp(order: FetchOrientation, rowSetSize: Long,
      fetchType: FetchType): TFetchResultsResp = {
    validateDefaultFetchOrientation(order)
    if (fetchType == FetchType.QUERY_OUTPUT) {
      assertState(FINISHED)
    }
    val fetchReq = new TFetchResultsReq(stmtHandle, order.toTFetchOrientation, rowSetSize)
    fetchReq.setFetchType(fetchType.toTFetchType)
    transportLock.lock()
    try {
      val fetchResp = client.FetchResults(fetchReq)
      verifySuccess(fetchResp.getStatus)
      fetchResp
    } catch {
      case e: Exception =>
        warn("Error fetching results: ", e)
        val resp = new TFetchResultsResp()
        resp.setStatus(KyuubiSQLException.toTStatus(e))
        resp
    } finally {
      transportLock.unlock()
    }
  }

  @throws[KyuubiSQLException]
  override def run(): Unit = {
    runInternal()
  }

  @throws[KyuubiSQLException]
  private def executeQuery(sql: String): Boolean = {
    val execReq = new TExecuteStatementReq(sessHandle, sql)
    execReq.setRunAsync(true)
    execReq.setConfOverlay(Map.empty[String, String].asJava)
    transportLock.lock()
    try {
      val execResp = client.ExecuteStatement(execReq)
      verifySuccess(execResp.getStatus)
      stmtHandle = execResp.getOperationHandle
    } catch {
      case e: Exception =>
        throw new KyuubiSQLException(e.toString, "08S01", e)
    } finally {
      transportLock.unlock()
    }
    val statusReq = new TGetOperationStatusReq(stmtHandle)
    var operationComplete = false
    var statusResp: TGetOperationStatusResp = null
    while (!operationComplete) {
      try {
        transportLock.lock()
        try {
          statusResp = client.GetOperationStatus(statusReq)
        } finally {
          transportLock.unlock()
        }
        verifySuccess(statusResp.getStatus)
        if (statusResp.isSetOperationState) {
          statusResp.getOperationState match {
            case CLOSED_STATE =>
              operationComplete = true
            case FINISHED_STATE =>
              operationComplete = true
            case CANCELED_STATE =>
              throw new KyuubiSQLException("Query was cancelled", "01000")
            case ERROR_STATE =>
              throw new KyuubiSQLException(statusResp.getErrorMessage,
                statusResp.getSqlState, statusResp.getErrorCode)
            case UKNOWN_STATE =>
              throw new KyuubiSQLException("UnKnown query", "HY000")
            case _ =>
          }
        }
      } catch {
        case e: KyuubiSQLException => throw e
        case e: Exception =>
          throw new KyuubiSQLException(e.toString, "08S01", e)
      }
    }
    stmtHandle.isHasResultSet
  }

  @throws[KyuubiSQLException]
  override protected def execute(): Unit = {
    try {
      statementId = UUID.randomUUID().toString
      info(s"Running query '$statement' with $statementId")
      setState(RUNNING)
      setHasResultSet(executeQuery(statement))
      setState(FINISHED)
    } catch {
      case e: KyuubiSQLException =>
        if (!isClosedOrCanceled) {
          val err = KyuubiSparkUtil.exceptionString(e)
          onStatementError(statementId, e.getMessage, err)
          throw e
        }
    }
  }
}
