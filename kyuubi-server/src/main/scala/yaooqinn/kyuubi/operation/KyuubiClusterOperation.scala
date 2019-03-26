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

import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAccessControlException
import org.apache.hive.service.cli.thrift._
import org.apache.hive.service.cli.thrift.TFetchResultsReq
import org.apache.hive.service.cli.thrift.TFetchResultsResp
import org.apache.hive.service.cli.thrift.TOperationState._
import org.apache.spark.KyuubiConf._
import org.apache.spark.KyuubiSparkUtil
import org.apache.spark.sql.types._
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import yaooqinn.kyuubi.KyuubiSQLException
import yaooqinn.kyuubi.cli.{FetchOrientation, FetchType}
import yaooqinn.kyuubi.schema.RowSet
import yaooqinn.kyuubi.session.KyuubiClusterSession
import yaooqinn.kyuubi.ui.KyuubiServerMonitor

class KyuubiClusterOperation(session: KyuubiClusterSession, statement: String)
  extends Operation(session, statement) {

  import KyuubiClusterOperation._
  import KyuubiClusterSession._

  private val conf = session.getConf
  private val userName = session.getUserName
  private val client = session.thriftClient
  private val transportLock = session.thriftLock
  private val sessHandle = session.thriftHandle
  private var stmtHandle: TOperationHandle = _

  protected val operationTimeout =
    KyuubiSparkUtil.timeStringAsMs(conf.get(OPERATION_IDLE_TIMEOUT))

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
        error(s"Error occured when fetch schema for $userName.")
        throw new KyuubiSQLException(e.toString, "08S01", e)
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
        error(s"Error occured when fetch result for $userName, statementId:$statementId.")
        throw new KyuubiSQLException(e.toString, "08S01", e)
    } finally {
      transportLock.unlock()
    }
  }

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
        case e: Exception =>
          throw new KyuubiSQLException(e.toString, "08S01", e)
      }
    }
    stmtHandle.isHasResultSet
  }

  private def getQueryLog(incremental: Boolean = true,
      fetchSize: Int = DEFAULT_FETCH_MAX_ROWS): List[String] = {
    if (!isClosedOrCanceled) {
      var successFlag = true
      val logs = new ListBuffer[String]
      var fetchLogResp: TFetchResultsResp = null
      transportLock.lock()
      try {
        if (stmtHandle != null) {
          val fetchLogReq = new TFetchResultsReq(
            stmtHandle, getFetchOrientation(incremental), fetchSize)
          fetchLogReq.setFetchType(1)
          fetchLogResp = client.FetchResults(fetchLogReq)
          verifySuccess(fetchLogResp.getStatus)
        } else {
          if (isClosedOrCanceled) {
            throw new KyuubiSQLException(s"Method getQueryLog() failed. " +
              s"The statement:$statementId has been closed or cancelled.", "08S01")
          } else if (checkState(ERROR)) {
            throw new KyuubiSQLException(s"Method getQueryLog() failed. " +
              s"Because the stmtHandle of  statement:$statementId is null and " +
              s"the statement execution might fail.", "08S01")
          } else {
            successFlag = false
          }
        }
      } catch {
        case e: Exception =>
          throw new KyuubiSQLException(e.toString, "08S01", e)
      } finally {
        transportLock.unlock()
      }
      if (successFlag) {
        val logRows = fetchLogResp.getResults.getRows.asScala
        for (log <- logRows) {
          logs += String.valueOf(log.getColVals.get(0))
        }
        logs.toList
      } else {
        logs.toList
      }
    } else {
      List.empty[String]
    }
  }

  def execute(): Unit = {
   var logThread: Thread = null
   try {
     statementId = UUID.randomUUID().toString
     info(s"Running query '$statement' with $statementId")
     setState(RUNNING)

     KyuubiServerMonitor.getListener(session.getUserName).foreach {
       _.onStatementStart(
         statementId,
         session.getSessionHandle.getSessionId.toString,
         statement,
         statementId,
         session.getUserName)
     }

     setHasResultSet(executeQuery(statement))
     setState(FINISHED)
     KyuubiServerMonitor.getListener(session.getUserName).foreach(_.onStatementFinish(statementId))
     // Get log after the execution complete, avoid conflict with client-get-log-thread
     logThread = createLogThread()
     logThread.start()
     logThread.join(DEFAULT_QUERY_PROGRESS_THREAD_TIMEOUT)
   } catch {
     case e: KyuubiSQLException =>
       if (!isClosedOrCanceled) {
         onStatementError(statementId, e.getMessage, KyuubiSparkUtil.exceptionString(e))
         throw e
       }
     case e: HiveAccessControlException =>
       if (!isClosedOrCanceled) {
         val err = KyuubiSparkUtil.exceptionString(e)
         onStatementError(statementId, e.getMessage, err)
         throw new KyuubiSQLException(e.getMessage, "HiveAccessControlException", 3000, e)
       }
     case e: Throwable =>
       if (!isClosedOrCanceled) {
         val err = KyuubiSparkUtil.exceptionString(e)
         onStatementError(statementId, e.getMessage, err)
         throw new KyuubiSQLException(e.toString, "<unknown>", 10000, e)
       }
   } finally {
     if (logThread != null && !logThread.isInterrupted) {
       logThread.interrupt()
       logThread.join(DEFAULT_QUERY_PROGRESS_THREAD_TIMEOUT)
       showRemainingLogsIfAny()
     }
   }
  }

  def createLogThread(): Thread = {
    val threadName = s"KyuubiCluster-Operation-RunnableLog-$userName-$statementId"
    new Thread(threadName) {
      override def run(): Unit = {
        // Reset the offset of log file
        try {
          var logs = getQueryLog(false)
          while (logs.size > 0) {
            // fetch the log periodically and ouput to logging
            for (log <- logs) {
              info(log)
            }
            Thread.sleep(DEFAULT_QUERY_PROGRESS_INTERVAL)
            logs = getQueryLog()
          }
        } catch {
          case e: KyuubiSQLException =>
            error(e.getMessage)
          case e: InterruptedException =>
            debug("Getting log thread is interrupted, since query is done!")
            showRemainingLogsIfAny()
        }
      }
    }
  }

  def showRemainingLogsIfAny(): Unit = {
    var logs: List[String] = List.empty
    do {
      try {
        logs = getQueryLog()
      } catch {
        case e: Exception =>
          error(e.toString)
      }
      for (log <- logs) {
        info(log)
      }
    } while (logs.size > 0)
  }
}

object KyuubiClusterOperation {
  val DEFAULT_FETCH_ORIENTATION: FetchOrientation = FetchOrientation.FETCH_NEXT
  val DEFAULT_FETCH_MAX_ROWS = 100

  private val DEFAULT_QUERY_PROGRESS_INTERVAL = 1000
  private val DEFAULT_QUERY_PROGRESS_THREAD_TIMEOUT = 10 * 1000

  private def getFetchOrientation(incremental: Boolean): TFetchOrientation = {
    if (incremental) {
      TFetchOrientation.FETCH_NEXT
    } else {
      TFetchOrientation.FETCH_FIRST
    }
  }
}
