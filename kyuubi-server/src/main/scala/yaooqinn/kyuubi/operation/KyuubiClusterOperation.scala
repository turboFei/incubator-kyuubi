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

import java.sql.{ResultSet, SQLException, SQLWarning, Statement}
import java.util.UUID

import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAccessControlException
import org.apache.hive.jdbc.HiveStatement
import org.apache.spark.KyuubiConf._
import org.apache.spark.KyuubiSparkUtil
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import scala.collection.JavaConverters._

import yaooqinn.kyuubi.{KyuubiSQLException}
import yaooqinn.kyuubi.cli.FetchOrientation
import yaooqinn.kyuubi.schema.{RowSet, RowSetBuilder}
import yaooqinn.kyuubi.session.KyuubiClusterSession
import yaooqinn.kyuubi.ui.KyuubiServerMonitor

class KyuubiClusterOperation(session: KyuubiClusterSession, statement: String)
  extends Operation(session, statement) {

  import KyuubiClusterOperation._

  private val hiveConnection = session.getHiveConnection()
  private val conf = session.getConf
  private val userName = session.getUserName

  protected val operationTimeout =
    KyuubiSparkUtil.timeStringAsMs(conf.get(OPERATION_IDLE_TIMEOUT))

  private var result: ResultSet = _

  def close(): Unit = {
    // RDDs will be cleaned automatically upon garbage collection.
    debug(s"CLOSING $statementId")
    cleanup(CLOSED)
    cleanupOperationLog()
    if (hiveConnection != null && !hiveConnection.isClosed) {
      hiveConnection.close()
    }
  }

  def getResultSetSchema: StructType = if (result == null || result.getMetaData == null) {
    new StructType().add("Result", "string")
  } else {
    val metaData = result.getMetaData
    val schema = new StructType()
    metaData.getColumnType(1)
    for (i <- (1 to metaData.getColumnCount)) {
      schema.add(metaData.getColumnName(i), metaData.getColumnTypeName(i),
        metaData.isNullable(i) == 1)
    }
    schema
  }

  def getNextRowSet(order: FetchOrientation, rowSetSize: Long): RowSet = {
    validateDefaultFetchOrientation(order)
    assertState(FINISHED)
    setHasResultSet(true)
    RowSetBuilder.create(getResultSetSchema, hiveResultToRows(result), session.getProtocolVersion)
  }

  def hiveResultToRows(result: ResultSet): Seq[Row] = {
    val count = result.getRow
    val columns = result.getMetaData.getColumnCount
    val rows = new Array[Row](count)
    var i = 0
    while (result.next()) {
      val rowColumns = new Array[Object](columns)
      for (j <- (0 until columns)) {
        rowColumns(j) = result.getObject(j + 1)
      }
      rows(i) = Row.fromSeq(rowColumns.toSeq)
      i += 1
    }
    rows.toSeq
  }

  def execute(): Unit = {
   var hiveStatement: Statement = null
   var logThread: Thread = null
   try {
     statementId = UUID.randomUUID().toString
     info(s"Running query '$statement' with $statementId")
     setState(RUNNING)

     var hasResult = false
     KyuubiServerMonitor.getListener(session.getUserName).foreach {
       _.onStatementStart(
         statementId,
         session.getSessionHandle.getSessionId.toString,
         statement,
         statementId,
         session.getUserName)
     }
     hiveStatement = hiveConnection.createStatement()
     logThread = createLogThread(hiveStatement.asInstanceOf[HiveStatement])
     logThread.start()
     hasResult = hiveStatement.execute(statement)
     logThread.interrupt()
     logThread.join(DEFAULT_QUERY_PROGRESS_THREAD_TIMEOUT)

     if (hasResult) {
       result = hiveStatement.getResultSet
     }
     setState(FINISHED)
     KyuubiServerMonitor.getListener(session.getUserName).foreach(_.onStatementFinish(statementId))
   } catch {
     case e: KyuubiSQLException =>
       if (!isClosedOrCanceled) {
         onStatementError(statementId, e.getMessage, KyuubiSparkUtil.exceptionString(e))
         throw e
       }
     case e: SQLException =>
       if (!isClosedOrCanceled) {
         onStatementError(statementId, e.getMessage, KyuubiSparkUtil.exceptionString(e))
         throw new KyuubiSQLException(e.getMessage, "HiveStatementExecutionException", 2020, e)
       }
     case e: HiveAccessControlException =>
       if (!isClosedOrCanceled) {
         onStatementError(statementId, e.getMessage, KyuubiSparkUtil.exceptionString(e))
         throw new KyuubiSQLException(e.getMessage, "HiveAccessControlException", 3000, e)
       }
     case e: Throwable =>
       if (!isClosedOrCanceled) {
         onStatementError(statementId, e.getMessage, KyuubiSparkUtil.exceptionString(e))
         throw new KyuubiSQLException(e.toString, "<unknown>", 10000, e)
       }
   } finally {
     if (logThread != null && !logThread.isInterrupted) {
       logThread.interrupt()
       logThread.join(DEFAULT_QUERY_PROGRESS_THREAD_TIMEOUT)
       showRemainingLogsIfAny(hiveStatement.asInstanceOf[HiveStatement])
     }
     if (hiveStatement != null) {
       hiveStatement.close()
     }
   }
  }

  def createLogThread(statement: HiveStatement): Thread = {
    val threadName = s"HiveStatement-RunnableLog-$userName-$statementId"
    new Thread(threadName) {
      override def run(): Unit = {
        while (statement.hasMoreLogs) {
          try {
            // fetch the log periodically and ouput to logging
            for (log <- statement.getQueryLog.asScala) {
              info(log)
            }
            Thread.sleep(DEFAULT_QUERY_PROGRESS_INTERVAL)
          } catch {
            case e: SQLException =>
              error(new SQLWarning(e))
            case e: InterruptedException =>
              debug("Getting log thread is interrupted, since query is done!")
              showRemainingLogsIfAny(statement)
          }
        }
      }
    }
  }

  def showRemainingLogsIfAny(statement: HiveStatement): Unit = {
    var logs: Array[String] = Array.empty
    do {
      try {
        logs = statement.getQueryLog.asScala.toArray
      } catch {
        case e: SQLException =>
          error(new SQLWarning(e))
      }
      for (log <- logs) {
        info(log)
      }
    } while (logs.size > 0)
  }

  def cleanup(state: OperationState) {
    if (this.state != CLOSED) {
      setState(state)
    }
    val backgroundHandle = getBackgroundHandle
    if (backgroundHandle != null) {
      backgroundHandle.cancel(true)
    }
    if (hiveConnection != null && !hiveConnection.isClosed) {
      hiveConnection.close()
    }
  }
}

object KyuubiClusterOperation {
  private val DEFAULT_QUERY_PROGRESS_INTERVAL = 1000
  private val DEFAULT_QUERY_PROGRESS_THREAD_TIMEOUT = 10 * 1000
}
