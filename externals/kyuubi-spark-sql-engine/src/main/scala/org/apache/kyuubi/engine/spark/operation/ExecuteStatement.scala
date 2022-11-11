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

package org.apache.kyuubi.engine.spark.operation

import java.util.concurrent.RejectedExecutionException

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hive.service.rpc.thrift.TProgressUpdateResp
import org.apache.spark.kyuubi.{SparkProgressMonitor, SQLOperationListener}
import org.apache.spark.kyuubi.SparkUtilsHelper.redact
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.command.{DataWritingCommandExec, ExecutedCommandExec}
import org.apache.spark.sql.execution.datasources.InsertIntoDataSourceCommand
import org.apache.spark.sql.execution.datasources.v2.{AppendDataExecV1, OverwriteByExpressionExecV1, V2TableWriteExec}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.kyuubi.operation.{ExecuteStatementHelper, KyuubiOperationHelper}
import org.apache.spark.sql.types._

import org.apache.kyuubi.{KyuubiSQLException, Logging, Utils}
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.config.KyuubiEbayConf._
import org.apache.kyuubi.engine.spark.KyuubiSparkUtil._
import org.apache.kyuubi.engine.spark.events.SparkOperationEvent
import org.apache.kyuubi.engine.spark.session.SparkSessionImpl
import org.apache.kyuubi.events.EventBus
import org.apache.kyuubi.operation.{ArrayFetchIterator, IterableFetchIterator, OperationState, OperationStatus}
import org.apache.kyuubi.operation.OperationState.OperationState
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.session.Session

class ExecuteStatement(
    session: Session,
    override val statement: String,
    override val shouldRunAsync: Boolean,
    queryTimeout: Long,
    incrementalCollect: Boolean)
  extends SparkOperation(session) with Logging {

  private val operationLog: OperationLog = OperationLog.createOperationLog(session, getHandle)
  override def getOperationLog: Option[OperationLog] = Option(operationLog)
  private var schema: StructType = _

  private val operationSparkListenerEnabled =
    spark.conf.getOption(OPERATION_SPARK_LISTENER_ENABLED.key) match {
      case Some(s) => s.toBoolean
      case _ => session.sessionManager.getConf.get(OPERATION_SPARK_LISTENER_ENABLED)
    }

  private val operationListener: Option[SQLOperationListener] =
    if (operationSparkListenerEnabled) {
      Some(new SQLOperationListener(this, spark))
    } else {
      None
    }

  private val progressEnable = spark.conf.getOption(SESSION_PROGRESS_ENABLE.key) match {
    case Some(s) => s.toBoolean
    case _ => session.sessionManager.getConf.get(SESSION_PROGRESS_ENABLE)
  }

  private val progressPlanEnable = spark.conf.getOption(SESSION_PROGRESS_PLAN_ENABLE.key) match {
    case Some(s) => s.toBoolean
    case _ => session.sessionManager.getConf.get(SESSION_PROGRESS_PLAN_ENABLE)
  }

  EventBus.post(SparkOperationEvent(this))

  override protected def resultSchema: StructType = {
    if (schema == null || schema.isEmpty) {
      new StructType().add("Result", "string")
    } else {
      schema
    }
  }

  override protected def beforeRun(): Unit = {
    OperationLog.setCurrentOperationLog(operationLog)
    setState(OperationState.PENDING)
    setHasResultSet(true)
  }

  override protected def afterRun(): Unit = {
    OperationLog.removeCurrentOperationLog()
  }

  def setNumOutputRows(metrics: Map[String, SQLMetric], key: String): Unit = {
    if (metrics != null) {
      val modifiedRowCount = metrics.get(key).map(_.value).getOrElse(0L)
      info(s"$modifiedRowCount rows inserted/updated/deleted/merged by $statementId")
      numModifiedRows = modifiedRowCount
    }
  }

  private def withMetrics(qe: QueryExecution): Unit = {
    val checkedSparkPlan = qe.executedPlan match {
      case a: AdaptiveSparkPlanExec =>
        a.executedPlan
      case plan => plan
    }
    checkedSparkPlan match {
      case DataWritingCommandExec(cmd, _) =>
        setNumOutputRows(cmd.metrics, "numOutputRows")
      case ExecutedCommandExec(cmd) =>
        cmd match {
          case c: InsertIntoDataSourceCommand =>
            setNumOutputRows(c.metrics, "numOutputRows")
          case _ => // TODO: Support delta Update(WithJoin)Command/Delete(WithJoin)Command
        }
      case a: AppendDataExecV1 =>
        setNumOutputRows(a.metrics, "numOutputRows")
      case a: OverwriteByExpressionExecV1 =>
        setNumOutputRows(a.metrics, "numOutputRows")
      case a: V2TableWriteExec =>
        setNumOutputRows(a.metrics, "numOutputRows")
      case _ =>
    }
  }

  // temp table collect
  private val tempTableDb =
    spark.conf.getOption(OPERATION_TEMP_TABLE_DATABASE.key) match {
      case Some(db) => db
      case _ => session.sessionManager.getConf.get(OPERATION_TEMP_TABLE_DATABASE)
    }
  private val tempTableCollect =
    spark.conf.getOption(OPERATION_TEMP_TABLE_COLLECT.key) match {
      case Some(s) => s.toBoolean
      case _ => session.sessionManager.getConf.get(OPERATION_TEMP_TABLE_COLLECT)
    }
  private val tempTableCollectMinFileSize =
    spark.conf.getOption(OPERATION_TEMP_TABLE_COLLECT_MIN_FILE_SIZE.key) match {
      case Some(s) => s.toLong
      case _ => session.sessionManager.getConf.get(OPERATION_TEMP_TABLE_COLLECT_MIN_FILE_SIZE)
    }
  private val tempTableCollectFileCoalesceSize =
    spark.conf.getOption(OPERATION_TEMP_TABLE_COLLECT_FILE_COALESCE_SIZE.key) match {
      case Some(s) => s.toLong
      case _ => session.sessionManager.getConf.get(OPERATION_TEMP_TABLE_COLLECT_FILE_COALESCE_SIZE)
    }
  private val tempTableId =
    TableIdentifier(s"kyuubi_temp_$statementId".replaceAll("-", "_"), Some(tempTableDb))
  private val tempTableName: String = tempTableId.unquotedString
  private var tempTableEnabled: Boolean = false
  private var fileSystem: FileSystem = _
  private var tempCoalescePath: Path = _
  private val tempViewName = s"kyuubi_cache_$statementId".replaceAll("-", "_")
  private var tempViewEnabled: Boolean = false
  private def withStatement[T](statement: String)(f: => T): T = {
    spark.sparkContext.setJobDescription(redact(
      spark.sessionState.conf.stringRedactionPattern,
      statement))
    try {
      f
    } finally {
      spark.sparkContext.setJobDescription(redactedStatement)
    }
  }
  private def clearTempTableOrViewIfNeeded(): Unit = {
    if (tempTableEnabled || tempViewEnabled) {
      Utils.tryLogNonFatalError {
        withLocalProperties[Unit] {
          val toDrop = if (tempTableEnabled) tempTableName else tempViewName
          val dropTempTable = s"DROP TABLE IF EXISTS $toDrop"
          withStatement(dropTempTable)(spark.sql(dropTempTable))
        }

        Option(tempCoalescePath).foreach { _ =>
          Option(fileSystem).foreach(_.delete(tempCoalescePath, true))
        }
      }
    }
  }

  private def executeStatement(): Unit = withLocalProperties {
    try {
      setState(OperationState.RUNNING)
      info(diagnostics)
      Thread.currentThread().setContextClassLoader(spark.sharedState.jarClassLoader)
      operationListener.foreach(spark.sparkContext.addSparkListener(_))
      result = spark.sql(statement)
      schema = result.schema
      // only save to temp table for incremental collect mode
      if (tempTableCollect && incrementalCollect && ExecuteStatementHelper.isDQL(statement)) {
        if (KyuubiOperationHelper.isInMemoryTableScan(result.queryExecution.sparkPlan)) {
          info("The query is already using in memory table, skip to use temp table.")
        } else if (KyuubiOperationHelper.sortable(result.queryExecution.sparkPlan)) {

          /**
           * If the query is sortable, we shall use temporary view instead of temporary table.
           */
          info(s"Using $tempViewName to cache the query result")
          tempViewEnabled = true
          val createViewDDL = s"CREATE TEMPORARY VIEW $tempViewName AS $statement"
          val cacheViewDDL = s"CACHE TABLE $tempViewName"
          try {
            withStatement(createViewDDL)(spark.sql(createViewDDL))
            withStatement(cacheViewDDL)(spark.sql(cacheViewDDL))
            result = spark.sql(s"SELECT * FROM $tempViewName")
          } catch {
            case e: Throwable => error(s"Error creating view with $createViewDDL", e)
          }
        } else {
          info(s"Using temp table collect with min file size $tempTableCollectMinFileSize")
          tempTableEnabled = true
          val tempTableSchema = result.schema.zipWithIndex.map { case (dt, index) =>
            val colType = dt.dataType match {
              case NullType => StringType.simpleString
              case _ => dt.dataType.simpleString
            }
            s"c$index $colType"
          }.mkString(",")
          try {
            val tempTableDDL = s"CREATE TEMPORARY TABLE $tempTableName ($tempTableSchema)"
            val insertTempTable = s"INSERT OVERWRITE $tempTableName $statement"
            withStatement(tempTableDDL)(spark.sql(tempTableDDL))
            withStatement(insertTempTable)(spark.sql(insertTempTable))

            val tempTableMetadata = spark.sessionState.catalog.getTableMetadata(tempTableId)
            val tempTablePath = new Path(tempTableMetadata.location)
            fileSystem = tempTablePath.getFileSystem(spark.sparkContext.hadoopConfiguration)
            val contentSummary = fileSystem.getContentSummary(tempTablePath)
            val dataSize = contentSummary.getLength
            val fileCount = contentSummary.getFileCount

            if (dataSize / fileCount > tempTableCollectMinFileSize) {
              result = spark.sql(s"SELECT * FROM $tempTableName")
            } else {
              val coalesceNum = math.max(dataSize / tempTableCollectFileCoalesceSize, 1).toInt
              val sessionScratchDir = session.asInstanceOf[SparkSessionImpl].sessionScratchDir
              if (!fileSystem.exists(sessionScratchDir)) {
                fileSystem.mkdirs(sessionScratchDir)
              }
              tempCoalescePath = new Path(sessionScratchDir, "coalesce_" + statementId)
              withStatement(s"COALESCE $tempTableName WITH NUMBER $coalesceNum") {
                spark.read
                  .schema(tempTableMetadata.schema)
                  .parquet(tempTablePath.toString)
                  .coalesce(coalesceNum)
                  .write
                  .mode(SaveMode.Overwrite)
                  .parquet(tempCoalescePath.toString)
              }
              result = spark.read
                .schema(tempTableMetadata.schema)
                .parquet(tempCoalescePath.toString)
            }
          } catch {
            case e: Throwable =>
              error(
                s"Error creating temp table $tempTableName to save the result of $statement",
                e)
          }
        }
      }
      withMetrics(result.queryExecution)
      iter =
        if (incrementalCollect) {
          info("Execute in incremental collect mode")
          new IterableFetchIterator[Row](result.toLocalIterator().asScala.toIterable)
        } else {
          val resultMaxRows = spark.conf.getOption(OPERATION_RESULT_MAX_ROWS.key).map(_.toInt)
            .orElse(spark.conf.getOption(EBAY_OPERATION_MAX_RESULT_COUNT.key).map(_.toInt))
            .getOrElse(session.sessionManager.getConf.get(OPERATION_RESULT_MAX_ROWS))
          if (resultMaxRows <= 0) {
            info("Execute in full collect mode")
            new ArrayFetchIterator(result.collect())
          } else {
            info(s"Execute with max result rows[$resultMaxRows]")
            new ArrayFetchIterator(result.take(resultMaxRows))
          }
        }
      setCompiledStateIfNeeded()
      setState(OperationState.FINISHED)
    } catch {
      onError(cancel = true)
    } finally {
      shutdownTimeoutMonitor()
    }
  }

  override protected def runInternal(): Unit = {
    addTimeoutMonitor(queryTimeout)
    if (shouldRunAsync) {
      val asyncOperation = new Runnable {
        override def run(): Unit = {
          OperationLog.setCurrentOperationLog(operationLog)
          executeStatement()
        }
      }

      try {
        val sparkSQLSessionManager = session.sessionManager
        val backgroundHandle = sparkSQLSessionManager.submitBackgroundOperation(asyncOperation)
        setBackgroundHandle(backgroundHandle)
      } catch {
        case rejected: RejectedExecutionException =>
          setState(OperationState.ERROR)
          val ke =
            KyuubiSQLException("Error submitting query in background, query rejected", rejected)
          setOperationException(ke)
          throw ke
      }
    } else {
      executeStatement()
    }
  }

  override def cleanup(targetState: OperationState): Unit = {
    operationListener.foreach(_.cleanup())
    super.cleanup(targetState)
  }

  override def setState(newState: OperationState): Unit = {
    super.setState(newState)
    EventBus.post(
      SparkOperationEvent(this, operationListener.flatMap(_.getExecutionId)))
  }

  override def getStatus: OperationStatus = {
    if (progressEnable) {
      val progressMonitor = new SparkProgressMonitor(spark, statementId, progressPlanEnable)
      setOperationJobProgress(new TProgressUpdateResp(
        progressMonitor.headers,
        progressMonitor.rows,
        progressMonitor.progressedPercentage,
        progressMonitor.executionStatus,
        progressMonitor.footerSummary,
        startTime))
    }
    super.getStatus
  }

  def setCompiledStateIfNeeded(): Unit = synchronized {
    if (getStatus.state == OperationState.RUNNING) {
      val lastAccessCompiledTime =
        if (result != null) {
          val phase = result.queryExecution.tracker.phases
          if (phase.contains("parsing") && phase.contains("planning")) {
            val compiledTime = phase("planning").endTimeMs - phase("parsing").startTimeMs
            lastAccessTime + compiledTime
          } else {
            0L
          }
        } else {
          0L
        }
      super.setState(OperationState.COMPILED)
      if (lastAccessCompiledTime > 0L) {
        lastAccessTime = lastAccessCompiledTime
      }
      EventBus.post(
        SparkOperationEvent(this, operationListener.flatMap(_.getExecutionId)))
    }
  }

  override def close(): Unit = {
    clearTempTableOrViewIfNeeded()
    super.close()
  }
}
