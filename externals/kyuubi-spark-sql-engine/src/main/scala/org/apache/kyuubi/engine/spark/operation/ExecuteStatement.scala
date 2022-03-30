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

import java.util.concurrent.{RejectedExecutionException, ScheduledExecutorService, TimeUnit}

import scala.collection.JavaConverters._

import org.apache.spark.kyuubi.SQLOperationListener
import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.command.{DataWritingCommandExec, ExecutedCommandExec}
import org.apache.spark.sql.execution.datasources.InsertIntoDataSourceCommand
import org.apache.spark.sql.execution.datasources.v2.{AppendDataExecV1, OverwriteByExpressionExecV1, V2TableWriteExec}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types._

import org.apache.kyuubi.{KyuubiSQLException, Logging}
import org.apache.kyuubi.config.KyuubiConf.{OPERATION_PROGRESS_PERCENTAGE_INTERVAL, OPERATION_RESULT_MAX_ROWS}
import org.apache.kyuubi.engine.spark.KyuubiSparkUtil._
import org.apache.kyuubi.engine.spark.events.SparkOperationEvent
import org.apache.kyuubi.events.EventBus
import org.apache.kyuubi.operation.{ArrayFetchIterator, IterableFetchIterator, OperationState, OperationType}
import org.apache.kyuubi.operation.OperationState.OperationState
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.session.Session
import org.apache.kyuubi.util.ThreadUtils

class ExecuteStatement(
    session: Session,
    override val statement: String,
    override val shouldRunAsync: Boolean,
    queryTimeout: Long,
    incrementalCollect: Boolean)
  extends SparkOperation(OperationType.EXECUTE_STATEMENT, session) with Logging {
  private var lastGetProgressTime: Long = 0
  private val progressPercentageInterval: Long =
    spark.conf.get(
      OPERATION_PROGRESS_PERCENTAGE_INTERVAL.key,
      session.sessionManager.getConf.get(OPERATION_PROGRESS_PERCENTAGE_INTERVAL).toString).toLong

  private var statementTimeoutCleaner: Option[ScheduledExecutorService] = None

  private val operationLog: OperationLog = OperationLog.createOperationLog(session, getHandle)
  override def getOperationLog: Option[OperationLog] = Option(operationLog)

  private val operationListener: SQLOperationListener = new SQLOperationListener(this, spark)

  EventBus.post(SparkOperationEvent(this))

  override protected def resultSchema: StructType = {
    if (result == null || result.schema.isEmpty) {
      new StructType().add("Result", "string")
    } else {
      result.schema
    }
  }

  override protected def beforeRun(): Unit = {
    OperationLog.setCurrentOperationLog(operationLog)
    setState(OperationState.PENDING)
    setHasResultSet(true)
  }

  override protected def afterRun(): Unit = {
    progressPercentage = 1.0
    OperationLog.removeCurrentOperationLog()
  }

  private def updateProgressIfNeeded(): Unit = {
    val now = System.currentTimeMillis()
    if (progressPercentage != 1.0 && now - lastGetProgressTime > progressPercentageInterval) {
      val statusTracker = spark.sparkContext.statusTracker
      val jobIds = statusTracker.getJobIdsForGroup(statementId)
      val jobs = jobIds.flatMap { id => statusTracker.getJobInfo(id) }
      val stages = jobs.flatMap { job =>
        job.stageIds().flatMap(statusTracker.getStageInfo)
      }
      val taskCount = stages.map(_.numTasks()).sum
      val completedTaskCount = stages.map(_.numCompletedTasks).sum
      progressPercentage =
        if (taskCount == 0) {
          0.0
        } else {
          completedTaskCount.toDouble / taskCount
        }
      lastGetProgressTime = now
    }
  }

  override def getProgressPercentage: Double = {
    updateProgressIfNeeded()
    super.getProgressPercentage
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

  private def executeStatement(): Unit = withLocalProperties {
    try {
      setState(OperationState.RUNNING)
      info(diagnostics)
      Thread.currentThread().setContextClassLoader(spark.sharedState.jarClassLoader)
      // TODO: Make it configurable
      spark.sparkContext.addSparkListener(operationListener)
      result = spark.sql(statement)
      // TODO #921: COMPILED need consider eagerly executed commands
      setState(OperationState.COMPILED)
      debug(result.queryExecution)
      withMetrics(result.queryExecution)
      iter =
        if (incrementalCollect) {
          info("Execute in incremental collect mode")
          new IterableFetchIterator[Row](result.toLocalIterator().asScala.toIterable)
        } else {
          val resultMaxRows = spark.conf.getOption(OPERATION_RESULT_MAX_ROWS.key).map(_.toInt)
            .getOrElse(session.sessionManager.getConf.get(OPERATION_RESULT_MAX_ROWS))
          if (resultMaxRows <= 0) {
            info("Execute in full collect mode")
            new ArrayFetchIterator(result.collect())
          } else {
            info(s"Execute with max result rows[$resultMaxRows]")
            new ArrayFetchIterator(result.take(resultMaxRows))
          }
        }
      setState(OperationState.FINISHED)
    } catch {
      onError(cancel = true)
    } finally {
      statementTimeoutCleaner.foreach(_.shutdown())
    }
  }

  override protected def runInternal(): Unit = {
    addTimeoutMonitor()
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

  private def addTimeoutMonitor(): Unit = {
    if (queryTimeout > 0) {
      val timeoutExecutor =
        ThreadUtils.newDaemonSingleThreadScheduledExecutor("query-timeout-thread")
      timeoutExecutor.schedule(
        new Runnable {
          override def run(): Unit = {
            cleanup(OperationState.TIMEOUT)
          }
        },
        queryTimeout,
        TimeUnit.SECONDS)
      statementTimeoutCleaner = Some(timeoutExecutor)
    }
  }

  override def cleanup(targetState: OperationState): Unit = {
    spark.sparkContext.removeSparkListener(operationListener)
    super.cleanup(targetState)
  }

  override def setState(newState: OperationState): Unit = {
    super.setState(newState)
    EventBus.post(
      SparkOperationEvent(this, operationListener.getExecutionId))
  }
}
