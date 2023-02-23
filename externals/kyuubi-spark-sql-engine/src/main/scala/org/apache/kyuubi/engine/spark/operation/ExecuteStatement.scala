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
import org.apache.spark.kyuubi.SparkUtilsHelper.redact
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.execution.{QueryExecution, SQLExecution}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.command.{DataWritingCommandExec, ExecutedCommandExec}
import org.apache.spark.sql.execution.datasources.InsertIntoDataSourceCommand
import org.apache.spark.sql.execution.datasources.v2.{AppendDataExecV1, OverwriteByExpressionExecV1, V2TableWriteExec}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.kyuubi.SparkDatasetHelper
import org.apache.spark.sql.kyuubi.operation.ExecuteStatementHelper
import org.apache.spark.sql.types._

import org.apache.kyuubi.{KyuubiSQLException, Logging, Utils}
import org.apache.kyuubi.config.KyuubiConf.OPERATION_RESULT_MAX_ROWS
import org.apache.kyuubi.config.KyuubiEbayConf._
import org.apache.kyuubi.engine.spark.KyuubiSparkUtil._
import org.apache.kyuubi.engine.spark.session.SparkSessionImpl
import org.apache.kyuubi.operation.{ArrayFetchIterator, FetchIterator, IterableFetchIterator, OperationState}
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
  override protected def supportProgress: Boolean = true
  private var schema: StructType = _

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

  private def withMetrics(qe: QueryExecution): Unit =
    try {
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
    } catch {
      case e: Throwable => error("Error updating query metrics", e)
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
  private val tempTableCollectPartitionBytes =
    spark.conf.getOption(OPERATION_TEMP_TABLE_COLLECT_PARTITION_BYTES.key) match {
      case Some(s) => s.toLong
      case _ => session.sessionManager.getConf.get(OPERATION_TEMP_TABLE_COLLECT_PARTITION_BYTES)
    }
  private val tempTableCollectFileCoalesceNumThreshold =
    spark.conf.getOption(OPERATION_TEMP_TABLE_COLLECT_FILE_COALESCE_NUM_THRESHOLD.key) match {
      case Some(s) => s.toLong
      case _ =>
        session.sessionManager.getConf.get(OPERATION_TEMP_TABLE_COLLECT_FILE_COALESCE_NUM_THRESHOLD)
    }
  private val tempTableCollectSortLimitEnabled =
    spark.conf.getOption(OPERATION_TEMP_TABLE_COLLECT_SORT_LIMIT_ENABLED.key) match {
      case Some(s) => s.toBoolean
      case _ => session.sessionManager.getConf.get(OPERATION_TEMP_TABLE_COLLECT_SORT_LIMIT_ENABLED)
    }
  private val tempTableCollectSortLimitSize =
    spark.conf.getOption(OPERATION_TEMP_TABLE_COLLECT_SORT_LIMIT_SIZE.key) match {
      case Some(s) => s.toInt
      case _ => session.sessionManager.getConf.get(OPERATION_TEMP_TABLE_COLLECT_SORT_LIMIT_SIZE)
    }
  private val validIdentifier = statementId.replaceAll("-", "_")
  private var tempTableEnabled: Boolean = false
  private var tempTableName: String = _
  private var tempViewEnabled: Boolean = false
  private var tempViewName: String = _
  private var fileSystem: FileSystem = _
  private var tempTablePath: Path = _
  private var tempCoalescePath: Path = _
  private val filesMiniPartNum = spark.sessionState.conf.getConf(SQLConf.FILES_MIN_PARTITION_NUM)
  private val topKThreshold = spark.sessionState.conf.getConf(SQLConf.TOP_K_SORT_FALLBACK_THRESHOLD)
  private def setFilesMiniPartitionNum(minPartitionNum: Option[Int]): Unit = {
    spark.sessionState.conf.setConf(SQLConf.FILES_MIN_PARTITION_NUM, minPartitionNum)
  }
  private def setTopKThreshold(topKThreshold: Int): Unit = {
    spark.sessionState.conf.setConf(SQLConf.TOP_K_SORT_FALLBACK_THRESHOLD, topKThreshold)
  }
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
  private def dropTableIfExists(table: String): Unit = {
    val dropTempTable = s"DROP TABLE IF EXISTS $table"
    withStatement(dropTempTable)(spark.sql(dropTempTable))
  }
  private def clearTempTableOrViewIfNeeded(): Unit = {
    if (tempTableEnabled || tempViewEnabled) {
      Utils.tryLogNonFatalError {
        withLocalProperties[Unit] {
          Seq(tempTableName, tempViewName).filterNot(_ == null).foreach { table =>
            dropTableIfExists(table)
          }
        }

        Seq(tempTablePath, tempCoalescePath).filterNot(_ == null).foreach { path =>
          Option(fileSystem).foreach(_.delete(path, true))
        }
        setTopKThreshold(topKThreshold)
        setFilesMiniPartitionNum(filesMiniPartNum)
      }
    }
  }
  private def saveSortableQueryResult(): Unit = {
    var savedIntoTempTable = false

    if (tempTableCollectSortLimitEnabled) {
      try {
        val rowCount = ExecuteStatementHelper.existingLimit(result.queryExecution.sparkPlan)
          .map(_.toLong)
          .filter(_ < tempTableCollectMinFileSize)
          .getOrElse(result.count())
        if (rowCount < tempTableCollectSortLimitSize) {
          if (topKThreshold < tempTableCollectSortLimitSize) {
            setTopKThreshold(tempTableCollectSortLimitSize)
          }
          val topKStatement = s"SELECT * FROM($statement) LIMIT $rowCount"
          val sparkPlan = spark.sql(topKStatement).queryExecution.sparkPlan
          if (ExecuteStatementHelper.isTopKSort(sparkPlan)) {
            // topKSort(TakeOrderedAndProjectExec) can save the result into single file with order
            saveIntoTempTable(topKStatement)
            savedIntoTempTable = true
          }
        } else {
          info(s"The row count $rowCount exceeds the sort limit $tempTableCollectSortLimitSize")
        }
      } catch {
        case e: Throwable => error(s"Error saving the sort query into temp table", e)
      }
    }

    if (!savedIntoTempTable) {
      cacheIntoTempView(statement)
    }
  }
  private def cacheIntoTempView(statement: String): Unit = {
    info(s"Using temp view to cache the query result for $statement")
    tempViewEnabled = true
    tempViewName = s"kyuubi_cache_$validIdentifier"
    val createViewDDL = s"CREATE TEMPORARY VIEW $tempViewName AS $statement"
    val cacheViewDDL = s"CACHE TABLE $tempViewName"
    try {
      withStatement(createViewDDL)(spark.sql(createViewDDL))
      withStatement(cacheViewDDL)(spark.sql(cacheViewDDL))
      val readViewStatement = s"SELECT * FROM $tempViewName"
      result = withStatement(readViewStatement)(spark.sql(readViewStatement))
    } catch {
      case e: Throwable => error(s"Error creating view with $createViewDDL", e)
    }
  }
  private def saveIntoTempTable(statement: String): Unit = {
    info(s"Using temp table collect for $statement with min file size $tempTableCollectMinFileSize")
    tempTableEnabled = true
    tempTableName = s"$tempTableDb.kyuubi_temp_$validIdentifier"
    val partitionCol = s"kyuubi_$validIdentifier"
    val validColsSchema = StructType(result.schema.zipWithIndex.map { case (dt, index) =>
      val colType = dt.dataType match {
        case NullType => StringType
        case _ => dt.dataType
      }
      StructField(s"c$index", colType)
    })
    val tempTableSchemaString =
      (validColsSchema.fields ++ Seq(StructField(partitionCol, StringType))).map { sf =>
        s"${sf.name} ${sf.dataType.simpleString}"
      }.mkString(",")
    try {
      val sessionScratchDir = session.asInstanceOf[SparkSessionImpl].sessionScratchDir
      fileSystem = sessionScratchDir.getFileSystem(spark.sparkContext.hadoopConfiguration)
      if (!fileSystem.exists(sessionScratchDir)) {
        fileSystem.mkdirs(sessionScratchDir)
      }
      tempTablePath = new Path(sessionScratchDir, "temp_" + statementId)
      val tempTableDDL =
        s"""
           | CREATE TABLE $tempTableName ($tempTableSchemaString)
           | USING PARQUET
           | PARTITIONED BY ($partitionCol)
           | LOCATION '$tempTablePath'
           |""".stripMargin
      val insertTempTable =
        s"""
           |INSERT OVERWRITE TABLE $tempTableName PARTITION($partitionCol='$statementId')
           |($statement)
           |""".stripMargin
      withStatement(tempTableDDL)(spark.sql(tempTableDDL))
      withStatement(insertTempTable)(spark.sql(insertTempTable))
      val contentSummary = fileSystem.getContentSummary(tempTablePath)
      val dataSize = contentSummary.getLength
      val fileCount = contentSummary.getFileCount

      /**
       * The expected partition number to read results, used to prevent splitting many partitions.
       */
      val partitionNumber = math.max(dataSize / tempTableCollectPartitionBytes, 1).toInt
      setFilesMiniPartitionNum(Some(partitionNumber))

      val selectedColNames = validColsSchema.fields.map(_.name)
      if (fileCount <= tempTableCollectFileCoalesceNumThreshold ||
        dataSize / fileCount > tempTableCollectMinFileSize) {
        result = spark.sql(s"SELECT ${selectedColNames.mkString(",")} FROM $tempTableName")
      } else {
        tempCoalescePath = new Path(sessionScratchDir, "coalesce_" + statementId)
        withStatement(s"COALESCE $tempTableName WITH NUMBER $partitionNumber") {
          spark.read
            .parquet(tempTablePath.toString)
            .selectExpr(selectedColNames: _*)
            .coalesce(partitionNumber)
            .write
            .mode(SaveMode.Overwrite)
            .parquet(tempCoalescePath.toString)
        }
        result = spark.read
          .schema(validColsSchema)
          .parquet(tempCoalescePath.toString)
      }
    } catch {
      case e: Throwable =>
        error(s"Error creating temp table $tempTableName to save the result of $statement", e)
    }
  }

  protected def incrementalCollectResult(resultDF: DataFrame): Iterator[Any] = {
    resultDF.toLocalIterator().asScala
  }

  protected def fullCollectResult(resultDF: DataFrame): Array[_] = {
    resultDF.collect()
  }

  protected def takeResult(resultDF: DataFrame, maxRows: Int): Array[_] = {
    resultDF.take(maxRows)
  }

  protected def executeStatement(): Unit = withLocalProperties {
    try {
      setState(OperationState.RUNNING)
      info(diagnostics)
      Thread.currentThread().setContextClassLoader(spark.sharedState.jarClassLoader)
      addOperationListener()
      result = spark.sql(statement)
      schema = result.schema
      // only save to temp table for incremental collect mode
      if (tempTableCollect && incrementalCollect && ExecuteStatementHelper.isDQL(statement)) {
        if (ExecuteStatementHelper.sortable(result.queryExecution.sparkPlan)) {
          saveSortableQueryResult()
        } else {
          saveIntoTempTable(statement)
        }
      }
      withMetrics(result.queryExecution)
      iter = collectAsIterator(result)
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
      setState(OperationState.COMPILED)
      if (lastAccessCompiledTime > 0L) {
        lastAccessTime = lastAccessCompiledTime
      }
    }
  }

  override def close(): Unit = {
    clearTempTableOrViewIfNeeded()
    super.close()
  }

  override def getResultSetMetadataHints(): Seq[String] =
    Seq(
      s"__kyuubi_operation_result_format__=$resultFormat",
      s"__kyuubi_operation_result_arrow_timestampAsString__=$timestampAsString")

  private def collectAsIterator(resultDF: DataFrame): FetchIterator[_] = {
    val resultMaxRows = spark.conf.getOption(OPERATION_RESULT_MAX_ROWS.key).map(_.toInt)
      .orElse(spark.conf.getOption(EBAY_OPERATION_MAX_RESULT_COUNT.key).map(_.toInt))
      .getOrElse(session.sessionManager.getConf.get(OPERATION_RESULT_MAX_ROWS))
    if (incrementalCollect) {
      if (resultMaxRows > 0) {
        warn(s"Ignore ${OPERATION_RESULT_MAX_ROWS.key} on incremental collect mode.")
      }
      info("Execute in incremental collect mode")
      new IterableFetchIterator[Any](new Iterable[Any] {
        override def iterator: Iterator[Any] = incrementalCollectResult(resultDF)
      })
    } else {
      val internalArray = if (resultMaxRows <= 0) {
        info("Execute in full collect mode")
        fullCollectResult(resultDF)
      } else {
        info(s"Execute with max result rows[$resultMaxRows]")
        takeResult(resultDF, resultMaxRows)
      }
      new ArrayFetchIterator(internalArray)
    }
  }
}

class ArrowBasedExecuteStatement(
    session: Session,
    override val statement: String,
    override val shouldRunAsync: Boolean,
    queryTimeout: Long,
    incrementalCollect: Boolean)
  extends ExecuteStatement(session, statement, shouldRunAsync, queryTimeout, incrementalCollect) {

  override protected def incrementalCollectResult(resultDF: DataFrame): Iterator[Any] = {
    collectAsArrow(convertComplexType(resultDF)) { rdd =>
      rdd.toLocalIterator
    }
  }

  override protected def fullCollectResult(resultDF: DataFrame): Array[_] = {
    collectAsArrow(convertComplexType(resultDF)) { rdd =>
      rdd.collect()
    }
  }

  override protected def takeResult(resultDF: DataFrame, maxRows: Int): Array[_] = {
    // this will introduce shuffle and hurt performance
    val limitedResult = resultDF.limit(maxRows)
    collectAsArrow(convertComplexType(limitedResult)) { rdd =>
      rdd.collect()
    }
  }

  /**
   * refer to org.apache.spark.sql.Dataset#withAction(), assign a new execution id for arrow-based
   * operation, so that we can track the arrow-based queries on the UI tab.
   */
  private def collectAsArrow[T](df: DataFrame)(action: RDD[Array[Byte]] => T): T = {
    SQLExecution.withNewExecutionId(df.queryExecution, Some("collectAsArrow")) {
      df.queryExecution.executedPlan.resetMetrics()
      action(SparkDatasetHelper.toArrowBatchRdd(df))
    }
  }

  override protected def isArrowBasedOperation: Boolean = true

  override val resultFormat = "arrow"

  private def convertComplexType(df: DataFrame): DataFrame = {
    SparkDatasetHelper.convertTopLevelComplexTypeToHiveString(df, timestampAsString)
  }

}
