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

import java.io.File
import java.util.UUID

import scala.collection.JavaConverters._

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.{FileUtil, Path}
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAccessControlException
import org.apache.spark.KyuubiConf._
import org.apache.spark.KyuubiSparkUtil
import org.apache.spark.scheduler.cluster.KyuubiSparkExecutorUtils
import org.apache.spark.sql.{AnalysisException, DataFrame, Row, SparkSQLUtils}
import org.apache.spark.sql.catalyst.catalog.FunctionResource
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.execution.command.AddJarCommand
import org.apache.spark.sql.types._

import yaooqinn.kyuubi.{KyuubiSQLException, Logging}
import yaooqinn.kyuubi.cli.FetchOrientation
import yaooqinn.kyuubi.schema.{RowSet, RowSetBuilder}
import yaooqinn.kyuubi.session.KyuubiSession
import yaooqinn.kyuubi.ui.KyuubiServerMonitor
import yaooqinn.kyuubi.utils.ReflectUtils

class KyuubiOperation(session: KyuubiSession, statement: String)
  extends Operation(session, statement) {

  import KyuubiOperation._

  private val sparkSession = session.sparkSession
  private val conf = sparkSession.conf

  protected val operationTimeout =
    KyuubiSparkUtil.timeStringAsMs(conf.get(OPERATION_IDLE_TIMEOUT))

  private var result: DataFrame = _
  private var iter: Iterator[Row] = _

  private val incrementalCollect: Boolean = conf.get(OPERATION_INCREMENTAL_COLLECT).toBoolean

  def close(): Unit = {
    // RDDs will be cleaned automatically upon garbage collection.
    debug(s"CLOSING $statementId")
    cleanup(CLOSED)
    cleanupOperationLog()
    sparkSession.sparkContext.clearJobGroup()
  }

  def getResultSetSchema: StructType = if (result == null || result.schema.isEmpty) {
    new StructType().add("Result", "string")
  } else {
    result.schema
  }

  def getNextRowSet(order: FetchOrientation, rowSetSize: Long): RowSet = {
    validateDefaultFetchOrientation(order)
    assertState(FINISHED)
    setHasResultSet(true)
    val taken = if (order == FetchOrientation.FETCH_FIRST) {
      result.toLocalIterator().asScala.take(rowSetSize.toInt)
    } else {
      iter.take(rowSetSize.toInt)
    }
    RowSetBuilder.create(getResultSetSchema, taken.toSeq, session.getProtocolVersion)
  }

  def localizeAndAndResource(path: String): Unit = try {
    if (isResourceDownloadable(path)) {
      val src = new Path(path)
      val destFileName = src.getName
      val destFile =
        new File(session.getResourcesSessionDir, destFileName).getCanonicalPath
      val fs = src.getFileSystem(sparkSession.sparkContext.hadoopConfiguration)
      fs.copyToLocalFile(src, new Path(destFile))
      FileUtil.chmod(destFile, "ugo+rx", true)
      AddJarCommand(destFile).run(session.sparkSession)
    }
  } catch {
    case e: Exception => throw new KyuubiSQLException(s"Failed to read external resource: $path", e)
  }

   def execute(): Unit = {
    try {
      statementId = UUID.randomUUID().toString
      info(s"Running query '$statement' with $statementId")
      setState(RUNNING)

      val classLoader = SparkSQLUtils.getUserJarClassLoader(sparkSession)
      Thread.currentThread().setContextClassLoader(classLoader)

      KyuubiServerMonitor.getListener(session.getUserName).foreach {
        _.onStatementStart(
          statementId,
          session.getSessionHandle.getSessionId.toString,
          statement,
          statementId,
          session.getUserName)
      }
      sparkSession.sparkContext.setJobGroup(statementId, statement)
      KyuubiSparkUtil.setActiveSparkContext(sparkSession.sparkContext)

      val parsedPlan = SparkSQLUtils.parsePlan(sparkSession, statement)
      parsedPlan match {
        case c if c.nodeName == "CreateFunctionCommand" =>
          val resources =
            ReflectUtils.getFieldValue(c, "resources").asInstanceOf[Seq[FunctionResource]]
          resources.foreach { case FunctionResource(_, uri) =>
            localizeAndAndResource(uri)
          }
        case a if a.nodeName == "AddJarCommand" =>
          val path = ReflectUtils.getFieldValue(a, "path").asInstanceOf[String]
          localizeAndAndResource(path)
        case _ =>
      }
      result = SparkSQLUtils.toDataFrame(sparkSession, parsedPlan)
      KyuubiServerMonitor.getListener(session.getUserName).foreach {
        _.onStatementParsed(statementId, result.queryExecution.toString())
      }

      if (conf.get(BACKEND_SESSION_LONG_CACHE).toBoolean &&
        KyuubiSparkUtil.classIsLoadable(conf.get(BACKEND_SESSION_TOKEN_UPDATE_CLASS))) {
        KyuubiSparkExecutorUtils.populateTokens(sparkSession.sparkContext, session.ugi)
      }
      debug(result.queryExecution.toString())
      iter = if (incrementalCollect) {
        info("Executing query in incremental collection mode")
        result.toLocalIterator().asScala
      } else {
        val resultLimit = conf.get(OPERATION_RESULT_LIMIT).toInt
        if (resultLimit >= 0) {
          result.take(resultLimit).toList.toIterator
        } else {
          result.collect().toList.iterator
        }
      }
      setState(FINISHED)
      KyuubiServerMonitor.getListener(session.getUserName).foreach(_.onStatementFinish(statementId))
    } catch {
      case e: KyuubiSQLException =>
        if (!isClosedOrCanceled) {
          onStatementError(statementId, e.getMessage, KyuubiSparkUtil.exceptionString(e))
          throw e
        }
      case e: ParseException =>
        if (!isClosedOrCanceled) {
          onStatementError(
            statementId, e.withCommand(statement).getMessage, KyuubiSparkUtil.exceptionString(e))
          throw new KyuubiSQLException(
            e.withCommand(statement).getMessage, "ParseException", 2000, e)
        }
      case e: AnalysisException =>
        if (!isClosedOrCanceled) {
          onStatementError(statementId, e.getMessage, KyuubiSparkUtil.exceptionString(e))
          throw new KyuubiSQLException(e.getMessage, "AnalysisException", 2001, e)
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
      if (statementId != null) {
        sparkSession.sparkContext.cancelJobGroup(statementId)
      }
    }
  }

  def cleanup(state: OperationState) {
    if (this.state != CLOSED) {
      setState(state)
    }
    val backgroundHandle = getBackgroundHandle
    if (backgroundHandle != null) {
      backgroundHandle.cancel(true)
    }
    if (statementId != null) {
      sparkSession.sparkContext.cancelJobGroup(statementId)
    }
  }
}

object KyuubiOperation {
  val DEFAULT_FETCH_ORIENTATION: FetchOrientation = FetchOrientation.FETCH_NEXT
  val DEFAULT_FETCH_MAX_ROWS = 100

  def isResourceDownloadable(resource: String): Boolean = {
    val scheme = new Path(resource).toUri.getScheme
    StringUtils.equalsIgnoreCase(scheme, "hdfs")
  }
}
