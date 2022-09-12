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

import java.io.FileNotFoundException
import java.util.{ArrayList => JArrayList, List => JList, Map => JMap}

import scala.collection.JavaConverters._
import scala.math.BigDecimal.RoundingMode

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.{Path, PathFilter}
import org.apache.hive.service.rpc.thrift.TTableSchema
import org.apache.spark.kyuubi.SparkUtilsHelper
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.kyuubi.operation.DownloadDataHelper._
import org.apache.spark.sql.types._

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.config.KyuubiEbayConf._
import org.apache.kyuubi.engine.spark.schema.SchemaHelper
import org.apache.kyuubi.engine.spark.session.SparkSessionImpl
import org.apache.kyuubi.operation.IterableFetchIterator
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.session.Session

private[kyuubi] case class DownloadDataBlock(
    path: Option[Path] = None,
    offset: Option[Long] = None,
    schema: Option[String] = None,
    dataSize: Long)

class DownloadDataOperation(
    session: Session,
    tableName: String,
    query: String,
    format: String,
    options: JMap[String, String]) extends SparkOperation(session) {
  import DownloadDataOperation._
  private val operationLog: OperationLog = OperationLog.createOperationLog(session, getHandle)
  override def getOperationLog: Option[OperationLog] = Option(operationLog)

  override protected def resultSchema: StructType = {
    new StructType()
      .add("FILE_NAME", "string", nullable = true, "The file name to be transferred.")
      .add("DATA", "binary", nullable = true, "The data to be transferred.")
      .add("SCHEMA", "string", nullable = true, "The data schema to be transferred.")
      .add("SIZE", "bigint", nullable = true, "The size to be transferred in this fetch.")
  }

  private val writeOptions =
    DEFAULT_OPTIONS ++ Option(options).map(_.asScala).getOrElse(Map.empty[String, String]).toMap

  override def statement: String = s"Generating download files with arguments " +
    s"[${tableName}, ${query}, ${format}, ${writeOptions}]"

  private val sessionScratchDir = session.asInstanceOf[SparkSessionImpl].sessionScratchDir
  private val fs = sessionScratchDir.getFileSystem(spark.sparkContext.hadoopConfiguration)

  override protected def runInternal(): Unit = {
    downloadData()
  }

  private def downloadData(): Unit = withLocalProperties {
    val numFiles = writeOptions.get("numFiles").map(_.toInt)
    val fetchSize = writeOptions("fetchBlockSize").toLong
    val keepDataType = writeOptions("keepDataType").toBoolean
    val dataDownloadMaxSize = session.sessionManager.getConf.get(DATA_DOWNLOAD_MAX_SIZE)

    try {
      assert(
        fetchSize >= 1L * 1024 * 1024 && fetchSize <= 20L * 1024 * 1024,
        s"fetchBlockSize(${fetchSize}) should be greater than 1M and less than 20M.")

      if (StringUtils.isNotEmpty(tableName) && StringUtils.isNotEmpty(query)) {
        throw KyuubiSQLException("Both table name and query are specified.")
      }

      val (_result, schemaStr, resultPath) =
        if (keepDataType) {
          withRetry(writeDataKeepDataType(
            spark,
            fs,
            tableName,
            query,
            format,
            new Path(sessionScratchDir, statementId),
            options,
            writeOptions,
            dataDownloadMaxSize))

        } else {
          withRetry(writeData(
            spark,
            fs,
            tableName,
            query,
            format,
            new Path(sessionScratchDir, statementId),
            options,
            writeOptions,
            dataDownloadMaxSize))
        }
      result = _result

      info(s"Running query $statementId in ${session.handle} DOWNLOAD $query")
      val dataSize = fs.getContentSummary(resultPath).getLength
      info(s"Try to download $dataSize bytes data")

      // Rename file name
      writeOptions.get("fileName") match {
        case Some(fileName) =>
          fs.listStatus(resultPath, PATH_FILTER).foreach { fileStatus =>
            val originName = fileStatus.getPath.getName
            val name = numFiles match {
              case Some(num) if num == 1 =>
                fileName
              case _ =>
                originName.substring(0, 11) + fileName
            }
            val ext = originName.split("-").last
            fs.rename(fileStatus.getPath, new Path(fileStatus.getPath.getParent, name + "-" + ext))
          }

        case _ => // do nothing
      }

      val list: JList[DownloadDataBlock] = new JArrayList[DownloadDataBlock]()
      // Add total data size to first row.
      list.add(DownloadDataBlock(schema = Some(schemaStr), dataSize = dataSize))
      // and then add data.
      fs.listStatus(resultPath, PATH_FILTER).map(_.getPath).sortBy(_.getName).foreach { path =>
        val dataLen = fs.getFileStatus(path).getLen
        // Cast to BigDecimal to avoid overflowing
        val fetchBatchs =
          BigDecimal(dataLen)./(BigDecimal(fetchSize)).setScale(0, RoundingMode.CEILING).longValue()
        assert(fetchBatchs < Int.MaxValue, "The fetch batch too large.")

        (0 until fetchBatchs.toInt).foreach { i =>
          val fetchSizeInBatch = if (i == fetchBatchs - 1) dataLen - i * fetchSize else fetchSize
          list.add(DownloadDataBlock(
            path = Some(path),
            offset = Some(i * fetchSize),
            dataSize = fetchSizeInBatch))
        }

        list.add(DownloadDataBlock(path = Some(path), dataSize = -1))
      }

      val rows = list.iterator().asScala.map { dataBlock =>
        val dataSize = dataBlock.dataSize
        dataBlock.path match {
          case Some(path) =>
            if (dataSize >= 0) {
              val buffer = new Array[Byte](dataSize.toInt)
              SparkUtilsHelper.tryWithResource(fs.open(path)) { is =>
                is.seek(dataBlock.offset.get)
                is.readFully(buffer)
              }
              // data row
              Row(path.getName, buffer, null, Long.box(dataSize))
            } else {
              // End of file row
              Row(path.getName, null, null, Long.box(dataSize))
            }

          case _ =>
            // Schema row and total data size row
            Row(null, null, dataBlock.schema.get, Long.box(dataSize))
        }
      }
      iter = new IterableFetchIterator[Row](rows.toIterable)
      info(s"Add ${list.size()} data blocks to be fetched.")
    } catch onError(cancel = true)
  }

  private lazy val realSchema =
    if (result == null || result.schema.isEmpty) {
      new StructType()
        .add("Result", "string", nullable = true, "")
    } else {
      result.schema
    }

  override def getResultSetSchema: TTableSchema = {
    if (writeOptions.get("useRealSchema").nonEmpty
      && writeOptions("useRealSchema").equalsIgnoreCase("true")) {
      SchemaHelper.toTTableSchema(realSchema)
    } else {
      super.getResultSetSchema
    }
  }

  private def withRetry[T](f: => T): T = {
    val maxRetry = 2
    var retryNum = 0

    def retryable(t: Throwable): Boolean = {
      var cur = t
      while (retryNum < maxRetry && cur != null) {
        KyuubiSQLException.findCause(cur) match {
          case f: FileNotFoundException if !f.getMessage.contains("shuffle_") =>
            // For some commands, they may failed when initiating dataset, since it will trigger
            // execution on dataset initialization. We need manually build a QueryExecution to
            // get the optimized plan.
            val qe =
              if (result != null) {
                result.queryExecution
              } else {
                val parsed = spark.sessionState.sqlParser.parsePlan(query)
                new QueryExecution(spark, parsed)
              }
            qe.optimizedPlan.foreach {
              case LogicalRelation(_, _, Some(table), _) =>
                qe.sparkSession.sessionState.refreshTable(table.identifier.toString)
              case HiveTableRelation(tableMeta, _, _, _, _) =>
                qe.sparkSession.sessionState.refreshTable(tableMeta.identifier.toString)
              case _ =>
            }
            return true
          case c => cur = c.getCause()
        }
      }
      false
    }

    var res: Option[T] = None
    do {
      if (retryNum > 0) {
        info(s"Start to retry query $statementId.")
      }
      try {
        res = Some(f)
      } catch {
        case e if retryable(e) =>
          error(s"Query $statementId failed out of error ${e.getCause.getMessage}")
          retryNum += 1
        case e: Throwable =>
          throw e
      }
    } while (res.isEmpty)
    res.get
  }
}

object DownloadDataOperation {
  private val PATH_FILTER = new PathFilter {
    override def accept(path: Path): Boolean =
      !path.getName.equals("_SUCCESS") && !path.getName.endsWith("crc")
  }

  private val DEFAULT_BLOCK_SIZE = 10 * 1024 * 1024

  // Please see CSVOptions for more details.
  private val DEFAULT_OPTIONS = Map(
    "timestampFormat" -> "yyyy-MM-dd HH:mm:ss",
    "dateFormat" -> "yyyy-MM-dd",
    "delimiter" -> ",",
    "escape" -> "\"",
    "compression" -> "gzip",
    "header" -> "true",
    "maxRecordsPerFile" -> "0",
    // To avoid Zeta client timeout
    "fetchBlockSize" -> DEFAULT_BLOCK_SIZE.toString,
    "maxFetchBlockTime" -> "30000",
    // To avoid coalesce
    "minFileSize" -> (DEFAULT_BLOCK_SIZE - 1 * 1024 * 1024).toString,
    // clsfd need to keep origin data type to upload data to AWS
    "keepDataType" -> "false")

  def downloadDataSizeExceeded(dataSize: Long, maxSize: Long): KyuubiSQLException = {
    KyuubiSQLException(
      s"Too much download data requested: " +
        s"${SparkUtilsHelper.bytesToString(dataSize)}, " +
        s"which exceeds ${SparkUtilsHelper.bytesToString(maxSize)}",
      vendorCode = 500001)
  }

  def downloadSingleDataSizeExceeded(num: Int, dataSize: Long): KyuubiSQLException = {
    KyuubiSQLException(
      s"The numFiles($num) too small, please try to increase the " +
        s"number of downloaded files for ${dataSize / (9L * 1024 * 1024 * 1024)}.",
      vendorCode = 500002)
  }
}
