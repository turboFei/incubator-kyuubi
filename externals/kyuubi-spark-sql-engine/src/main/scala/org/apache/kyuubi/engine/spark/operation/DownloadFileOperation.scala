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

import java.util.{ArrayList => JArrayList, List => JList, Map => JMap}

import scala.collection.JavaConverters._
import scala.math.BigDecimal.RoundingMode

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.Row
import org.apache.spark.sql.kyuubi.SparkEbayUtils

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.engine.spark.KyuubiSparkUtil.diagnostics
import org.apache.kyuubi.engine.spark.operation.DownloadFileOperation.DOWNLOAD_FILE_OPTION
import org.apache.kyuubi.operation.{IterableFetchIterator, OperationHandle, OperationState}
import org.apache.kyuubi.session.Session

class DownloadFileOperation(
    session: Session,
    tableName: String,
    query: String,
    format: String,
    options: JMap[String, String],
    override protected val handle: OperationHandle = OperationHandle())
  extends DownloadDataOperation(
    session,
    tableName,
    query,
    format,
    options,
    handle) {

  private val downloadFilePath = new Path(options.get(DOWNLOAD_FILE_OPTION))
  private val fs = downloadFilePath.getFileSystem(spark.sparkContext.hadoopConfiguration)

  override val statement: String = s"DOWNLOAD FILE $downloadFilePath"
  override protected def executeStatement(): Unit = withLocalProperties {
    try {
      setState(OperationState.RUNNING)
      info(diagnostics)
      Thread.currentThread().setContextClassLoader(spark.sharedState.jarClassLoader)
      addOperationListener()

      val fetchSize = Option(options).map(_.asScala).flatMap(_.get("fetchBlockSize"))
        .getOrElse(DownloadDataOperation.DEFAULT_BLOCK_SIZE.toString).toLong

      assert(
        fetchSize >= 1L * 1024 * 1024 && fetchSize <= 20L * 1024 * 1024,
        s"fetchBlockSize(${fetchSize}) should be greater than 1M and less than 20M.")

      val fileStatus = fs.getFileStatus(downloadFilePath)
      if (fileStatus == null || !fileStatus.isFile) {
        throw KyuubiSQLException(
          s"The file[$downloadFilePath] to be downloaded does not exist or is not a file.")
      }

      info(s"Running query $statementId in ${session.handle} DOWNLOAD $downloadFilePath")
      val dataSize = fileStatus.getLen
      info(s"Try to download $dataSize bytes data")

      val list: JList[DownloadDataBlock] = new JArrayList[DownloadDataBlock]()

      // Add total data size to first row.
      list.add(DownloadDataBlock(schema = None, dataSize = dataSize))

      val fetchBatchs =
        BigDecimal(dataSize)./(BigDecimal(fetchSize)).setScale(0, RoundingMode.CEILING).longValue()
      assert(fetchBatchs < Int.MaxValue, "The fetch batch too large.")

      (0 until fetchBatchs.toInt).foreach { i =>
        val fetchSizeInBatch = if (i == fetchBatchs - 1) dataSize - i * fetchSize else fetchSize
        list.add(DownloadDataBlock(
          path = Some(downloadFilePath),
          offset = Some(i * fetchSize),
          dataSize = fetchSizeInBatch))
      }
      list.add(DownloadDataBlock(path = Some(downloadFilePath), dataSize = -1))

      val rows = list.iterator().asScala.map { dataBlock =>
        val dataSize = dataBlock.dataSize
        dataBlock.path match {
          case Some(path) =>
            if (dataSize >= 0) {
              val buffer = new Array[Byte](dataSize.toInt)
              SparkEbayUtils.tryWithResource(fs.open(path)) { is =>
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
            Row(null, null, null, Long.box(dataSize))
        }
      }
      iter = new IterableFetchIterator[Row](rows.toIterable)
      info(s"Add ${list.size()} data blocks to be fetched.")

      setState(OperationState.FINISHED)
    } catch {
      onError(cancel = true)
    } finally {
      shutdownTimeoutMonitor()
    }
  }

}

object DownloadFileOperation {
  val DOWNLOAD_FILE_OPTION = "download_file"
}
