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

import java.io.FileOutputStream
import java.nio.ByteBuffer
import java.util.Locale

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.config.KyuubiEbayConf._
import org.apache.kyuubi.engine.spark.operation.TransferDataOperation.uploadDataSizeExceeded
import org.apache.kyuubi.engine.spark.session.SparkSessionImpl
import org.apache.kyuubi.operation.ArrayFetchIterator
import org.apache.kyuubi.session.Session

class TransferDataOperation(
    session: Session,
    values: ByteBuffer,
    path: String) extends SparkOperation(session) {
  override protected def resultSchema: StructType = {
    new StructType()
      .add("FILE_NAME", "string", nullable = true, "The file transferred to server side.")
  }

  override protected def runInternal(): Unit = {
    transferData()
  }

  private def transferData(): Unit = withLocalProperties {
    try {
      if (!session.sessionManager.getConf.get(DATA_UPLOAD_ENABLED)) {
        throw KyuubiSQLException("UPLOAD DATA is not supported")
      }
      val hadoopConf = spark.sparkContext.hadoopConfiguration
      val sessionScratchDir = session.asInstanceOf[SparkSessionImpl].sessionScratchDir
      val fileSystem = sessionScratchDir.getFileSystem(hadoopConf)

      if (!fileSystem.exists(sessionScratchDir)) {
        fileSystem.mkdirs(sessionScratchDir)
      }
      val targetPath = new Path(sessionScratchDir, sanitize(path))
      persistData(fileSystem, hadoopConf, targetPath)
      iter = new ArrayFetchIterator[Row](Array(Row(targetPath.toString)))
    } catch onError(cancel = true)
  }

  private def persistData(
      fileSystem: FileSystem,
      hadoopConf: Configuration,
      targetPath: Path): Unit = this.synchronized {
    val length = values.limit() - values.position()
    val maxTempFileSize = session.sessionManager.getConf.get(DATA_UPLOAD_TEMPORARY_FILE_MAX_SIZE)

    val uri = targetPath.toUri
    val defaultFs = FileSystem.getDefaultUri(hadoopConf).getScheme
    val isDefaultLocal = defaultFs == null || defaultFs == "file"
    // The Hadoop LocalFileSystem (r1.0.4) has known issues with syncing (HADOOP-7844).
    // Therefore, for local files, use FileOutputStream instead.
    val dstream =
      if ((isDefaultLocal && uri.getScheme == null) || uri.getScheme == "file") {
        new FileOutputStream(uri.getPath, true)
      } else {
        if (!fileSystem.exists(targetPath)) {
          if (length > maxTempFileSize) {
            throw uploadDataSizeExceeded(length, maxTempFileSize)
          }
          fileSystem.create(targetPath)
        } else {
          val targetFileLength = fileSystem.getFileStatus(targetPath).getLen + length
          if (targetFileLength > maxTempFileSize) {
            throw uploadDataSizeExceeded(targetFileLength, maxTempFileSize)
          }
          fileSystem.append(targetPath)
        }
      }

    try {
      dstream.write(values.array(), values.position, length)
      dstream.close()
      info(s"Writing $length size bytes to path $targetPath")
    } catch {
      case e: Exception =>
        dstream.close()
        error(s"Error uploading data to path $targetPath", e)
        throw e
    }
  }

  private def sanitize(str: String): String = {
    str.replaceAll("[ :/]", "-").replaceAll("[.${}'\"]", "_").toLowerCase(Locale.ROOT)
  }
}

object TransferDataOperation {
  // align with carmel error code
  def uploadDataSizeExceeded(dataSize: Long, maxSize: Long): KyuubiSQLException = {
    KyuubiSQLException(
      s"Too big size for a single uploaded file: $dataSize, " +
        s"which exceeds $maxSize.",
      vendorCode = 500003)
  }
}
