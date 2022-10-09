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

package org.apache.kyuubi.engine.spark

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files

import scala.collection.mutable.ArrayBuffer

import org.apache.kyuubi.Utils
import org.apache.kyuubi.client.util.BatchUtils
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.KyuubiApplicationManager
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.server.LogAggManager

class SparkBatchProcessBuilder(
    override val proxyUser: String,
    override val conf: KyuubiConf,
    batchId: String,
    batchName: String,
    override val mainResource: Option[String],
    override val mainClass: String,
    batchConf: Map[String, String],
    batchArgs: Seq[String],
    override val extraEngineLog: Option[OperationLog])
  extends SparkProcessBuilder(proxyUser, conf, batchId, extraEngineLog) {
  import SparkProcessBuilder._

  private var batchSqlFileDir: File = _
  private var logAggregated: Boolean = false

  override protected val commands: Array[String] = {
    val buffer = new ArrayBuffer[String]()
    buffer += executable
    Option(mainClass).foreach { cla =>
      buffer += CLASS
      buffer += cla
    }

    val batchKyuubiConf = new KyuubiConf(false)
    batchConf.foreach(entry => { batchKyuubiConf.set(entry._1, entry._2) })

    // for spark batch etl sql jobs
    batchConf.get(BatchUtils.SPARK_BATCH_ETL_SQL_STATEMENTS_KEY).foreach { etlStatements =>
      batchSqlFileDir = Utils.createTempDir(namePrefix = "kyuubi-batch").toFile
      val etlSqlFile = new File(batchSqlFileDir, s"kyuubi-batch-$batchId.sql")
      Files.write(etlSqlFile.toPath(), etlStatements.getBytes(StandardCharsets.UTF_8))
      batchKyuubiConf.unset(BatchUtils.SPARK_BATCH_ETL_SQL_STATEMENTS_KEY)
      batchKyuubiConf.set("spark.etl.sql.files", etlSqlFile.getAbsolutePath)
      // TODO: revert it after typo fixed
      batchKyuubiConf.set("spark.ebay.sql.files", etlSqlFile.getAbsolutePath)
    }

    // tag batch application
    KyuubiApplicationManager.tagApplication(batchId, "spark", clusterManager(), batchKyuubiConf)

    (batchKyuubiConf.getAll ++ sparkAppNameConf() ++ procConf() ++ mergeKyuubiFiles(
      batchConf) ++ mergeKyuubiJars(batchConf)).foreach { case (k, v) =>
      buffer += CONF
      buffer += s"${convertConfigKey(k)}=$v"
    }

    buffer += PROXY_USER
    buffer += proxyUser

    assert(mainResource.isDefined)
    buffer += mainResource.get

    batchArgs.foreach { arg => buffer += arg }

    buffer.toArray
  }

  private def sparkAppNameConf(): Map[String, String] = {
    Option(batchName).filterNot(_.isEmpty).map { appName =>
      Map(APP_KEY -> appName)
    }.getOrElse(Map())
  }

  override protected def module: String = "kyuubi-spark-batch-submit"

  override def clusterManager(): Option[String] = {
    batchConf.get(MASTER_KEY).orElse(defaultMaster)
  }

  override def close(destroyProcess: Boolean = !waitCompletion): Unit = {
    super.close(destroyProcess)
    Option(batchSqlFileDir).foreach { dir =>
      Utils.tryLogNonFatalError(Utils.deleteDirectoryRecursively(dir))
    }
    if (!logAggregated) {
      LogAggManager.get.foreach(_.aggLog(engineLog, batchId))
      logAggregated = true
    }
  }
}
