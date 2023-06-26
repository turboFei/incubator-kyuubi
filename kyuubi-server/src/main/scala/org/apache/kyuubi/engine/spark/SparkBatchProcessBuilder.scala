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
import java.util.Base64

import scala.collection.mutable.ArrayBuffer

import org.apache.kyuubi.{KyuubiException, Utils}
import org.apache.kyuubi.client.util.BatchUtils
import org.apache.kyuubi.config.{KyuubiConf, KyuubiEbayConf}
import org.apache.kyuubi.engine.KyuubiApplicationManager
import org.apache.kyuubi.operation.log.OperationLog

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

  override protected val commands: Array[String] = {
    val buffer = new ArrayBuffer[String]()
    buffer += executable
    Option(mainClass).foreach { cla =>
      buffer += CLASS
      buffer += cla
    }

    val batchKyuubiConf = new KyuubiConf(false)
    // complete `spark.master` if absent on kubernetes
    completeMasterUrl(batchKyuubiConf)
    batchConf.foreach(entry => { batchKyuubiConf.set(entry._1, entry._2) })

    // for spark batch etl sql jobs
    val encodedEtlStatements = batchConf.get(BatchUtils.SPARK_BATCH_ETL_SQL_ENCODED_STATEMENTS_KEY)
    val rawEtlStatements = batchConf.get(BatchUtils.SPARK_BATCH_ETL_SQL_STATEMENTS_KEY)
    if (encodedEtlStatements.isDefined || rawEtlStatements.isDefined) {
      val etlStatements = if (encodedEtlStatements.isDefined) {
        try {
          new String(Base64.getDecoder.decode(encodedEtlStatements.get), StandardCharsets.UTF_8)
        } catch {
          case e: Throwable =>
            throw new KyuubiException("Failed to decode the encoded etl statements", e)
        }
      } else {
        rawEtlStatements.get
      }
      batchSqlFileDir = Utils.createTempDir("kyuubi-batch").toFile
      val etlSqlFile = new File(batchSqlFileDir, s"kyuubi-batch-$batchId.sql")
      Files.write(etlSqlFile.toPath(), etlStatements.getBytes(StandardCharsets.UTF_8))
      batchKyuubiConf.unset(BatchUtils.SPARK_BATCH_ETL_SQL_ENCODED_STATEMENTS_KEY)
      batchKyuubiConf.unset(BatchUtils.SPARK_BATCH_ETL_SQL_STATEMENTS_KEY)
      batchKyuubiConf.set("spark.etl.sql.files", etlSqlFile.getAbsolutePath)
      // TODO: revert it after typo fixed
      batchKyuubiConf.set("spark.ebay.sql.files", etlSqlFile.getAbsolutePath)
    }

    // tag batch application
    KyuubiApplicationManager.tagApplication(batchId, "spark", clusterManager(), batchKyuubiConf)

    (batchKyuubiConf.getAll ++ sparkAppNameConf() ++ mergeKyuubiFiles(
      batchConf) ++ mergeKyuubiJars(batchConf)).foreach { case (k, v) =>
      buffer += CONF
      buffer += s"${convertConfigKey(k)}=$v"
    }

    setupKerberos(buffer)

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
    batchConf.get(MASTER_KEY).orElse(super.clusterManager())
  }

  override def kubernetesContext(): Option[String] = {
    batchConf.get(KUBERNETES_CONTEXT_KEY).orElse(super.kubernetesContext())
  }

  override def kubernetesNamespace(): Option[String] = {
    batchConf.get(KUBERNETES_NAMESPACE_KEY).orElse(super.kubernetesNamespace())
  }

  override def cluster(): Option[String] = {
    batchConf.get(KyuubiEbayConf.SESSION_CLUSTER.key).orElse(super.cluster())
  }

  override def close(destroyProcess: Boolean = !waitCompletion): Unit = {
    super.close(destroyProcess)
    Option(batchSqlFileDir).foreach { dir =>
      Utils.tryLogNonFatalError(Utils.deleteDirectoryRecursively(dir))
    }
  }

  override protected def getSessionBatchFiles(): Seq[String] = {
    conf.get(KyuubiEbayConf.KYUUBI_SESSION_BATCH_SPARK_FILES)
  }

  override protected def getSessionBatchJars(): Seq[String] = {
    conf.get(KyuubiEbayConf.KYUUBI_SESSION_BATCH_SPARK_JARS)
  }
}
