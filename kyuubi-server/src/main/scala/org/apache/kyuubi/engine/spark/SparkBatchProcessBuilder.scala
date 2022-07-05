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

import scala.collection.mutable.ArrayBuffer

import org.apache.kyuubi.config.{KyuubiConf, KyuubiEbayConf}
import org.apache.kyuubi.engine.KubernetesApplicationOperation.LABEL_KYUUBI_UNIQUE_KEY
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

  override protected val commands: Array[String] = {
    val buffer = new ArrayBuffer[String]()
    buffer += executable
    Option(mainClass).foreach { cla =>
      buffer += CLASS
      buffer += cla
    }

    // tag for YARN
    val batchJobTag = batchConf.get(TAG_KEY).map(_ + ",").getOrElse("") + batchId
    var allConf = batchConf ++ Map(TAG_KEY -> batchJobTag) ++ sparkAppNameConf() ++ procConf() ++
      mergeBatchFiles(batchConf) ++ mergeBatchJars(batchConf)
    // tag for K8S
    conf.getOption("spark.kubernetes.driver.label." + LABEL_KYUUBI_UNIQUE_KEY).foreach(option => {
      allConf = allConf ++ Map("spark.kubernetes.driver.label." + LABEL_KYUUBI_UNIQUE_KEY -> option)
    })

    allConf.foreach { case (k, v) =>
      buffer += CONF
      buffer += s"$k=$v"
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

  private def mergeBatchFiles(sparkConf: Map[String, String]): Map[String, String] = {
    val batchFiles = conf.get(KyuubiEbayConf.BATCH_SPARK_FILES)
    if (batchFiles.nonEmpty) {
      sparkConf.get(SPARK_FILES) match {
        case Some(files) =>
          Map(SPARK_FILES -> s"$files,${batchFiles.mkString(",")}")
        case _ =>
          Map(SPARK_FILES -> batchFiles.mkString(","))
      }
    } else {
      Map()
    }
  }

  private def mergeBatchJars(sparkConf: Map[String, String]): Map[String, String] = {
    val batchJars = conf.get(KyuubiEbayConf.BATCH_SPARK_JARS)
    if (batchJars.nonEmpty) {
      sparkConf.get(SPARK_JARS) match {
        case Some(jars) =>
          Map(SPARK_JARS -> s"$jars,${batchJars.mkString(",")}")
        case _ =>
          Map(SPARK_JARS -> batchJars.mkString(","))
      }
    } else {
      Map()
    }
  }

  override protected def module: String = "kyuubi-spark-batch-submit"

  override def clusterManager(): Option[String] = {
    batchConf.get(MASTER_KEY).orElse(defaultMaster)
  }
}
