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

package org.apache.kyuubi.ebay

import java.util.{Map => JMap, UUID}

import scala.collection.JavaConverters._

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.{KyuubiConf, KyuubiEbayConf}
import org.apache.kyuubi.config.KyuubiEbayConf._
import org.apache.kyuubi.ebay.TagBasedSessionConfAdvisor.getSessionTagConf
import org.apache.kyuubi.plugin.SessionConfAdvisor

class TessConfAdvisor extends SessionConfAdvisor with Logging {
  import TagBasedSessionConfAdvisor.fileConfCache
  import TessConfAdvisor._

  private val kyuubiConf = KyuubiEbayConf._kyuubiConf

  private val tessConfOverlayFile = kyuubiConf.get(SESSION_TESS_CONF_FILE)
  private val tessConfTag = kyuubiConf.get(ENGINE_SPARK_TESS_CONFIG_TAG)
  private val tessEnabledDefault = kyuubiConf.get(ENGINE_SPARK_TESS_ENABLED)

  private val TESS_DEFAULT_SPARK_DRIVER_CORES =
    kyuubiConf.get(SESSION_TESS_SPARK_DRIVER_CORES_DEFAULT)
  private val TESS_DEFAULT_SPARK_EXECUTOR_CORES =
    kyuubiConf.get(SESSION_TESS_SPARK_EXECUTOR_CORES_DEFAULT)
  private def clusterTessConfFile(cluster: Option[String]): Option[String] = {
    cluster.map(c => s"$tessConfOverlayFile.$c")
  }

  override def getConfOverlay(
      user: String,
      sessionConf: JMap[String, String]): JMap[String, String] = {
    if ("true".equalsIgnoreCase(sessionConf.getOrDefault(
        ENGINE_SPARK_TESS_ENABLED.key,
        tessEnabledDefault.toString))) {
      val sessionCluster = sessionConf.asScala.get(SESSION_CLUSTER.key)

      val tessTagConf = getSessionTagConf(tessConfTag, sessionCluster)

      val temporaryTessConf = tessTagConf ++ sessionConf.asScala
      val tessContext = temporaryTessConf.get(SPARK_KUBERNETES_CONTEXT)
        .orElse(temporaryTessConf.get(KyuubiConf.KUBERNETES_CONTEXT.key))
      val tessNamespace = temporaryTessConf.get(SPARK_KUBERNETES_NAMESPACE)
        .orElse(temporaryTessConf.get(KyuubiConf.KUBERNETES_NAMESPACE.key))

      val tessDefaultConf = getTessContextNamespaceConf(
        tessTagConf,
        user,
        tessContext,
        tessNamespace).filterNot { case (k, _) => sessionConf.containsKey(k) }

      val tessConfOverlay = getTessContextNamespaceConf(
        fileConfCache.get(tessConfOverlayFile).getAll,
        user,
        tessContext,
        tessNamespace)
      val clusterTessConfOverlay = getTessContextNamespaceConf(
        clusterTessConfFile(sessionCluster).map(fileConfCache.get).map(_.getAll).getOrElse(
          Map.empty[String, String]),
        user,
        tessContext,
        tessNamespace)
      val tessOverlayConf = tessDefaultConf ++ tessConfOverlay ++ clusterTessConfOverlay

      val tempSessionConf = sessionConf.asScala.toMap ++ tessOverlayConf

      val appName = tempSessionConf.get(ADLC_APP).map(_.trim).getOrElse("")
      val appInstance = tempSessionConf.get(ADLC_AI).map(_.trim).getOrElse("")
      val appImage = tempSessionConf.get(ADLC_IMAGE).map(_.trim).getOrElse("")

      require(
        appName.nonEmpty && appInstance.nonEmpty,
        s"$ADLC_APP and $ADLC_AI must be specified for Spark on TESS.")

      val appConf = Map(
        DRIVER_ANNOTATION_APPLICATION_NAME -> appName,
        DRIVER_LABEL_APPLICATION_INSTANCE -> appInstance,
        DRIVER_ANNOTATION_SHERLOCK_LOGS -> appName,
        DRIVER_ANNOTATION_SHERLOCK_METRICS -> appName,
        EXECUTOR_ANNOTATION_APPLICATION_NAME -> appName,
        EXECUTOR_LABEL_APPLICATION_INSTANCE -> appInstance,
        EXECUTOR_ANNOTATION_SHERLOCK_LOGS -> appName,
        EXECUTOR_ANNOTATION_SHERLOCK_METRICS -> appName,
        CONTAINER_IMAGE -> appImage).filter(_._2.nonEmpty)

      val tessCoresConf = getTessCores(tempSessionConf)

      val tessUploadPathConf = getTessUploadPath(user, tempSessionConf)

      var allConf = tessOverlayConf ++ tessCoresConf ++ tessUploadPathConf ++ appConf

      val temporarySessionConf = sessionConf.asScala ++ allConf
      val kubernetesContext = temporarySessionConf.get(SPARK_KUBERNETES_CONTEXT)
        .orElse(temporarySessionConf.get(KyuubiConf.KUBERNETES_CONTEXT.key))
      val kubernetesNamespace = temporarySessionConf.get(SPARK_KUBERNETES_NAMESPACE)
        .orElse(temporarySessionConf.get(KyuubiConf.KUBERNETES_NAMESPACE.key))
        .orNull
      kubernetesContext.foreach { context =>
        allConf = allConf ++ TessFileSessionConfCache.getTessContextSessionConf(
          context,
          kubernetesNamespace)
      }

      KyuubiEbayConf.confOverlayForSessionType(sessionConf.asScala.toMap, allConf).asJava
    } else {
      Map.empty[String, String].asJava
    }
  }

  private def getTessCores(conf: Map[String, String]): Map[String, String] = {
    val driverRequestCores = conf.get(DRIVER_REQUEST_CORES)
      .orElse(conf.get(DRIVER_CORES))
      .orElse(Some(TESS_DEFAULT_SPARK_DRIVER_CORES))
    val driverLimitCores = conf.get(DRIVER_LIMIT_CORES)
      .orElse(driverRequestCores)

    val executorRequestCores = conf.get(EXECUTOR_REQUEST_CORES)
      .orElse(conf.get(EXECUTOR_CORES))
      .orElse(Some(TESS_DEFAULT_SPARK_EXECUTOR_CORES))
    val executorLimitCores = conf.get(EXECUTOR_LIMIT_CORES)
      .orElse(executorRequestCores)

    (driverRequestCores.map { c =>
      DRIVER_REQUEST_CORES -> c
    } ++ driverLimitCores.map { c =>
      DRIVER_LIMIT_CORES -> c
    } ++ executorRequestCores.map { c =>
      EXECUTOR_REQUEST_CORES -> c
    } ++ executorLimitCores.map { c =>
      EXECUTOR_LIMIT_CORES -> c
    }).toSeq.toMap
  }

  private def getTessUploadPath(user: String, conf: Map[String, String]): Map[String, String] = {
    conf.get(KUBERNETES_FILE_UPLOAD_PATH).orElse {
      val randomDirName = s"kyuubi-spark-upload-${UUID.randomUUID()}"
      conf.get(KyuubiEbayConf.SESSION_TESS_SCRATCH_DIR.key).map(_ + s"/$user/$randomDirName")
    }.map { p =>
      KUBERNETES_FILE_UPLOAD_PATH -> p
    }.toMap
  }
}

object TessConfAdvisor {
  final private val ADLC = "kyuubi.hadoop.adlc."

  final val ADLC_APP = ADLC + "app"
  final val ADLC_AI = ADLC + "ai"
  @deprecated("using spark.kubernetes.container.image directly")
  final val ADLC_IMAGE = ADLC + "image"

  final private val KUBERNETES_DRIVER = "spark.kubernetes.driver."
  final private val KUBERNETES_EXECUTOR = "spark.kubernetes.executor."
  final private val LABEL = "label."
  final private val ANNOTATION = "annotation."

  final private val DRIVER_LABEL_PREFIX = KUBERNETES_DRIVER + LABEL
  final private val DRIVER_ANNOTATION_PREFIX = KUBERNETES_DRIVER + ANNOTATION
  final private val EXECUTOR_LABEL_PREFIX = KUBERNETES_EXECUTOR + LABEL
  final private val EXECUTOR_ANNOTATION_PREFIX = KUBERNETES_EXECUTOR + ANNOTATION

  final private val REQUEST_CORES = "request.cores"
  final private val LIMIT_CORES = "limit.cores"

  final private val DRIVER_CORES = "spark.driver.cores"
  final private val EXECUTOR_CORES = "spark.executor.cores"
  final private val DRIVER_REQUEST_CORES = KUBERNETES_DRIVER + REQUEST_CORES
  final private val DRIVER_LIMIT_CORES = KUBERNETES_DRIVER + LIMIT_CORES
  final private val EXECUTOR_REQUEST_CORES = KUBERNETES_EXECUTOR + REQUEST_CORES
  final private val EXECUTOR_LIMIT_CORES = KUBERNETES_EXECUTOR + LIMIT_CORES

  final private val APPLICATION_NAME = "application.tess.io/name"
  final private val APPLICATION_INSTANCE = "applicationinstance.tess.io/name"
  final private val SHERLOCK_LOGS = "io.sherlock.logs/namespace"
  final private val SHERLOCK_METRICS = "io.sherlock.metrics/namespace"

  final val DRIVER_ANNOTATION_APPLICATION_NAME = DRIVER_ANNOTATION_PREFIX + APPLICATION_NAME
  final val EXECUTOR_ANNOTATION_APPLICATION_NAME = EXECUTOR_ANNOTATION_PREFIX + APPLICATION_NAME
  final val DRIVER_LABEL_APPLICATION_INSTANCE = DRIVER_LABEL_PREFIX + APPLICATION_INSTANCE
  final val EXECUTOR_LABEL_APPLICATION_INSTANCE = EXECUTOR_LABEL_PREFIX + APPLICATION_INSTANCE
  final val DRIVER_ANNOTATION_SHERLOCK_LOGS = DRIVER_ANNOTATION_PREFIX + SHERLOCK_LOGS
  final val EXECUTOR_ANNOTATION_SHERLOCK_LOGS = EXECUTOR_ANNOTATION_PREFIX + SHERLOCK_LOGS
  final val DRIVER_ANNOTATION_SHERLOCK_METRICS = DRIVER_ANNOTATION_PREFIX + SHERLOCK_METRICS
  final val EXECUTOR_ANNOTATION_SHERLOCK_METRICS = EXECUTOR_ANNOTATION_PREFIX + SHERLOCK_METRICS

  final val CONTAINER_IMAGE = "spark.kubernetes.container.image"

  final val KUBERNETES_FILE_UPLOAD_PATH = "spark.kubernetes.file.upload.path"

  final val SPARK_KUBERNETES_CONTEXT = "spark.kubernetes.context"
  final val SPARK_KUBERNETES_NAMESPACE = "spark.kubernetes.namespace"

  /**
   * Get the tess context and namespace conf for the user
   * {key} = value
   * ___tess_{cluster}___.{key} = value
   * ___tess_{cluster}___.___{user}___.{key} = value
   * ___ns_{namespace}___.{key} = value
   * ___ns_{namespace}___.___{user}___.{key} = value
   * ___tess_{cluster}_ns_{namespace}___.{key} = value
   * ___tess_{cluster}_ns_{namespace}___.___{user}___.{key} = value
   */
  def getTessContextNamespaceConf(
      tessConf: Map[String, String],
      user: String,
      tessContext: Option[String],
      tessNamespace: Option[String]): Map[String, String] = {
    val kyuubiConf = new KyuubiConf(false)
    tessConf.foreach { case (k, v) =>
      kyuubiConf.set(k, v)
    }

    val contextDefaults =
      tessContext.map(c => getTagConfOnly(kyuubiConf, s"tess_$c")).getOrElse(Map.empty)
    val namespaceDefaults =
      tessNamespace.map(ns => getTagConfOnly(kyuubiConf, s"ns_$ns")).getOrElse(Map.empty)
    val contextNamespaceDefaults = tessContext.zip(tessNamespace).headOption.map { case (c, ns) =>
      getTagConfOnly(kyuubiConf, s"tess_${c}_ns_${ns}")
    }.getOrElse(Map.empty)

    val newKyuubiConf = new KyuubiConf(false)
    (tessConf ++ contextDefaults ++ namespaceDefaults ++ contextNamespaceDefaults).foreach {
      case (k, v) =>
        newKyuubiConf.set(k, v)
    }
    newKyuubiConf.getUserDefaults(user).getAll
  }
}
