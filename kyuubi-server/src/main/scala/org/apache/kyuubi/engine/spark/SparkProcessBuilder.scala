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

import java.io.{File, FileFilter, IOException}
import java.nio.file.Paths
import java.util.Locale

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import com.google.common.annotations.VisibleForTesting
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.security.UserGroupInformation

import org.apache.kyuubi._
import org.apache.kyuubi.config.{KyuubiConf, KyuubiEbayConf}
import org.apache.kyuubi.config.KyuubiEbayConf._
import org.apache.kyuubi.engine.{ApplicationManagerInfo, KyuubiApplicationManager, ProcBuilder}
import org.apache.kyuubi.engine.KubernetesApplicationOperation.{KUBERNETES_SERVICE_HOST, KUBERNETES_SERVICE_PORT}
import org.apache.kyuubi.engine.ProcBuilder.KYUUBI_ENGINE_LOG_PATH_KEY
import org.apache.kyuubi.ha.HighAvailabilityConf
import org.apache.kyuubi.ha.client.AuthTypes
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.util.{KubernetesUtils, Validator}

class SparkProcessBuilder(
    override val proxyUser: String,
    override val conf: KyuubiConf,
    val engineRefId: String,
    val extraEngineLog: Option[OperationLog] = None)
  extends ProcBuilder with Logging {

  @VisibleForTesting
  def this(proxyUser: String, conf: KyuubiConf) {
    this(proxyUser, conf, "")
  }

  import SparkProcessBuilder._

  private[kyuubi] val sparkHome = getEngineHome(shortName)

  override protected val executable: String = {
    Paths.get(sparkHome, "bin", SPARK_SUBMIT_FILE).toFile.getCanonicalPath
  }

  override def mainClass: String = "org.apache.kyuubi.engine.spark.SparkSQLEngine"

  /**
   * Add `spark.master` if KUBERNETES_SERVICE_HOST and KUBERNETES_SERVICE_PORT
   * are defined. So we can deploy spark on kubernetes without setting `spark.master`
   * explicitly when kyuubi-servers are on kubernetes, which also helps in case that
   * api-server is not exposed to us.
   */
  override protected def completeMasterUrl(conf: KyuubiConf): Unit = {
    try {
      (
        clusterManager(),
        sys.env.get(KUBERNETES_SERVICE_HOST),
        sys.env.get(KUBERNETES_SERVICE_PORT)) match {
        case (None, Some(kubernetesServiceHost), Some(kubernetesServicePort)) =>
          // According to "https://kubernetes.io/docs/concepts/architecture/control-plane-
          // node-communication/#node-to-control-plane", the API server is configured to listen
          // for remote connections on a secure HTTPS port (typically 443), so we set https here.
          val masterURL = s"k8s://https://${kubernetesServiceHost}:${kubernetesServicePort}"
          conf.set(MASTER_KEY, masterURL)
        case _ =>
      }
    } catch {
      case e: Exception =>
        warn("Failed when setting up spark.master with kubernetes environment automatically.", e)
    }
  }

  /**
   * Converts kyuubi config key so that Spark could identify.
   * - If the key is start with `spark.`, keep it AS IS as it is a Spark Conf
   * - If the key is start with `hadoop.`, it will be prefixed with `spark.hadoop.`
   * - Otherwise, the key will be added a `spark.` prefix
   */
  protected def convertConfigKey(key: String): String = {
    if (key.startsWith("spark.")) {
      key
    } else if (key.startsWith("hadoop.")) {
      "spark.hadoop." + key
    } else {
      "spark." + key
    }
  }

  private[kyuubi] def extractSparkCoreScalaVersion(fileNames: Iterable[String]): String = {
    fileNames.collectFirst { case SPARK_CORE_SCALA_VERSION_REGEX(scalaVersion) => scalaVersion }
      .getOrElse(throw new KyuubiException("Failed to extract Scala version from spark-core jar"))
  }

  override protected val engineScalaBinaryVersion: String = {
    val sparkCoreScalaVersion =
      extractSparkCoreScalaVersion(Paths.get(sparkHome, "jars").toFile.list())
    StringUtils.defaultIfBlank(System.getenv("SPARK_SCALA_VERSION"), sparkCoreScalaVersion)
  }

  override protected lazy val engineHomeDirFilter: FileFilter = file => {
    val r = SCALA_COMPILE_VERSION match {
      case "2.12" => SPARK_HOME_REGEX_SCALA_212
      case "2.13" => SPARK_HOME_REGEX_SCALA_213
    }
    file.isDirectory && r.findFirstMatchIn(file.getName).isDefined
  }

  override protected lazy val commands: Array[String] = {
    // complete `spark.master` if absent on kubernetes
    completeMasterUrl(conf)

    KyuubiApplicationManager.tagApplication(engineRefId, shortName, clusterManager(), conf)
    val buffer = new ArrayBuffer[String]()
    buffer += executable
    buffer += CLASS
    buffer += mainClass

    var allConf = conf.getAll

    // if enable sasl kerberos authentication for zookeeper, need to upload the server keytab file
    if (AuthTypes.withName(conf.get(HighAvailabilityConf.HA_ZK_ENGINE_AUTH_TYPE))
        == AuthTypes.KERBEROS) {
      allConf = allConf ++ zkAuthKeytabFileConf(allConf)
    }

    allConf = allConf ++ mergeKyuubiFiles(allConf) ++ mergeKyuubiJars(allConf)
    // pass spark engine log path to spark conf
    (allConf ++ engineLogPathConf ++ appendPodNameConf(allConf)).foreach { case (k, v) =>
      buffer += CONF
      buffer += s"${convertConfigKey(k)}=$v"
    }

    setUpMoveQueue(allConf, buffer)

    setupKerberos(buffer)

    mainResource.foreach { r => buffer += r }

    buffer.toArray
  }

  override protected def module: String = "kyuubi-spark-sql-engine"

  private def setUpMoveQueue(allConf: Map[String, String], buffer: ArrayBuffer[String]): Unit = {
    if (KyuubiEbayConf.moveQueueEnabled(conf)) {
      // only use init queue if the spark.yarn.queue is specified
      if (allConf.get(YARN_QUEUE).isDefined) {
        conf.get(SESSION_ENGINE_LAUNCH_MOVE_QUEUE_INIT_QUEUE).foreach { initQueue =>
          buffer += CONF
          buffer += s"$YARN_QUEUE=$initQueue"
        }
      }
    }
  }

  protected def setupKerberos(buffer: ArrayBuffer[String]): Unit = {
    // if the keytab is specified, PROXY_USER is not supported
    tryKeytab() match {
      case None =>
        setSparkUserName(proxyUser, buffer)
        buffer += PROXY_USER
        buffer += proxyUser
      case Some(name) =>
        setSparkUserName(name, buffer)
    }
  }

  private def tryKeytab(): Option[String] = {
    val principal = conf.getOption(PRINCIPAL)
    val keytab = conf.getOption(KEYTAB)
    if (principal.isEmpty || keytab.isEmpty) {
      None
    } else {
      try {
        val ugi = UserGroupInformation
          .loginUserFromKeytabAndReturnUGI(principal.get, keytab.get)
        if (ugi.getShortUserName != proxyUser) {
          warn(s"The session proxy user: $proxyUser is not same with " +
            s"spark principal: ${ugi.getShortUserName}, so we can't support use keytab. " +
            s"Fallback to use proxy user.")
          None
        } else {
          Some(ugi.getShortUserName)
        }
      } catch {
        case e: IOException =>
          error(s"Failed to login for ${principal.get}", e)
          None
      }
    }
  }

  private def zkAuthKeytabFileConf(sparkConf: Map[String, String]): Map[String, String] = {
    val zkAuthKeytab = conf.get(HighAvailabilityConf.HA_ZK_AUTH_KEYTAB)
    if (zkAuthKeytab.isDefined) {
      sparkConf.get(SPARK_FILES) match {
        case Some(files) =>
          Map(SPARK_FILES -> s"$files,${zkAuthKeytab.get}")
        case _ =>
          Map(SPARK_FILES -> zkAuthKeytab.get)
      }
    } else {
      Map()
    }
  }

  protected def getSessionBatchFiles(): Seq[String] = {
    conf.get(KyuubiEbayConf.KYUUBI_SESSION_SPARK_FILES)
  }

  protected def getSessionBatchJars(): Seq[String] = {
    conf.get(KyuubiEbayConf.KYUUBI_SESSION_SPARK_JARS)
  }

  protected def mergeKyuubiFiles(sparkConf: Map[String, String]): Map[String, String] = {
    val batchFiles = getSessionBatchFiles()
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

  protected def mergeKyuubiJars(sparkConf: Map[String, String]): Map[String, String] = {
    val batchJars = getSessionBatchJars()
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

  override def shortName: String = "spark"

  protected lazy val defaultsConf: Map[String, String] = {
    val confDir = env.getOrElse(SPARK_CONF_DIR, s"$sparkHome${File.separator}conf")
    try {
      val confFile = new File(s"$confDir${File.separator}$SPARK_CONF_FILE_NAME")
      if (confFile.exists()) {
        Utils.getPropertiesFromFile(Some(confFile))
      } else {
        Map.empty[String, String]
      }
    } catch {
      case _: Exception =>
        warn(s"Failed to load spark configurations from $confDir")
        Map.empty[String, String]
    }
  }

  override def appMgrInfo(): ApplicationManagerInfo = {
    ApplicationManagerInfo(
      clusterManager(),
      kubernetesContext(),
      kubernetesNamespace(),
      cluster())
  }

  def appendPodNameConf(conf: Map[String, String]): Map[String, String] = {
    val appName = conf.getOrElse(APP_KEY, "spark")
    val map = mutable.Map.newBuilder[String, String]
    if (clusterManager().exists(cm => cm.toLowerCase(Locale.ROOT).startsWith("k8s"))) {
      if (!conf.contains(KUBERNETES_EXECUTOR_POD_NAME_PREFIX)) {
        val prefix = KubernetesUtils.generateExecutorPodNamePrefix(appName, engineRefId)
        map += (KUBERNETES_EXECUTOR_POD_NAME_PREFIX -> prefix)
      }
      if (deployMode().exists(_.toLowerCase(Locale.ROOT) == "cluster")) {
        if (!conf.contains(KUBERNETES_DRIVER_POD_NAME)) {
          val name = KubernetesUtils.generateDriverPodName(appName, engineRefId)
          map += (KUBERNETES_DRIVER_POD_NAME -> name)
        }
      }
    }
    map.result().toMap
  }

  override def clusterManager(): Option[String] = {
    conf.getOption(MASTER_KEY).orElse(defaultsConf.get(MASTER_KEY))
  }

  def deployMode(): Option[String] = {
    conf.getOption(DEPLOY_MODE_KEY).orElse(defaultsConf.get(DEPLOY_MODE_KEY))
  }

  override def isClusterMode(): Boolean = {
    clusterManager().map(_.toLowerCase(Locale.ROOT)) match {
      case Some(m) if m.startsWith("yarn") || m.startsWith("k8s") =>
        deployMode().exists(_.toLowerCase(Locale.ROOT) == "cluster")
      case _ => false
    }
  }

  def kubernetesContext(): Option[String] = {
    conf.getOption(KUBERNETES_CONTEXT_KEY).orElse(defaultsConf.get(KUBERNETES_CONTEXT_KEY))
  }

  def kubernetesNamespace(): Option[String] = {
    conf.getOption(KUBERNETES_NAMESPACE_KEY).orElse(defaultsConf.get(KUBERNETES_NAMESPACE_KEY))
  }

  def cluster(): Option[String] = {
    conf.get(KyuubiEbayConf.SESSION_CLUSTER)
  }

  override def validateConf: Unit = Validator.validateConf(conf)

  // For spark on kubernetes, spark pod using env SPARK_USER_NAME as current user
  def setSparkUserName(userName: String, buffer: ArrayBuffer[String]): Unit = {
    clusterManager().foreach { cm =>
      if (cm.toUpperCase.startsWith("K8S")) {
        buffer += CONF
        buffer += s"spark.kubernetes.driverEnv.SPARK_USER_NAME=$userName"
        buffer += CONF
        buffer += s"spark.executorEnv.SPARK_USER_NAME=$userName"
      }
    }
  }

  private[spark] def engineLogPathConf(): Map[String, String] = {
    Map(KYUUBI_ENGINE_LOG_PATH_KEY -> engineLog.getAbsolutePath)
  }
}

object SparkProcessBuilder {
  final val APP_KEY = "spark.app.name"
  final val TAG_KEY = "spark.yarn.tags"
  final val MASTER_KEY = "spark.master"
  final val DEPLOY_MODE_KEY = "spark.submit.deployMode"
  final val KUBERNETES_CONTEXT_KEY = "spark.kubernetes.context"
  final val KUBERNETES_NAMESPACE_KEY = "spark.kubernetes.namespace"
  final val KUBERNETES_DRIVER_POD_NAME = "spark.kubernetes.driver.pod.name"
  final val KUBERNETES_EXECUTOR_POD_NAME_PREFIX = "spark.kubernetes.executor.podNamePrefix"
  final val INTERNAL_RESOURCE = "spark-internal"

  /**
   * The path configs from Spark project that might upload local files:
   * - SparkSubmit
   * - org.apache.spark.deploy.yarn.Client::prepareLocalResources
   * - KerberosConfDriverFeatureStep::configurePod
   * - KubernetesUtils.uploadAndTransformFileUris
   */
  final val PATH_CONFIGS = Seq(
    SPARK_FILES,
    "spark.jars",
    "spark.archives",
    "spark.yarn.jars",
    "spark.yarn.dist.files",
    "spark.yarn.dist.pyFiles",
    "spark.submit.pyFiles",
    "spark.yarn.dist.jars",
    "spark.yarn.dist.archives",
    "spark.kerberos.keytab",
    "spark.yarn.keytab",
    "spark.kubernetes.kerberos.krb5.path",
    "spark.kubernetes.file.upload.path")

  final val CONF = "--conf"
  final val CLASS = "--class"
  final val PROXY_USER = "--proxy-user"
  final val SPARK_FILES = "spark.files"
  final val SPARK_JARS = "spark.jars"
  final private val PRINCIPAL = "spark.kerberos.principal"
  final private val KEYTAB = "spark.kerberos.keytab"
  final val YARN_QUEUE = "spark.yarn.queue"
  // Get the appropriate spark-submit file
  final val SPARK_SUBMIT_FILE = if (Utils.isWindows) "spark-submit.cmd" else "spark-submit"
  final private val SPARK_CONF_DIR = "SPARK_CONF_DIR"
  final private val SPARK_CONF_FILE_NAME = "spark-defaults.conf"

  final private[kyuubi] val SPARK_CORE_SCALA_VERSION_REGEX =
    """^spark-core_(\d\.\d+).*.jar$""".r

  final private[kyuubi] val SPARK_HOME_REGEX_SCALA_212 =
    """^spark-\d+\.\d+\.\d+-bin-hadoop\d+(\.\d+)?$""".r

  final private[kyuubi] val SPARK_HOME_REGEX_SCALA_213 =
    """^spark-\d+\.\d+\.\d+-bin-hadoop\d(\.\d+)?+-scala\d+(\.\d+)?$""".r
}
