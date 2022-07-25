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

package org.apache.kyuubi.kubernetes.test.spark

import scala.collection.JavaConverters._
import scala.concurrent.duration._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.net.NetUtils

import org.apache.kyuubi.{Logging, Utils, WithKyuubiServer, WithSimpleDFSService}
import org.apache.kyuubi.client.api.v1.dto.BatchRequest
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{FRONTEND_CONNECTION_URL_USE_HOSTNAME, FRONTEND_THRIFT_BINARY_BIND_HOST}
import org.apache.kyuubi.engine.{ApplicationOperation, KubernetesApplicationOperation}
import org.apache.kyuubi.engine.spark.SparkProcessBuilder
import org.apache.kyuubi.kubernetes.test.MiniKube
import org.apache.kyuubi.operation.SparkQueryTests
import org.apache.kyuubi.session.{KyuubiBatchSessionImpl, KyuubiSessionManager}
import org.apache.kyuubi.zookeeper.ZookeeperConf.ZK_CLIENT_PORT_ADDRESS

abstract class SparkOnKubernetesSuiteBase
  extends WithKyuubiServer with Logging {
  private val apiServerAddress = {
    MiniKube.getKubernetesClient.getMasterUrl.toString
  }

  protected def sparkOnK8sConf: KyuubiConf = {
    // TODO Support more Spark version
    // Spark official docker image: https://hub.docker.com/r/apache/spark/tags
    KyuubiConf().set("spark.master", s"k8s://$apiServerAddress")
      .set("spark.kubernetes.container.image", "apache/spark:v3.2.1")
      .set("spark.kubernetes.container.image.pullPolicy", "IfNotPresent")
      .set("spark.executor.instances", "1")
      .set("spark.executor.memory", "512M")
      .set("spark.driver.memory", "512M")
      .set("spark.kubernetes.driver.request.cores", "250m")
      .set("spark.kubernetes.executor.request.cores", "250m")
      .set("kyuubi.kubernetes.context", "minikube")
      .set("kyuubi.frontend.protocols", "THRIFT_BINARY,REST")
  }
}

/**
 * This test is for Kyuubi Server with Spark engine Using client deploy-mode on Kubernetes:
 *
 *                        Real World                                   Kubernetes Pod
 *  -------------------------------------------------------         ---------------------
 *  |          JDBC                                       |         |                   |
 *  |  Client  ---->  Kyuubi Server  ---->  Spark Driver  |  ---->  |  Spark Executors  |
 *  |                                                     |         |                   |
 *  -------------------------------------------------------         ---------------------
 */
class SparkClientModeOnKubernetesSuiteBase extends SparkOnKubernetesSuiteBase {
  override protected val conf: KyuubiConf = {
    sparkOnK8sConf.set("spark.submit.deployMode", "client")
  }
}

class SparkClientModeOnKubernetesSuite extends SparkClientModeOnKubernetesSuiteBase
  with SparkQueryTests {
  override protected def jdbcUrl: String = getJdbcUrl
}

/**
 * This test is for Kyuubi Server with Spark engine Using cluster deploy-mode on Kubernetes:
 *
 *               Real World                         Kubernetes Pod                Kubernetes Pod
 *  ----------------------------------          ---------------------         ---------------------
 *  |          JDBC                   |         |                   |         |                   |
 *  |  Client  ---->  Kyuubi Server   |  ---->  |    Spark Driver   |  ---->  |  Spark Executors  |
 *  |                                 |         |                   |         |                   |
 *  ----------------------------------          ---------------------         ---------------------
 */
class SparkClusterModeOnKubernetesSuiteBase
  extends SparkOnKubernetesSuiteBase with WithSimpleDFSService {
  private val localhostAddress = Utils.findLocalInetAddress.getHostAddress
  private val driverTemplate =
    Thread.currentThread().getContextClassLoader.getResource("driver.yml")

  override val hadoopConf: Configuration = {
    val hdfsConf: Configuration = new Configuration()
    hdfsConf.set("dfs.namenode.rpc-bind-host", "0.0.0.0")
    hdfsConf.set("dfs.namenode.servicerpc-bind-host", "0.0.0.0")
    hdfsConf.set("dfs.datanode.hostname", localhostAddress)
    hdfsConf.set("dfs.datanode.address", s"0.0.0.0:${NetUtils.getFreeSocketPort}")
    // spark use 185 as userid in docker
    hdfsConf.set("hadoop.proxyuser.185.groups", "*")
    hdfsConf.set("hadoop.proxyuser.185.hosts", "*")
    hdfsConf
  }

  override protected lazy val conf: KyuubiConf = {
    sparkOnK8sConf.set("spark.submit.deployMode", "cluster")
      .set("spark.kubernetes.file.upload.path", s"hdfs://$localhostAddress:$getDFSPort/spark")
      .set("spark.hadoop.dfs.client.use.datanode.hostname", "true")
      .set("spark.kubernetes.authenticate.driver.serviceAccountName", "spark")
      .set("spark.kubernetes.driver.podTemplateFile", driverTemplate.getPath)
      .set(ZK_CLIENT_PORT_ADDRESS.key, localhostAddress)
      .set(FRONTEND_CONNECTION_URL_USE_HOSTNAME.key, "false")
      .set(FRONTEND_THRIFT_BINARY_BIND_HOST.key, localhostAddress)
  }
}

class SparkClusterModeOnKubernetesSuite
  extends SparkClusterModeOnKubernetesSuiteBase with SparkQueryTests {
  override protected def jdbcUrl: String = getJdbcUrl
}

class KyuubiOperationKubernetesClusterClientModeSuite
  extends SparkClientModeOnKubernetesSuiteBase {
  private lazy val k8sOperation: KubernetesApplicationOperation = {
    val operation = new KubernetesApplicationOperation
    operation.initialize(conf)
    operation
  }

  private def sessionManager: KyuubiSessionManager =
    server.backendService.sessionManager.asInstanceOf[KyuubiSessionManager]

  test("Spark Client Mode On Kubernetes Kyuubi KubernetesApplicationOperation Suite") {
    val sparkProcessBuilder = new SparkProcessBuilder("kyuubi", conf)
    val batchRequest = new BatchRequest(
      "spark",
      sparkProcessBuilder.mainResource.get,
      sparkProcessBuilder.mainClass,
      null,
      conf.getAll.asJava,
      Seq.empty[String].asJava)

    val sessionHandle = sessionManager.openBatchSession(
      "kyuubi",
      "passwd",
      "localhost",
      batchRequest.getConf.asScala.toMap,
      batchRequest)

    eventually(timeout(3.minutes), interval(50.milliseconds)) {
      val state = k8sOperation.getApplicationInfoByTag(sessionHandle.identifier.toString)
      assert(state.nonEmpty)
      assert(state.contains("id"))
      assert(state.contains("name"))
      assert(state("state") === "RUNNING")
    }

    val killResponse = k8sOperation.killApplicationByTag(sessionHandle.identifier.toString)
    assert(killResponse._1)
    assert(killResponse._2 startsWith "Succeeded to terminate:")

    val appInfo = k8sOperation.getApplicationInfoByTag(sessionHandle.identifier.toString)
    assert(!appInfo.contains("id"))
    assert(!appInfo.contains("name"))
    assert(appInfo("state") === "FINISHED")

    val failKillResponse = k8sOperation.killApplicationByTag(sessionHandle.identifier.toString)
    assert(!failKillResponse._1)
    assert(failKillResponse._2 === ApplicationOperation.NOT_FOUND)
  }
}

class KyuubiOperationKubernetesClusterClusterModeSuite
  extends SparkClusterModeOnKubernetesSuiteBase {
  private lazy val k8sOperation: KubernetesApplicationOperation = {
    val operation = new KubernetesApplicationOperation
    operation.initialize(conf)
    operation
  }

  private def sessionManager: KyuubiSessionManager =
    server.backendService.sessionManager.asInstanceOf[KyuubiSessionManager]

  test("Spark Cluster Mode On Kubernetes Kyuubi KubernetesApplicationOperation Suite") {
    val driverPodNamePrefix = "kyuubi-spark-driver"
    conf.set(
      "spark.kubernetes.driver.pod.name",
      driverPodNamePrefix + "-" + System.currentTimeMillis())

    val sparkProcessBuilder = new SparkProcessBuilder("kyuubi", conf)
    val batchRequest = new BatchRequest(
      "spark",
      sparkProcessBuilder.mainResource.get,
      sparkProcessBuilder.mainClass,
      null,
      conf.getAll.asJava,
      Seq.empty[String].asJava)

    val sessionHandle = sessionManager.openBatchSession(
      "runner",
      "passwd",
      "localhost",
      batchRequest.getConf.asScala.toMap,
      batchRequest)

    val session = sessionManager.getSession(sessionHandle).asInstanceOf[KyuubiBatchSessionImpl]
    val batchJobSubmissionOp = session.batchJobSubmissionOp

    eventually(timeout(3.minutes), interval(50.milliseconds)) {
      val state = batchJobSubmissionOp.currentApplicationState
      assert(state.nonEmpty)
      assert(state.exists(_("state") == "Running"))
      assert(state.exists(_("name").startsWith(driverPodNamePrefix)))
    }

    val killResponse = k8sOperation.killApplicationByTag(sessionHandle.identifier.toString)
    assert(killResponse._1)
    assert(killResponse._2 startsWith "Operation of deleted appId:")

    eventually(timeout(3.minutes), interval(50.milliseconds)) {
      val appInfo = k8sOperation.getApplicationInfoByTag(sessionHandle.identifier.toString)
      // We may kill engine start but not ready
      // An EOF Error occurred when the driver was starting
      assert(appInfo("state") == "Error" || appInfo("state") == "FINISHED")
    }

    val failKillResponse = k8sOperation.killApplicationByTag(sessionHandle.identifier.toString)
    assert(!failKillResponse._1)
    assert(failKillResponse._2 === ApplicationOperation.NOT_FOUND)
  }
}
