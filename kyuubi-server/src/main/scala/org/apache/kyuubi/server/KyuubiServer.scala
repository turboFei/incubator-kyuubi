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

package org.apache.kyuubi.server

import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import scala.util.Properties

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation

import org.apache.kyuubi._
import org.apache.kyuubi.config.{KyuubiConf, KyuubiEbayConf}
import org.apache.kyuubi.config.KyuubiConf.{FRONTEND_PROTOCOLS, FrontendProtocols}
import org.apache.kyuubi.config.KyuubiConf.FrontendProtocols._
import org.apache.kyuubi.events.{EventBus, KyuubiServerInfoEvent, ServerEventHandlerRegister}
import org.apache.kyuubi.ha.HighAvailabilityConf._
import org.apache.kyuubi.ha.client.{AuthTypes, ServiceDiscovery}
import org.apache.kyuubi.metrics.{MetricsConf, MetricsSystem}
import org.apache.kyuubi.server.metadata.jdbc.JDBCMetadataStoreConf
import org.apache.kyuubi.service.{AbstractBackendService, AbstractFrontendService, Serverable, ServiceState}
import org.apache.kyuubi.session.KyuubiSessionManager
import org.apache.kyuubi.util.{KyuubiHadoopUtils, SignalRegister}
import org.apache.kyuubi.zookeeper.EmbeddedZookeeper

object KyuubiServer extends Logging {
  private val zkServer = new EmbeddedZookeeper()
  private[kyuubi] var kyuubiServer: KyuubiServer = _

  private var clusterModeEnabled: Boolean = _
  private val clusterHadoopConf = new ConcurrentHashMap[Option[String], Configuration]().asScala

  def startServer(conf: KyuubiConf): KyuubiServer = {
    clusterModeEnabled = conf.get(KyuubiEbayConf.SESSION_CLUSTER_MODE_ENABLED)
    loadHadoopConf(Some(conf))
    if (!ServiceDiscovery.supportServiceDiscovery(conf)) {
      zkServer.initialize(conf)
      zkServer.start()
      conf.set(HA_ADDRESSES, zkServer.getConnectString)
      conf.set(HA_ZK_AUTH_TYPE, AuthTypes.NONE.toString)
    }

    val server = conf.get(KyuubiConf.SERVER_NAME) match {
      case Some(s) => new KyuubiServer(s)
      case _ => new KyuubiServer()
    }
    try {
      server.initialize(conf)
    } catch {
      case e: Exception =>
        if (zkServer.getServiceState == ServiceState.STARTED) {
          zkServer.stop()
        }
        throw e
    }
    server.start()
    Utils.addShutdownHook(() => server.stop(), Utils.SERVER_SHUTDOWN_PRIORITY)
    server
  }

  def main(args: Array[String]): Unit = {
    info(
      """
        |                  Welcome to
        |  __  __                           __
        | /\ \/\ \                         /\ \      __
        | \ \ \/'/'  __  __  __  __  __  __\ \ \____/\_\
        |  \ \ , <  /\ \/\ \/\ \/\ \/\ \/\ \\ \ '__`\/\ \
        |   \ \ \\`\\ \ \_\ \ \ \_\ \ \ \_\ \\ \ \L\ \ \ \
        |    \ \_\ \_\/`____ \ \____/\ \____/ \ \_,__/\ \_\
        |     \/_/\/_/`/___/> \/___/  \/___/   \/___/  \/_/
        |                /\___/
        |                \/__/
       """.stripMargin)
    info(s"Version: $KYUUBI_VERSION, Revision: $REVISION ($REVISION_TIME), Branch: $BRANCH," +
      s" Java: $JAVA_COMPILE_VERSION, Scala: $SCALA_COMPILE_VERSION," +
      s" Spark: $SPARK_COMPILE_VERSION, Hadoop: $HADOOP_COMPILE_VERSION," +
      s" Hive: $HIVE_COMPILE_VERSION, Flink: $FLINK_COMPILE_VERSION," +
      s" Trino: $TRINO_COMPILE_VERSION")
    info(s"Using Scala ${Properties.versionString}, ${Properties.javaVmName}," +
      s" ${Properties.javaVersion}")
    SignalRegister.registerLogger(logger)

    // register conf entries
    JDBCMetadataStoreConf
    val conf = new KyuubiConf().loadFileDefaults()
    UserGroupInformation.setConfiguration(KyuubiHadoopUtils.newHadoopConf(conf))
    startServer(conf)
  }

  def isClusterModeEnabled: Boolean = clusterModeEnabled

  def loadHadoopConf(conf: Option[KyuubiConf] = None): Unit = synchronized {
    val kyuubiConf = conf.getOrElse(new KyuubiConf().loadFileDefaults())
    if (clusterModeEnabled) {
      KyuubiEbayConf.getClusterList(kyuubiConf).foreach { cluster =>
        val clusterKyuubiConf = KyuubiEbayConf.loadClusterConf(kyuubiConf, Option(cluster))
        clusterHadoopConf.put(
          Option(cluster),
          KyuubiHadoopUtils.newHadoopConf(
            clusterKyuubiConf,
            clusterOpt = Option(cluster)))
      }
    } else {
      clusterHadoopConf.put(None, KyuubiHadoopUtils.newHadoopConf(kyuubiConf))
    }
  }

  def getHadoopConf(clusterOpt: Option[String]): Configuration = {
    clusterHadoopConf.get(clusterOpt).get
  }

  private[kyuubi] def refreshUserDefaultsConf(): Unit = kyuubiServer.conf.synchronized {
    val existedUserDefaults = kyuubiServer.conf.getAllUserDefaults
    val refreshedUserDefaults = KyuubiConf().loadFileDefaults().getAllUserDefaults
    var (unsetCount, updatedCount, addedCount) = (0, 0, 0)
    for ((k, _) <- existedUserDefaults if !refreshedUserDefaults.contains(k)) {
      kyuubiServer.conf.unset(k)
      unsetCount = unsetCount + 1
    }
    for ((k, v) <- refreshedUserDefaults) {
      if (existedUserDefaults.contains(k)) {
        if (!StringUtils.equals(existedUserDefaults.get(k).orNull, v)) {
          updatedCount = updatedCount + 1
        }
      } else {
        addedCount = addedCount + 1
      }
      kyuubiServer.conf.set(k, v)
    }
    info(s"Refreshed user defaults configs with changes of " +
      s"unset: $unsetCount, updated: $updatedCount, added: $addedCount")
  }

  private[kyuubi] def refreshUnlimitedUsers(): Unit = synchronized {
    val conf = KyuubiConf().loadFileDefaults()
    val sessionMgr = kyuubiServer.backendService.sessionManager.asInstanceOf[KyuubiSessionManager]
    KyuubiEbayConf.getClusterOptList(conf).foreach { clusterOpt =>
      val existingUnlimitedUsers = sessionMgr.getUnlimitedUsers(clusterOpt)
      sessionMgr.refreshUnlimitedUsers(clusterOpt, KyuubiEbayConf.loadClusterConf(conf, clusterOpt))
      val refreshedUnlimitedUsers = sessionMgr.getUnlimitedUsers(clusterOpt)
      info(s"Refreshed unlimited users from $existingUnlimitedUsers to $refreshedUnlimitedUsers")
    }
  }
}

class KyuubiServer(name: String) extends Serverable(name) {

  def this() = this(classOf[KyuubiServer].getSimpleName)

  override val backendService: AbstractBackendService =
    new KyuubiBackendService() with BackendServiceMetric

  override lazy val frontendServices: Seq[AbstractFrontendService] =
    conf.get(FRONTEND_PROTOCOLS).map(FrontendProtocols.withName).map {
      case THRIFT_BINARY => new KyuubiTBinaryFrontendService(this)
      case THRIFT_BINARY_SSL => new KyuubiTBinarySSLFrontendService(this)
      case THRIFT_HTTP => new KyuubiTHttpFrontendService(this)
      case REST =>
        warn("REST frontend protocol is experimental, API may change in the future.")
        new KyuubiRestFrontendService(this)
      case MYSQL =>
        warn("MYSQL frontend protocol is experimental.")
        new KyuubiMySQLFrontendService(this)
      case TRINO =>
        warn("Trino frontend protocol is experimental.")
        new KyuubiTrinoFrontendService(this)
      case other =>
        throw new UnsupportedOperationException(s"Frontend protocol $other is not supported yet.")
    }

  override def initialize(conf: KyuubiConf): Unit = synchronized {
    initLoggerEventHandler(conf)

    val kinit = new KinitAuxiliaryService()
    addService(kinit)

    val periodicGCService = new PeriodicGCService
    addService(periodicGCService)

    if (conf.get(MetricsConf.METRICS_ENABLED)) {
      addService(new MetricsSystem)
    }
    super.initialize(conf)
  }

  override def start(): Unit = {
    super.start()
    KyuubiServer.kyuubiServer = this
    KyuubiServerInfoEvent(this, ServiceState.STARTED).foreach(EventBus.post)
  }

  override def stop(): Unit = {
    KyuubiServerInfoEvent(this, ServiceState.STOPPED).foreach(EventBus.post)
    super.stop()
  }

  private def initLoggerEventHandler(conf: KyuubiConf): Unit = {
    ServerEventHandlerRegister.registerEventLoggers(conf)
  }

  override protected def stopServer(): Unit = {
    EventBus.deregisterAll()
  }
}
