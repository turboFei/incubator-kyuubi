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

package org.apache.kyuubi.session

import java.util.concurrent.{ConcurrentHashMap, Semaphore, TimeUnit}

import scala.collection.JavaConverters._

import com.codahale.metrics.MetricRegistry
import com.google.common.annotations.VisibleForTesting
import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import org.apache.hive.service.rpc.thrift.TProtocolVersion

import org.apache.kyuubi.{KyuubiException, KyuubiSQLException}
import org.apache.kyuubi.client.api.v1.dto.{Batch, BatchRequest}
import org.apache.kyuubi.config.{KyuubiConf, KyuubiEbayConf}
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.config.KyuubiEbayConf._
import org.apache.kyuubi.credentials.HadoopCredentialsManager
import org.apache.kyuubi.ebay.{BatchLogAggManager, BdpAccessManager}
import org.apache.kyuubi.ebay.carmel.gateway.session.{CarmelEndpointManager, CarmelSessionImpl}
import org.apache.kyuubi.engine.KyuubiApplicationManager
import org.apache.kyuubi.metrics.MetricsConstants._
import org.apache.kyuubi.metrics.MetricsSystem
import org.apache.kyuubi.operation.{KyuubiOperationManager, OperationState}
import org.apache.kyuubi.plugin.{GroupProvider, PluginLoader, SessionConfAdvisor}
import org.apache.kyuubi.server.metadata.{MetadataManager, MetadataRequestsRetryRef}
import org.apache.kyuubi.server.metadata.api.{Metadata, MetadataFilter}
import org.apache.kyuubi.sql.parser.server.KyuubiParser
import org.apache.kyuubi.util.{SignUtils, ThreadUtils}

class KyuubiSessionManager private (name: String) extends SessionManager(name) {

  def this() = this(classOf[KyuubiSessionManager].getSimpleName)

  private val parser = new KyuubiParser()

  val operationManager = new KyuubiOperationManager()
  val credentialsManager = new HadoopCredentialsManager()
  val applicationManager = new KyuubiApplicationManager()

  // Currently, the metadata manager is used by the REST frontend which provides batch job APIs,
  // so we initialize it only when Kyuubi starts with the REST frontend.
  lazy val metadataManager: Option[MetadataManager] =
    if (conf.isRESTEnabled) Some(new MetadataManager()) else None

  private val limiters = new ConcurrentHashMap[Option[String], SessionLimiter]().asScala
  private val batchLimiters = new ConcurrentHashMap[Option[String], SessionLimiter]().asScala
  // lazy is required for plugins since the conf is null when this class initialization
  lazy val sessionConfAdvisor: SessionConfAdvisor = PluginLoader.loadSessionConfAdvisor(conf)
  lazy val groupProvider: GroupProvider = PluginLoader.loadGroupProvider(conf)

  lazy val (signingPrivateKey, signingPublicKey) = SignUtils.generateKeyPair()

  var engineStartupProcessSemaphore: Option[Semaphore] = None

  private val engineConnectionAliveChecker =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor(s"$name-engine-alive-checker")

  val bdpManager = new BdpAccessManager()
  val carmelEndpointManager = new CarmelEndpointManager(this)

  override def initialize(conf: KyuubiConf): Unit = {
    this.conf = conf
    addService(applicationManager)
    addService(credentialsManager)
    addService(new BatchLogAggManager())
    addService(bdpManager)
    metadataManager.foreach(addService)
    initSessionLimiter(conf)
    initEngineStartupProcessSemaphore(conf)
    addService(carmelEndpointManager)
    super.initialize(conf)
  }

  override protected def createSession(
      protocol: TProtocolVersion,
      user: String,
      password: String,
      ipAddress: String,
      conf: Map[String, String]): Session = {
    val sessionCluster = getSessionCluster(conf)
    limiters.get(sessionCluster).foreach(_.increment(UserIpAddress(user, ipAddress)))
    val clusterConf = getClusterConf(conf)

    if (KyuubiEbayConf.isCarmelCluster(getConf, sessionCluster)) {
      new CarmelSessionImpl(
        protocol,
        user,
        password,
        ipAddress,
        conf,
        this,
        clusterConf.getUserDefaults(user),
        parser)
    } else {
      new KyuubiSessionImpl(
        protocol,
        user,
        password,
        ipAddress,
        conf,
        this,
        clusterConf.getUserDefaults(user),
        parser)
    }
  }

  override def openSession(
      protocol: TProtocolVersion,
      user: String,
      password: String,
      ipAddress: String,
      conf: Map[String, String]): SessionHandle = {
    val username = Option(user).filter(_.nonEmpty).getOrElse("anonymous")
    try {
      super.openSession(protocol, username, password, ipAddress, conf)
    } catch {
      case e: Throwable =>
        MetricsSystem.tracing { ms =>
          ms.incCount(CONN_FAIL)
          ms.incCount(MetricRegistry.name(CONN_FAIL, user))
          ms.incCount(MetricRegistry.name(CONN_FAIL, SessionType.INTERACTIVE.toString))
        }
        throw KyuubiSQLException(
          s"Error opening session for $username client ip $ipAddress, due to ${e.getMessage}",
          e)
    }
  }

  override def closeSession(sessionHandle: SessionHandle): Unit = {
    val session = getSession(sessionHandle).asInstanceOf[KyuubiSession]
    try {
      super.closeSession(sessionHandle)
    } finally {
      session match {
        case _: KyuubiBatchSession =>
          batchLimiters.get(session.sessionCluster).foreach(_.decrement(UserIpAddress(
            session.user,
            session.ipAddress)))
        case _ =>
          limiters.get(session.sessionCluster).foreach(_.decrement(UserIpAddress(
            session.user,
            session.ipAddress)))
      }
    }
  }

  // scalastyle:off
  def createBatchSession(
      user: String,
      password: String,
      ipAddress: String,
      conf: Map[String, String],
      batchType: String,
      batchName: Option[String],
      resource: String,
      className: String,
      batchConf: Map[String, String],
      batchArgs: Seq[String],
      recoveryMetadata: Option[Metadata] = None,
      shouldRunAsync: Boolean): KyuubiBatchSession = {
    // scalastyle:on
    val username = Option(user).filter(_.nonEmpty).getOrElse("anonymous")
    val clusterConf = getClusterConf(conf)
    val sessionConf = clusterConf.getUserDefaults(user)
    KyuubiEbayConf.getBatchTagDefaultConf(
      conf,
      BATCH_SPARK_HBASE_ENABLED,
      BATCH_SPARK_HBASE_CONFIG_TAG,
      clusterConf,
      sessionConf).foreach { case (key, value) =>
      sessionConf.set(key, value)
    }
    new KyuubiBatchSession(
      username,
      password,
      ipAddress,
      conf,
      this,
      sessionConf,
      batchType,
      batchName,
      resource,
      className,
      batchConf,
      batchArgs,
      recoveryMetadata,
      shouldRunAsync)
  }

  private[kyuubi] def openBatchSession(batchSession: KyuubiBatchSession): SessionHandle = {
    val user = batchSession.user
    val ipAddress = batchSession.ipAddress
    batchLimiters.get(batchSession.sessionCluster).foreach(_.increment(UserIpAddress(
      user,
      ipAddress)))
    val handle = batchSession.handle
    try {
      batchSession.open()
      setSession(handle, batchSession)
      logSessionCountInfo(batchSession, "opened")
      handle
    } catch {
      case e: Exception =>
        try {
          batchSession.close()
        } catch {
          case t: Throwable =>
            warn(s"Error closing batch session[$handle] for $user client ip: $ipAddress", t)
        }
        MetricsSystem.tracing { ms =>
          ms.incCount(CONN_FAIL)
          ms.incCount(MetricRegistry.name(CONN_FAIL, user))
          ms.incCount(MetricRegistry.name(CONN_FAIL, SessionType.BATCH.toString))
        }
        throw KyuubiSQLException(
          s"Error opening batch session[$handle] for $user client ip $ipAddress," +
            s" due to ${e.getMessage}",
          e)
    }
  }

  def openBatchSession(
      user: String,
      password: String,
      ipAddress: String,
      conf: Map[String, String],
      batchRequest: BatchRequest,
      shouldRunAsync: Boolean = true): SessionHandle = {
    if (KyuubiEbayConf.isCarmelCluster(getConf, getSessionCluster(conf))) {
      throw new KyuubiException(s"Batch function is disallowed for carmel cluster.")
    }
    val batchSession = createBatchSession(
      user,
      password,
      ipAddress,
      conf,
      batchRequest.getBatchType,
      Option(batchRequest.getName),
      batchRequest.getResource,
      batchRequest.getClassName,
      batchRequest.getConf.asScala.toMap,
      batchRequest.getArgs.asScala,
      None,
      shouldRunAsync)
    openBatchSession(batchSession)
  }

  def getBatchSession(sessionHandle: SessionHandle): Option[KyuubiBatchSession] = {
    getSessionOption(sessionHandle).map(_.asInstanceOf[KyuubiBatchSession])
  }

  def insertMetadata(metadata: Metadata): Unit = {
    metadataManager.foreach(_.insertMetadata(metadata))
  }

  def updateMetadata(metadata: Metadata): Unit = {
    metadataManager.foreach(_.updateMetadata(metadata))
  }

  def getMetadataRequestsRetryRef(identifier: String): Option[MetadataRequestsRetryRef] = {
    metadataManager.flatMap(mm => Option(mm.getMetadataRequestsRetryRef(identifier)))
  }

  def deRegisterMetadataRequestsRetryRef(identifier: String): Unit = {
    metadataManager.foreach(_.deRegisterRequestsRetryRef(identifier))
  }

  def getBatchFromMetadataStore(batchId: String): Option[Batch] = {
    metadataManager.flatMap(mm => mm.getBatch(batchId))
  }

  def getBatchesFromMetadataStore(filter: MetadataFilter, from: Int, size: Int): Seq[Batch] = {
    metadataManager.map(_.getBatches(filter, from, size)).getOrElse(Seq.empty)
  }

  def getBatchMetadata(batchId: String): Option[Metadata] = {
    metadataManager.flatMap(_.getBatchSessionMetadata(batchId))
  }

  @VisibleForTesting
  def cleanupMetadata(identifier: String): Unit = {
    metadataManager.foreach(_.cleanupMetadataById(identifier))
  }

  override def start(): Unit = synchronized {
    MetricsSystem.tracing { ms =>
      ms.registerGauge(CONN_OPEN, getOpenSessionCount, 0)
      ms.registerGauge(EXEC_POOL_ALIVE, getExecPoolSize, 0)
      ms.registerGauge(EXEC_POOL_ACTIVE, getActiveCount, 0)
      ms.registerGauge(EXEC_POOL_WORK_QUEUE_SIZE, getWorkQueueSize, 0)
    }
    super.start()
    startEngineAliveChecker()
  }

  def getBatchSessionsToRecover(kyuubiInstance: String): Seq[KyuubiBatchSession] = {
    Seq(OperationState.PENDING, OperationState.RUNNING).flatMap { stateToRecover =>
      metadataManager.map(_.getBatchesRecoveryMetadata(
        stateToRecover.toString,
        kyuubiInstance,
        0,
        Int.MaxValue).map { metadata =>
        createBatchSession(
          metadata.username,
          "anonymous",
          metadata.ipAddress,
          metadata.requestConf,
          metadata.engineType,
          Option(metadata.requestName),
          metadata.resource,
          metadata.className,
          metadata.requestConf,
          metadata.requestArgs,
          Some(metadata),
          shouldRunAsync = true)
      }).getOrElse(Seq.empty)
    }
  }

  def getPeerInstanceClosedBatchSessions(kyuubiInstance: String): Seq[Metadata] = {
    Seq(OperationState.PENDING, OperationState.RUNNING).flatMap { stateToKill =>
      metadataManager.map(_.getPeerInstanceClosedBatchesMetadata(
        stateToKill.toString,
        kyuubiInstance,
        0,
        Int.MaxValue)).getOrElse(Seq.empty)
    }
  }

  override protected def isServer: Boolean = true

  private def initSessionLimiter(conf: KyuubiConf): Unit = {
    KyuubiEbayConf.getClusterOptList(conf).foreach { clusterOpt =>
      val clusterConf = KyuubiEbayConf.loadClusterConf(conf, clusterOpt)
      val userLimit = clusterConf.get(SERVER_LIMIT_CONNECTIONS_PER_USER).getOrElse(0)
      val ipAddressLimit = clusterConf.get(SERVER_LIMIT_CONNECTIONS_PER_IPADDRESS).getOrElse(0)
      val userIpAddressLimit =
        clusterConf.get(SERVER_LIMIT_CONNECTIONS_PER_USER_IPADDRESS).getOrElse(0)
      val userUnlimitedList =
        clusterConf.get(KyuubiConf.SERVER_LIMIT_CONNECTIONS_USER_UNLIMITED_LIST)
      applySessionLimiter(
        userLimit,
        ipAddressLimit,
        userIpAddressLimit,
        userUnlimitedList).foreach {
        limiter =>
          limiters.put(clusterOpt, limiter)
      }

      val userBatchLimit = clusterConf.get(SERVER_LIMIT_BATCH_CONNECTIONS_PER_USER).getOrElse(0)
      val ipAddressBatchLimit = clusterConf.get(SERVER_LIMIT_BATCH_CONNECTIONS_PER_IPADDRESS)
        .getOrElse(0)
      val userIpAddressBatchLimit =
        clusterConf.get(SERVER_LIMIT_BATCH_CONNECTIONS_PER_USER_IPADDRESS).getOrElse(0)
      applySessionLimiter(
        userBatchLimit,
        ipAddressBatchLimit,
        userIpAddressBatchLimit,
        userUnlimitedList).foreach {
        batchLimiter =>
          batchLimiters.put(clusterOpt, batchLimiter)
      }
    }
  }

  private[kyuubi] def getUnlimitedUsers(cluster: Option[String]): Set[String] = {
    limiters.get(cluster).orElse(batchLimiters.get(cluster)).map(SessionLimiter.getUnlimitedUsers)
      .getOrElse(Set.empty)
  }

  private[kyuubi] def refreshUnlimitedUsers(cluster: Option[String], conf: KyuubiConf): Unit = {
    val userUnlimitedList = conf.get(SERVER_LIMIT_CONNECTIONS_USER_UNLIMITED_LIST).toSet
    limiters.get(cluster).foreach(SessionLimiter.resetUnlimitedUsers(_, userUnlimitedList))
    batchLimiters.get(cluster).foreach(SessionLimiter.resetUnlimitedUsers(_, userUnlimitedList))
  }

  private def applySessionLimiter(
      userLimit: Int,
      ipAddressLimit: Int,
      userIpAddressLimit: Int,
      userUnlimitedList: Seq[String]): Option[SessionLimiter] = {
    Seq(userLimit, ipAddressLimit, userIpAddressLimit).find(_ > 0).map(_ =>
      SessionLimiter(userLimit, ipAddressLimit, userIpAddressLimit, userUnlimitedList.toSet))
  }

  /**
   * Session conf for cluster mode.
   */
  private lazy val SESSION_CLUSTER_CONF_CACHE: LoadingCache[String, KyuubiConf] =
    CacheBuilder.newBuilder()
      .expireAfterWrite(conf.get(SESSION_CLUSTER_CONF_REFRESH_INTERVAL), TimeUnit.MILLISECONDS)
      .build(new CacheLoader[String, KyuubiConf] {
        override def load(cluster: String): KyuubiConf = {
          KyuubiEbayConf.loadClusterConf(conf, Option(cluster))
        }
      })

  private def getSessionCluster(sessionConf: Map[String, String]): Option[String] = {
    KyuubiEbayConf.getSessionCluster(this, sessionConf)
  }

  private[kyuubi] def getClusterConf(sessionConf: Map[String, String]): KyuubiConf = {
    getClusterConf(getSessionCluster(sessionConf))
  }

  private[kyuubi] def getClusterConf(clusterOpt: Option[String]): KyuubiConf = {
    try {
      clusterOpt.map { c =>
        info(s"Getting session conf for cluster $c")
        SESSION_CLUSTER_CONF_CACHE.get(c)
      }.getOrElse(conf)
    } catch {
      case e: Throwable =>
        error(s"Error getting session conf for cluster ${clusterOpt.getOrElse("")}", e)
        KyuubiEbayConf.loadClusterConf(conf, clusterOpt)
    }
  }

  override protected def logSessionCountInfo(session: Session, action: String): Unit = {
    info(s"${session.user}'s session with" +
      s" ${session.handle}" +
      s"${session.asInstanceOf[KyuubiSession].sessionCluster.map("/" + _).getOrElse("")}" +
      s"${session.name.map("/" + _).getOrElse("")} is $action," +
      s" current opening sessions $getOpenSessionCount")
  }

  private def startEngineAliveChecker(): Unit = {
    val interval = conf.get(KyuubiConf.ENGINE_ALIVE_PROBE_INTERVAL)
    val checkTask: Runnable = () => {
      allSessions().foreach { session =>
        if (!session.asInstanceOf[KyuubiSessionImpl].checkEngineConnectionAlive()) {
          try {
            closeSession(session.handle)
            logger.info(s"The session ${session.handle} has been closed " +
              s"due to engine unresponsiveness (checked by the engine alive checker).")
          } catch {
            case e: KyuubiSQLException =>
              warn(s"Error closing session ${session.handle}", e)
          }
        }
      }
    }
    engineConnectionAliveChecker.scheduleWithFixedDelay(
      checkTask,
      interval,
      interval,
      TimeUnit.MILLISECONDS)
  }

  private def initEngineStartupProcessSemaphore(conf: KyuubiConf): Unit = {
    val engineCreationLimit = conf.get(KyuubiConf.SERVER_LIMIT_ENGINE_CREATION)
    engineCreationLimit.filter(_ > 0).foreach { limit =>
      engineStartupProcessSemaphore = Some(new Semaphore(limit))
    }
  }
}
