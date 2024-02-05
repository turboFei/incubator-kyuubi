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

import java.util.Base64

import scala.collection.JavaConverters._

import org.apache.kyuubi.{KyuubiSQLException, Utils}
import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.client.KyuubiSyncThriftClient
import org.apache.kyuubi.config.{KyuubiConf, KyuubiEbayConf}
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.config.KyuubiConf.EngineOpenOnFailure._
import org.apache.kyuubi.config.KyuubiReservedKeys.{KYUUBI_ENGINE_CREDENTIALS_KEY, KYUUBI_SESSION_HANDLE_KEY, KYUUBI_SESSION_SIGN_PUBLICKEY, KYUUBI_SESSION_USER_SIGN}
import org.apache.kyuubi.engine.{EngineRef, KyuubiApplicationManager}
import org.apache.kyuubi.engine.spark.SparkProcessBuilder.YARN_QUEUE
import org.apache.kyuubi.events.{EventBus, KyuubiSessionEvent}
import org.apache.kyuubi.ha.client.DiscoveryClientProvider._
import org.apache.kyuubi.ha.client.ServiceNodeInfo
import org.apache.kyuubi.operation.{Operation, OperationHandle}
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.service.authentication.InternalSecurityAccessor
import org.apache.kyuubi.session.SessionType.SessionType
import org.apache.kyuubi.shaded.hive.service.rpc.thrift._
import org.apache.kyuubi.shaded.thrift.transport.TTransportException
import org.apache.kyuubi.sql.parser.server.KyuubiParser
import org.apache.kyuubi.sql.plan.command.RunnableCommand
import org.apache.kyuubi.util.SignUtils

class KyuubiSessionImpl(
    protocol: TProtocolVersion,
    user: String,
    password: String,
    ipAddress: String,
    conf: Map[String, String],
    override val sessionManager: KyuubiSessionManager,
    val sessionConf: KyuubiConf,
    doAsEnabled: Boolean,
    parser: KyuubiParser)
  extends KyuubiSession(protocol, user, password, ipAddress, conf, sessionManager) {

  override val sessionType: SessionType = SessionType.INTERACTIVE

  val sessionCluster = KyuubiEbayConf.getSessionCluster(sessionManager, normalizedConf)
  val sessionTag = KyuubiEbayConf.getSessionTag(normalizedConf)

  private[kyuubi] val optimizedConf: Map[String, String] = {
    val sessionClusterConf =
      sessionCluster.map(c => Map(KyuubiEbayConf.SESSION_CLUSTER.key -> c)).getOrElse(Map.empty)
    val queueConf =
      normalizedConf.get(QUEUE).map(queue => Map(YARN_QUEUE -> queue)).getOrElse(Map.empty)
    val confOverlay = sessionManager.sessionConfAdvisor.map(_.getConfOverlay(
      user,
      normalizedConf.asJava).asScala).reduce(_ ++ _)
    if (confOverlay != null) {
      sessionClusterConf ++ queueConf ++ normalizedConf ++ confOverlay
    } else {
      warn(s"the server plugin return null value for user: $user, ignore it")
      sessionClusterConf ++ queueConf ++ normalizedConf
    }
  }

  optimizedConf.foreach {
    case (USE_CATALOG, _) =>
    case (USE_DATABASE, _) =>
    case (key, value) => sessionConf.set(key, value)
  }

  private lazy val engineCredentials = renewEngineCredentials()

  lazy val engine: EngineRef = new EngineRef(
    sessionConf,
    user,
    doAsEnabled,
    sessionManager.groupProvider,
    handle.identifier.toString,
    sessionManager.applicationManager,
    sessionManager.engineStartupProcessSemaphore)
  private[kyuubi] val launchEngineOp = sessionManager.operationManager
    .newLaunchEngineOperation(this, sessionConf.get(SESSION_ENGINE_LAUNCH_ASYNC))

  private lazy val sessionUserSignBase64: String =
    SignUtils.signWithPrivateKey(user, sessionManager.signingPrivateKey)

  protected val sessionEvent = KyuubiSessionEvent(this)
  EventBus.post(sessionEvent)

  override def getSessionEvent: Option[KyuubiSessionEvent] = {
    Option(sessionEvent)
  }

  override def checkSessionAccessPathURIs(): Unit = {
    KyuubiApplicationManager.checkApplicationAccessPaths(
      sessionConf.get(ENGINE_TYPE),
      sessionConf.getAll,
      sessionManager.getConf)
  }

  @volatile protected var _client: KyuubiSyncThriftClient = _
  def client: KyuubiSyncThriftClient = _client

  @volatile protected var _engineSessionHandle: SessionHandle = _

  @volatile private var openSessionError: Option[Throwable] = None

  override def open(): Unit = handleSessionException {
    traceMetricsOnOpen()

    checkSessionAccessPathURIs()

    // we should call super.open before running launch engine operation
    super.open()

    runOperation(launchEngineOp)
    engineLastAlive = System.currentTimeMillis()
  }

  def getEngineNode: Option[ServiceNodeInfo] = {
    withDiscoveryClient(sessionConf) { discoveryClient =>
      engine.getServiceNode(discoveryClient, _client.hostPort)
    }
  }

  protected[kyuubi] def openEngineSession(extraEngineLog: Option[OperationLog] = None): Unit =
    handleSessionException {

      /** queue auto selection */
      queueAutoSelectionIfNeeded()

      withDiscoveryClient(sessionConf) { discoveryClient =>
        var openEngineSessionConf =
          optimizedConf ++ Map(KYUUBI_SESSION_HANDLE_KEY -> handle.identifier.toString)
        if (engineCredentials.nonEmpty) {
          sessionConf.set(KYUUBI_ENGINE_CREDENTIALS_KEY, engineCredentials)
          openEngineSessionConf =
            openEngineSessionConf ++ Map(KYUUBI_ENGINE_CREDENTIALS_KEY -> engineCredentials)
        }

        if (sessionConf.get(SESSION_USER_SIGN_ENABLED)) {
          openEngineSessionConf = openEngineSessionConf +
            (SESSION_USER_SIGN_ENABLED.key ->
              sessionConf.get(SESSION_USER_SIGN_ENABLED).toString) +
            (KYUUBI_SESSION_SIGN_PUBLICKEY ->
              Base64.getEncoder.encodeToString(
                sessionManager.signingPublicKey.getEncoded)) +
            (KYUUBI_SESSION_USER_SIGN -> sessionUserSignBase64)
        }

        val maxAttempts = sessionManager.getConf.get(ENGINE_OPEN_MAX_ATTEMPTS)
        val retryWait = sessionManager.getConf.get(ENGINE_OPEN_RETRY_WAIT)
        val openOnFailure =
          EngineOpenOnFailure.withName(sessionManager.getConf.get(ENGINE_OPEN_ON_FAILURE))
        var attempt = 0
        var shouldRetry = true
        while (attempt <= maxAttempts && shouldRetry) {
          val (host, port) = engine.getOrCreate(discoveryClient, extraEngineLog)

          def deregisterEngine(): Unit =
            try {
              engine.deregister(discoveryClient, (host, port))
            } catch {
              case e: Throwable =>
                warn(s"Error on de-registering engine [${engine.engineSpace} $host:$port]", e)
            }

          try {
            val passwd =
              if (sessionManager.getConf.get(ENGINE_SECURITY_ENABLED)) {
                InternalSecurityAccessor.get().issueToken()
              } else {
                Option(password).filter(_.nonEmpty).getOrElse("anonymous")
              }
            _client = KyuubiSyncThriftClient.createClient(user, passwd, host, port, sessionConf)
            _engineSessionHandle =
              _client.openSession(protocol, user, passwd, openEngineSessionConf)
            logSessionInfo(s"Connected to engine [$host:$port]/[${client.engineId.getOrElse("")}]" +
              s" with ${_engineSessionHandle}]")
            shouldRetry = false
          } catch {
            case e: TTransportException
                if attempt < maxAttempts && e.getCause.isInstanceOf[java.net.ConnectException] &&
                  e.getCause.getMessage.contains("Connection refused") =>
              warn(
                s"Failed to open [${engine.defaultEngineName} $host:$port] after" +
                  s" $attempt/$maxAttempts times, retrying",
                e.getCause)
              Thread.sleep(retryWait)
              openOnFailure match {
                case DEREGISTER_IMMEDIATELY => deregisterEngine()
                case _ =>
              }
              shouldRetry = true
            case e: Throwable =>
              error(
                s"Opening engine [${engine.defaultEngineName} $host:$port]" +
                  s" for $user session failed",
                e)
              openSessionError = Some(e)
              openOnFailure match {
                case DEREGISTER_IMMEDIATELY | DEREGISTER_AFTER_RETRY => deregisterEngine()
                case _ =>
              }
              throw e
          } finally {
            attempt += 1
            if (shouldRetry && _client != null) {
              try {
                _client.closeSession()
              } catch {
                case e: Throwable =>
                  warn(
                    "Error on closing broken client of engine " +
                      s"[${engine.defaultEngineName} $host:$port]",
                    e)
              }
            }
          }
        }
        sessionEvent.openedTime = System.currentTimeMillis()
        sessionEvent.remoteSessionId = _engineSessionHandle.identifier.toString
        _client.engineId.foreach(e => sessionEvent.engineId = e)
        EventBus.post(sessionEvent)
      }
    }

  override protected def runOperation(operation: Operation): OperationHandle = {
    if (operation != launchEngineOp) {
      try {
        waitForEngineLaunched()
      } catch {
        case t: Throwable =>
          operation.close()
          throw t
      }
      sessionEvent.totalOperations += 1
    }
    super.runOperation(operation)
  }

  @volatile private var engineLaunched: Boolean = false

  protected def waitForEngineLaunched(): Unit = {
    if (!engineLaunched) {
      Option(launchEngineOp).foreach { op =>
        val waitingStartTime = System.currentTimeMillis()
        logSessionInfo(s"Starting to wait the launch engine operation finished")

        op.getBackgroundHandle.get()

        val elapsedTime = System.currentTimeMillis() - waitingStartTime
        logSessionInfo(s"Engine has been launched, elapsed time: ${elapsedTime / 1000} s")

        if (_engineSessionHandle == null) {
          val ex = op.getStatus.exception.getOrElse(
            KyuubiSQLException(s"Failed to launch engine for $handle"))
          throw ex
        }

        engineLaunched = true
      }
    }
  }

  private def renewEngineCredentials(): String = {
    try {
      sessionManager.credentialsManager.renewCredentials(engine.appUser, sessionCluster)
    } catch {
      case e: Exception =>
        error(s"Failed to renew engine credentials for $handle", e)
        ""
    }
  }

  override def close(): Unit = {
    super.close()
    sessionManager.credentialsManager.removeSessionCredentialsEpoch(handle.identifier.toString)
    try {
      if (_client != null) _client.closeSession()
    } finally {
      openSessionError.foreach { _ => if (engine != null) engine.close() }
      sessionEvent.endTime = System.currentTimeMillis()
      EventBus.post(sessionEvent)
      traceMetricsOnClose()
    }
  }

  override def getInfo(infoType: TGetInfoType): TGetInfoValue = {
    sessionConf.get(SERVER_INFO_PROVIDER) match {
      case "SERVER" => super.getInfo(infoType)
      case "ENGINE" => withAcquireRelease() {
          waitForEngineLaunched()
          sessionManager.credentialsManager.sendCredentialsIfNeeded(
            handle.identifier.toString,
            engine.appUser,
            client.sendCredentials,
            sessionCluster)
          client.getInfo(infoType).getInfoValue
        }
      case unknown => throw new IllegalArgumentException(s"Unknown server info provider $unknown")
    }
  }

  override def executeStatement(
      statement: String,
      confOverlay: Map[String, String],
      runAsync: Boolean,
      queryTimeout: Long): OperationHandle = withAcquireRelease() {
    val kyuubiNode = parser.parsePlan(statement)
    kyuubiNode match {
      case command: RunnableCommand =>
        val operation = sessionManager.operationManager.newExecuteOnServerOperation(
          this,
          runAsync,
          command)
        runOperation(operation)
      case _ => super.executeStatement(statement, confOverlay, runAsync, queryTimeout)
    }
  }

  @volatile private var engineLastAlive: Long = _
  private val engineAliveTimeout = sessionConf.get(KyuubiConf.ENGINE_ALIVE_TIMEOUT)
  private val aliveProbeEnabled = sessionConf.get(KyuubiConf.ENGINE_ALIVE_PROBE_ENABLED)
  private val engineAliveMaxFailCount = sessionConf.get(KyuubiConf.ENGINE_ALIVE_MAX_FAILURES)
  @volatile private var engineAliveFailCount = 0

  def checkEngineConnectionAlive(): Boolean = {
    try {
      if (Option(client).exists(_.engineConnectionClosed)) return false
      if (!aliveProbeEnabled) return true
      getInfo(TGetInfoType.CLI_DBMS_VER)
      engineLastAlive = System.currentTimeMillis()
      engineAliveFailCount = 0
      true
    } catch {
      case e: Throwable =>
        val now = System.currentTimeMillis()
        engineAliveFailCount = engineAliveFailCount + 1
        if (now - engineLastAlive > engineAliveTimeout &&
          engineAliveFailCount >= engineAliveMaxFailCount) {
          error(s"The engineRef[${engine.getEngineRefId}] is marked as not alive "
            + s"due to a lack of recent successful alive probes. "
            + s"The time since last successful probe: "
            + s"${now - engineLastAlive} ms exceeds the timeout of $engineAliveTimeout ms. "
            + s"The engine has failed $engineAliveFailCount times, "
            + s"surpassing the maximum failure count of $engineAliveMaxFailCount.")
          false
        } else {
          warn(
            s"The engineRef[${engine.getEngineRefId}] alive probe fails, " +
              s"${now - engineLastAlive} ms exceeds timeout $engineAliveTimeout ms, " +
              s"and has failed $engineAliveFailCount times.",
            e)
          true
        }
    }
  }

  /** Auto select queue from BDP. */
  private def queueAutoSelectionIfNeeded(): Unit = {
    if (sessionManager.getConf.get(KyuubiEbayConf.ACCESS_BDP_QUEUE_AUTO_SELECTION)) {
      if (sessionConf.getOption(YARN_QUEUE).isEmpty) {
        val bdpCluster = sessionConf.get(KyuubiEbayConf.ACCESS_BDP_CLUSTER)
        try {
          val ignoreQueues =
            sessionManager.getConf.get(KyuubiEbayConf.ACCESS_BDP_QUEUE_AUTO_SELECTION_IGNORE_LIST)
          val queues =
            sessionManager.bdpManager.getUserQueues(user, bdpCluster).filterNot { queue =>
              ignoreQueues.contains(queue.getName)
            }
          if (queues.nonEmpty) {
            val queue = queues(scala.util.Random.nextInt(queues.size))
            logSessionInfo(
              s"Select queue[$queue] for $user/$sessionCluster/$bdpCluster automatically")
            sessionConf.set(YARN_QUEUE, queue.getName)
          }
        } catch {
          case e: Throwable =>
            logSessionInfo(
              s"Error selecting queues for $user/$sessionCluster/$bdpCluster automatically: " +
                Utils.prettyPrint(e))
        }
      }
    }
  }
}
