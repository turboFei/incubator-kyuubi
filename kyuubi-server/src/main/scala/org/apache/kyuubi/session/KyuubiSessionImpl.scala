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

import com.codahale.metrics.MetricRegistry
import org.apache.hive.service.rpc.thrift._
import org.apache.thrift.TException
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.{TSocket, TTransport}

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.client.KyuubiSyncThriftClient
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.engine.EngineRef
import org.apache.kyuubi.events.KyuubiSessionEvent
import org.apache.kyuubi.ha.client.ZooKeeperClientProvider._
import org.apache.kyuubi.metrics.MetricsConstants._
import org.apache.kyuubi.metrics.MetricsSystem
import org.apache.kyuubi.operation.{Operation, OperationHandle}
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.server.EventLoggingService
import org.apache.kyuubi.service.authentication.PlainSASLHelper

class KyuubiSessionImpl(
    protocol: TProtocolVersion,
    user: String,
    password: String,
    ipAddress: String,
    conf: Map[String, String],
    override val sessionManager: KyuubiSessionManager,
    sessionConf: KyuubiConf)
  extends AbstractSession(protocol, user, password, ipAddress, conf, sessionManager) {

  // TODO: needs improve the hardcode
  normalizedConf.foreach {
    case ("use:database", _) =>
    case ("kyuubi.engine.pool.size.threshold", _) =>
    case (key, value) => sessionConf.set(key, value)
  }

  val engine: EngineRef = new EngineRef(sessionConf, user)
  val launchEngineAsync = sessionConf.get(SESSION_ENGINE_LAUNCH_ASYNC)
  private[kyuubi] val launchEngineOp = sessionManager.operationManager.newLaunchEngineOperation(
    this, launchEngineAsync)
  @volatile
  var engineLaunched: Boolean = false

  private val sessionEvent = KyuubiSessionEvent(this)
  EventLoggingService.onEvent(sessionEvent)

  private var transport: TTransport = _
  private var _client: KyuubiSyncThriftClient = _
  def client: KyuubiSyncThriftClient = _client

  override val handle: SessionHandle = SessionHandle(protocol)
  private var _engineSessionHandle: SessionHandle = _

  override def open(): Unit = {
    MetricsSystem.tracing { ms =>
      ms.incCount(CONN_TOTAL)
      ms.incCount(MetricRegistry.name(CONN_OPEN, user))
    }

    // we should call super.open before running launch engine operation
    super.open()

    runOperation(launchEngineOp)
  }

  private[kyuubi] def openEngineSession(extraEngineLog: Option[OperationLog] = None): Unit = {
    withZkClient(sessionConf) { zkClient =>
      val (host, port) = engine.getOrCreate(zkClient, extraEngineLog)
      val passwd = Option(password).filter(_.nonEmpty).getOrElse("anonymous")
      val loginTimeout = sessionConf.get(ENGINE_LOGIN_TIMEOUT).toInt
      val requestTimeout = sessionConf.get(ENGINE_REQUEST_TIMEOUT).toInt
      transport = PlainSASLHelper.getPlainTransport(
        user, passwd, new TSocket(host, port, requestTimeout, loginTimeout))
      if (!transport.isOpen) {
        transport.open()
        logSessionInfo(s"Connected to engine [$host:$port]")
      }
      _client = new KyuubiSyncThriftClient(new TBinaryProtocol(transport))
      _engineSessionHandle = _client.openSession(protocol, user, passwd, normalizedConf)
      logSessionInfo(s"Opened engine session[${_engineSessionHandle}]")
      sessionEvent.openedTime = System.currentTimeMillis()
      sessionEvent.sessionId = handle.identifier.toString
      sessionEvent.remoteSessionId = _engineSessionHandle.identifier.toString
      sessionEvent.clientVersion = handle.protocol.getValue
      EventLoggingService.onEvent(sessionEvent)
    }
  }

  override protected def runOperation(operation: Operation): OperationHandle = {
    if (operation != launchEngineOp) {
      waitForEngineLaunched()
      sessionEvent.totalOperations += 1
    }
    super.runOperation(operation)
  }

  private def waitForEngineLaunched(): Unit = {
    if (!engineLaunched) {
      Option(launchEngineOp).foreach { op =>
        val waitingStartTime = System.currentTimeMillis()
        logSessionInfo(s"Starting to wait the launch engine operation finished")

        op.getBackgroundHandle.get()

        val elapsedTime = System.currentTimeMillis() - waitingStartTime
        logSessionInfo(s"Engine has been launched, elapsed time: ${elapsedTime / 1000} s")

        if (_engineSessionHandle == null) {
          val ex = op.getStatus.exception.getOrElse(
            KyuubiSQLException(s"Failed to launch engine for session[$handle]"))
          throw ex
        }

        engineLaunched = true
      }
    }
  }

  override def close(): Unit = {
    super.close()
    if (handle != null) {
      sessionManager.credentialsManager.removeSessionCredentialsEpoch(handle.identifier.toString)
    }
    try {
      if (_client != null) _client.closeSession()
    } catch {
      case e: TException =>
        throw KyuubiSQLException("Error while cleaning up the engine resources", e)
    } finally {
      sessionEvent.endTime = System.currentTimeMillis()
      EventLoggingService.onEvent(sessionEvent)
      MetricsSystem.tracing(_.decCount(MetricRegistry.name(CONN_OPEN, user)))
      if (transport != null && transport.isOpen) {
        transport.close()
      }
    }
  }
}
