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

package org.apache.kyuubi.carmel.gateway.session

import org.apache.hive.service.rpc.thrift.{TGetInfoType, TGetInfoValue, TProtocolVersion}

import org.apache.kyuubi.carmel.gateway.endpoint.SparkEndpoint
import org.apache.kyuubi.carmel.gateway.session.CarmelSessionStatus.CarmelSessionStatus
import org.apache.kyuubi.config.{KyuubiConf, KyuubiEbayConf}
import org.apache.kyuubi.config.KyuubiConf.{ENGINE_OPEN_MAX_ATTEMPTS, ENGINE_OPEN_RETRY_WAIT}
import org.apache.kyuubi.ebay.carmel.gateway.endpoint.UserInfo
import org.apache.kyuubi.events.EventBus
import org.apache.kyuubi.operation.FetchOrientation
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.session.{KyuubiSessionImpl, KyuubiSessionManager, QUEUE}
import org.apache.kyuubi.sql.parser.server.KyuubiParser

class CarmelSessionImpl(
    protocol: TProtocolVersion,
    user: String,
    password: String,
    ipAddress: String,
    conf: Map[String, String],
    sessionManager: KyuubiSessionManager,
    sessionConf: KyuubiConf,
    parser: KyuubiParser)
  extends KyuubiSessionImpl(
    protocol,
    user,
    password,
    ipAddress,
    conf,
    sessionManager,
    sessionConf,
    parser) {
  @volatile private var backendSessionStatus = CarmelSessionStatus.ACTIVE
  override def checkSessionAccessPathURIs(): Unit = {}
  override val sessionIdleTimeoutThreshold: Long =
    sessionManager.getConf.get(KyuubiEbayConf.CARMEL_SESSION_IDLE_TIME)

  private var _sparkEndpoint: SparkEndpoint = _
  def sparkEndpoint: SparkEndpoint = _sparkEndpoint

  override protected[kyuubi] def openEngineSession(extraEngineLog: Option[OperationLog]): Unit = {
    val userInfo = new UserInfo(user, Option(password).getOrElse("anonymous"))
    normalizedConf.get(QUEUE).foreach(userInfo.setAssignedQueue)
    val endpointMgr = sessionManager.carmelEndpointManager.getClusterEndpointManager(sessionCluster)
    val maxAttempts = sessionManager.getConf.get(ENGINE_OPEN_MAX_ATTEMPTS)
    val retryWait = sessionManager.getConf.get(ENGINE_OPEN_RETRY_WAIT)

    var attempt = 0
    var shouldRetry = true

    while (attempt <= maxAttempts && shouldRetry) {
      try {
        _sparkEndpoint = endpointMgr.createEndpoint(userInfo, attempt)
        _client = _sparkEndpoint.getClient
        _engineSessionHandle = _client.openSession(protocol, user, password, normalizedConf)
        // get engine id, name, url for carmel engine
        _client._engineId = getEngineConfig("spark.app.id")
        _client._engineName = getEngineConfig("spark.app.name")
        _client._engineUrl =
          getEngineConfig(sessionManager.getConf.get(KyuubiEbayConf.CARMEL_ENGINE_URL_KEY))
        _client._engineId.foreach(_sparkEndpoint.setId)
        logSessionInfo(s"Connected to $sparkEndpoint with ${_engineSessionHandle}]")
        endpointMgr.markGoodServer(_sparkEndpoint.getServerUrl)
        shouldRetry = false
      } catch {
        case e: Throwable if attempt < maxAttempts =>
          if (_sparkEndpoint != null && _sparkEndpoint.getServerUrl != null) {
            endpointMgr.markFailServer(_sparkEndpoint.getServerUrl);
            _sparkEndpoint.close();
          }
          warn(
            s"Failed to open after $attempt/$maxAttempts times: ${e.getMessage}, retrying",
            e.getCause)
          Thread.sleep(retryWait)
          shouldRetry = true
      } finally {
        attempt += 1
      }
    }
    sessionEvent.openedTime = System.currentTimeMillis()
    sessionEvent.remoteSessionId = _engineSessionHandle.identifier.toString
    sessionEvent.engineId = _sparkEndpoint.toString
    EventBus.post(sessionEvent)
  }

  private def getEngineConfig(config: String): Option[String] = {
    var configValue: Option[String] = None
    try {
      val opHandle = client.executeStatement(s"set $config", Map.empty, false, 0)
      val result = client.fetchResults(opHandle, FetchOrientation.FETCH_NEXT, Int.MaxValue, false)
      configValue = Option(result.getColumns.get(1).getStringVal.getValues.get(0))
    } catch {
      case e: Throwable => error(s"Error fetching engine config: $config", e)
    }
    configValue
  }

  override def getInfo(infoType: TGetInfoType): TGetInfoValue = {
    waitForEngineLaunched()
    infoType match {
      // carmel hive service rpc does not support this type
      case TGetInfoType.CLI_ODBC_KEYWORDS => TGetInfoValue.stringValue("Unimplemented")
      case _ => client.getInfo(infoType).getInfoValue
    }
  }

  def getBackendSessionStatus: CarmelSessionStatus = backendSessionStatus
  def setBackendSessionStatus(sessionStatus: CarmelSessionStatus): Unit = {
    backendSessionStatus = sessionStatus
  }
}
