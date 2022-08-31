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

import java.util.Base64

import org.apache.hadoop.conf.Configuration
import org.apache.hive.service.rpc.thrift._
import org.apache.thrift.protocol.TProtocol
import org.apache.thrift.server.ServerContext

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.cli.Handle
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiEbayConf.SESSION_CLUSTER
import org.apache.kyuubi.config.KyuubiReservedKeys._
import org.apache.kyuubi.ha.client.KyuubiServiceDiscovery
import org.apache.kyuubi.metrics.MetricsConstants._
import org.apache.kyuubi.metrics.MetricsSystem
import org.apache.kyuubi.service.{Serverable, Service, TBinaryFrontendService}
import org.apache.kyuubi.service.TFrontendService.{CURRENT_SERVER_CONTEXT, FeServiceServerContext, OK_STATUS}
import org.apache.kyuubi.session.KyuubiSessionImpl

final class KyuubiTBinaryFrontendService(
    override val serverable: Serverable)
  extends TBinaryFrontendService("KyuubiTBinaryFrontend") {

  override lazy val discoveryService: Option[Service] = {
    if (KyuubiServiceDiscovery.supportServiceDiscovery(conf)) {
      Some(new KyuubiServiceDiscovery(this))
    } else {
      None
    }
  }

  override protected def hadoopConf(sessionConf: Map[String, String]): Configuration = {
    if (KyuubiServer.isClusterModeEnabled) {
      val normalizedConf = be.sessionManager.validateAndNormalizeConf(sessionConf)
      val clusterOpt = normalizedConf.get(SESSION_CLUSTER.key).orElse(conf.get(SESSION_CLUSTER))
      KyuubiServer.getHadoopConf(clusterOpt)
    } else {
      KyuubiServer.getHadoopConf(None)
    }
  }

  override def initialize(conf: KyuubiConf): Unit = synchronized {
    super.initialize(conf)

    server.foreach(_.setServerEventHandler(new FeTServerEventHandler() {
      override def createContext(input: TProtocol, output: TProtocol): ServerContext = {
        MetricsSystem.tracing { ms =>
          ms.incCount(THRIFT_BINARY_CONN_OPEN)
          ms.incCount(THRIFT_BINARY_CONN_TOTAL)
        }
        new FeServiceServerContext()
      }

      override def deleteContext(
          serverContext: ServerContext,
          input: TProtocol,
          output: TProtocol): Unit = {
        super.deleteContext(serverContext, input, output)
        MetricsSystem.tracing { ms =>
          ms.decCount(THRIFT_BINARY_CONN_OPEN)
        }
      }
    }))
  }

  override def OpenSession(req: TOpenSessionReq): TOpenSessionResp = {
    debug(req.toString)
    info("Client protocol version: " + req.getClient_protocol)
    val resp = new TOpenSessionResp
    try {
      val sessionHandle = getSessionHandle(req, resp)

      val respConfiguration = new java.util.HashMap[String, String]()
      val launchEngineOp = be.sessionManager.getSession(sessionHandle)
        .asInstanceOf[KyuubiSessionImpl].launchEngineOp

      val opHandleIdentifier = Handle.toTHandleIdentifier(launchEngineOp.getHandle.identifier)
      respConfiguration.put(
        KYUUBI_SESSION_ENGINE_LAUNCH_HANDLE_GUID,
        Base64.getMimeEncoder.encodeToString(opHandleIdentifier.getGuid))
      respConfiguration.put(
        KYUUBI_SESSION_ENGINE_LAUNCH_HANDLE_SECRET,
        Base64.getMimeEncoder.encodeToString(opHandleIdentifier.getSecret))

      resp.setSessionHandle(sessionHandle.toTSessionHandle)
      resp.setConfiguration(respConfiguration)
      resp.setStatus(OK_STATUS)
      Option(CURRENT_SERVER_CONTEXT.get()).foreach(_.setSessionHandle(sessionHandle))
    } catch {
      case e: Exception =>
        error("Error opening session: ", e)
        resp.setStatus(KyuubiSQLException.toTStatus(e, verbose = true))
    }
    resp
  }

  override protected def isServer(): Boolean = true

  override def RenewDelegationToken(req: TRenewDelegationTokenReq): TRenewDelegationTokenResp = {
    debug(req.toString)
    val resp = new TRenewDelegationTokenResp
    resp.setStatus(notSupportTokenErrorStatus)
    resp
  }
}
