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

import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.conf.Configuration
import org.apache.hive.service.rpc.thrift._

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.cli.Handle
import org.apache.kyuubi.client.api.v1.dto.BatchRequest
import org.apache.kyuubi.config.KyuubiConf.SESSION_CLUSTER
import org.apache.kyuubi.config.KyuubiReservedKeys._
import org.apache.kyuubi.ha.client.KyuubiServiceDiscovery
import org.apache.kyuubi.service.{Serverable, Service, TBinaryFrontendService}
import org.apache.kyuubi.service.TFrontendService.{CURRENT_SERVER_CONTEXT, OK_STATUS, SERVER_VERSION}
import org.apache.kyuubi.session.{KyuubiBatchSessionImpl, KyuubiSessionImpl, KyuubiSessionManager, SessionHandle}

final class KyuubiTBinaryFrontendService(
    override val serverable: Serverable)
  extends TBinaryFrontendService("KyuubiTBinaryFrontend") {

  private val mapper = new ObjectMapper().registerModule(DefaultScalaModule)

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

  @throws[KyuubiSQLException]
  override protected def getSessionHandle(
      req: TOpenSessionReq,
      res: TOpenSessionResp): SessionHandle = {
    val protocol = getMinVersion(SERVER_VERSION, req.getClient_protocol)
    res.setServerProtocolVersion(protocol)
    val ipAddress = authFactory.getIpAddress.orNull
    val configuration =
      Option(req.getConfiguration).map(_.asScala.toMap).getOrElse(Map.empty[String, String])
    configuration.get("kyuubi.batch.request").map { requestBody =>
      val batchRequest = mapper.readValue(requestBody, classOf[BatchRequest])
      if (batchRequest.getConf != null) {
        Option(req.getConfiguration.putAll(batchRequest.getConf))
      }
      val userName = getUserName(req)
      be.sessionManager.asInstanceOf[KyuubiSessionManager].openBatchSession(
        userName,
        req.getPassword,
        ipAddress,
        Option(batchRequest.getConf.asScala.toMap).getOrElse(Map()),
        batchRequest)
    }.getOrElse {
      val userName = getUserName(req)
      be.openSession(protocol, userName, req.getPassword, ipAddress, configuration)
    }
  }

  override def OpenSession(req: TOpenSessionReq): TOpenSessionResp = {
    debug(req.toString)
    info("Client protocol version: " + req.getClient_protocol)
    val resp = new TOpenSessionResp
    try {
      val sessionHandle = getSessionHandle(req, resp)
      val respConfiguration = new java.util.HashMap[String, String]()

      be.sessionManager.getSession(sessionHandle) match {
        case ks: KyuubiSessionImpl =>
          val launchEngineOp = ks.launchEngineOp
          val opHandleIdentifier = Handle.toTHandleIdentifier(launchEngineOp.getHandle.identifier)
          respConfiguration.put(
            KYUUBI_SESSION_ENGINE_LAUNCH_HANDLE_GUID,
            Base64.getMimeEncoder.encodeToString(opHandleIdentifier.getGuid))
          respConfiguration.put(
            KYUUBI_SESSION_ENGINE_LAUNCH_HANDLE_SECRET,
            Base64.getMimeEncoder.encodeToString(opHandleIdentifier.getSecret))

        case kbs: KyuubiBatchSessionImpl =>
          val batchOp = kbs.batchJobSubmissionOp
          val opHandleIdentifier = Handle.toTHandleIdentifier(batchOp.getHandle.identifier)
          respConfiguration.put(
            "kyuubi.batch.handle.guid",
            Base64.getMimeEncoder.encodeToString(opHandleIdentifier.getGuid))
          respConfiguration.put(
            "kyuubi.batch.handle.secret",
            Base64.getMimeEncoder.encodeToString(opHandleIdentifier.getSecret))
      }

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
