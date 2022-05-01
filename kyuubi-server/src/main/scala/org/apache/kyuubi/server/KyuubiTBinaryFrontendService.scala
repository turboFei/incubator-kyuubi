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
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hive.service.rpc.thrift.{TOpenSessionReq, TOpenSessionResp, TRenewDelegationTokenReq, TRenewDelegationTokenResp}

import org.apache.kyuubi.{KyuubiSQLException, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{SESSION_CLUSTER, SESSION_CLUSTER_MODE_ENABLED}
import org.apache.kyuubi.ha.client.{KyuubiServiceDiscovery, ServiceDiscovery}
import org.apache.kyuubi.service.{Serverable, Service, TBinaryFrontendService}
import org.apache.kyuubi.service.TFrontendService.{CURRENT_SERVER_CONTEXT, OK_STATUS}
import org.apache.kyuubi.session.KyuubiSessionImpl
import org.apache.kyuubi.util.KyuubiHadoopUtils

final class KyuubiTBinaryFrontendService(
    override val serverable: Serverable)
  extends TBinaryFrontendService("KyuubiTBinaryFrontend") {

  override lazy val discoveryService: Option[Service] = {
    if (ServiceDiscovery.supportServiceDiscovery(conf)) {
      Some(new KyuubiServiceDiscovery(this))
    } else {
      None
    }
  }

  private val clusterHadoopConf = new ConcurrentHashMap[String, Configuration]().asScala

  override protected def initHadoopConf(conf: KyuubiConf): Unit = {
    if (conf.get(SESSION_CLUSTER_MODE_ENABLED)) {
      Utils.getDefinedPropertiesClusterList().foreach { cluster =>
        clusterHadoopConf.put(
          cluster,
          KyuubiHadoopUtils.newHadoopConf(
            conf,
            clusterOpt = Option(cluster)))
      }
    } else {
      super.initHadoopConf(conf)
    }
  }

  override protected def getHadoopConf(sessionConf: Map[String, String]): Configuration = {
    if (conf.get(SESSION_CLUSTER_MODE_ENABLED)) {
      val normalizedConf = be.sessionManager.validateAndNormalizeConf(sessionConf)
      val clusterOpt = normalizedConf.get(SESSION_CLUSTER.key).orElse(conf.get(SESSION_CLUSTER))

      // if the cluster hadoop conf is not loaded but in the cluster list, load it later
      val clusterInvalid = clusterOpt.isEmpty || (
        !clusterHadoopConf.contains(clusterOpt.get) ||
          !Utils.getDefinedPropertiesClusterList().contains(clusterOpt.get))

      if (clusterInvalid) {
        val clusterList = Utils.getDefinedPropertiesClusterList()
        throw KyuubiSQLException(
          s"Please specify the cluster to access with session conf[${SESSION_CLUSTER.key}]," +
            s" which should be one of ${clusterList.mkString("[", ",", "]")}," +
            s" current value is $clusterOpt")
      }
      clusterHadoopConf.getOrElseUpdate(
        clusterOpt.get,
        KyuubiHadoopUtils.newHadoopConf(conf, clusterOpt = clusterOpt))
    } else {
      super.getHadoopConf(sessionConf)
    }
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

      val opHandleIdentifier = launchEngineOp.getHandle.identifier.toTHandleIdentifier
      respConfiguration.put(
        "kyuubi.session.engine.launch.handle.guid",
        Base64.getMimeEncoder.encodeToString(opHandleIdentifier.getGuid))
      respConfiguration.put(
        "kyuubi.session.engine.launch.handle.secret",
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
