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

import java.util.EnumSet
import java.util.concurrent.atomic.AtomicBoolean
import javax.servlet.DispatcherType

import org.apache.hadoop.conf.Configuration
import org.eclipse.jetty.servlet.FilterHolder

import org.apache.kyuubi.{KyuubiException, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{FRONTEND_REST_BIND_HOST, FRONTEND_REST_BIND_PORT}
import org.apache.kyuubi.server.api.v1.ApiRootResource
import org.apache.kyuubi.server.http.authentication.{AuthenticationFilter, KyuubiHttpAuthenticationFactory}
import org.apache.kyuubi.server.ui.JettyServer
import org.apache.kyuubi.service.{AbstractFrontendService, Serverable, Service, ServiceUtils}
import org.apache.kyuubi.service.authentication.KyuubiAuthenticationFactory
import org.apache.kyuubi.util.KyuubiHadoopUtils

/**
 * A frontend service based on RESTful api via HTTP protocol.
 * Note: Currently, it only be used in the Kyuubi Server side.
 */
class KyuubiRestFrontendService(override val serverable: Serverable)
  extends AbstractFrontendService("KyuubiRestFrontendService") {

  private var server: JettyServer = _

  private val isStarted = new AtomicBoolean(false)

  private lazy val hadoopConf: Configuration = KyuubiHadoopUtils.newHadoopConf(conf)

  override def initialize(conf: KyuubiConf): Unit = synchronized {
    val host = conf.get(FRONTEND_REST_BIND_HOST)
      .getOrElse(Utils.findLocalInetAddress.getHostAddress)
    server = JettyServer(getName, host, conf.get(FRONTEND_REST_BIND_PORT))
    super.initialize(conf)
  }

  override def connectionUrl: String = {
    checkInitialized()
    server.getServerUri
  }

  private def startInternal(): Unit = {
    val contextHandler = ApiRootResource.getServletHandler(this)
    val holder = new FilterHolder(new AuthenticationFilter(conf))
    contextHandler.addFilter(holder, "/*", EnumSet.allOf(classOf[DispatcherType]))
    val authenticationFactory = new KyuubiHttpAuthenticationFactory(conf)
    server.addHandler(authenticationFactory.httpHandlerWrapperFactory.wrapHandler(contextHandler))

    server.addStaticHandler("org/apache/kyuubi/ui/static", "/static")
    server.addRedirectHandler("/", "/static")
    server.addStaticHandler("org/apache/kyuubi/ui/swagger", "/swagger")
    server.addRedirectHandler("/docs", "/swagger")
  }

  override def start(): Unit = synchronized {
    if (!isStarted.get) {
      try {
        server.start()
        isStarted.set(true)
        info(s"$getName has started at ${server.getServerUri}")
        startInternal()
      } catch {
        case e: Exception => throw new KyuubiException(s"Cannot start $getName", e)
      }
    }
    super.start()
  }

  override def stop(): Unit = synchronized {
    if (isStarted.getAndSet(false)) {
      server.stop()
    }
    super.stop()
  }

  def getUserName(sessionConf: Map[String, String]): String = {
    val realUser: String = ServiceUtils.getShortName(
      Option(AuthenticationFilter.getUserName).filter(_.nonEmpty).getOrElse("anonymous"))
    getProxyUser(sessionConf, Option(AuthenticationFilter.getUserIpAddress).orNull, realUser)
  }

  private def getProxyUser(
      sessionConf: Map[String, String],
      ipAddress: String,
      realUser: String): String = {
    if (sessionConf == null) {
      realUser
    } else {
      sessionConf.get(KyuubiAuthenticationFactory.HS2_PROXY_USER).map { proxyUser =>
        KyuubiAuthenticationFactory.verifyProxyAccess(realUser, proxyUser, ipAddress, hadoopConf)
        proxyUser
      }.getOrElse(realUser)
    }
  }

  override val discoveryService: Option[Service] = None
}
