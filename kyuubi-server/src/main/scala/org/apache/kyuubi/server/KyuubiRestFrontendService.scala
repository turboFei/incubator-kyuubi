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
import java.util.concurrent.{Future, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import javax.servlet.DispatcherType
import javax.ws.rs.{NotAllowedException, WebApplicationException}
import javax.ws.rs.core.Response.Status

import com.google.common.annotations.VisibleForTesting
import org.apache.hadoop.conf.Configuration
import org.eclipse.jetty.servlet.{ErrorPageErrorHandler, FilterHolder}

import org.apache.kyuubi.{KyuubiException, Utils}
import org.apache.kyuubi.config.{KyuubiConf, KyuubiEbayConf}
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.server.api.v1.ApiRootResource
import org.apache.kyuubi.server.http.authentication.{AuthenticationFilter, KyuubiHttpAuthenticationFactory}
import org.apache.kyuubi.server.ui.{JettyServer, JettyUtils}
import org.apache.kyuubi.service.{AbstractFrontendService, Serverable, Service, ServiceUtils}
import org.apache.kyuubi.service.authentication.{AuthMethods, AuthTypes, KyuubiAuthenticationFactory}
import org.apache.kyuubi.session.{KyuubiSession, KyuubiSessionManager, SessionHandle}
import org.apache.kyuubi.util.ThreadUtils

/**
 * A frontend service based on RESTful api via HTTP protocol.
 * Note: Currently, it only be used in the Kyuubi Server side.
 */
class KyuubiRestFrontendService(override val serverable: Serverable)
  extends AbstractFrontendService("KyuubiRestFrontendService") {

  private var server: JettyServer = _

  private val isStarted = new AtomicBoolean(false)

  private def hadoopConf(sessionConf: Map[String, String]): Configuration = {
    if (KyuubiServer.isClusterModeEnabled) {
      val clusterOpt = KyuubiEbayConf.getSessionCluster(be.sessionManager, sessionConf)
      KyuubiServer.getHadoopConf(clusterOpt)
    } else {
      KyuubiServer.getHadoopConf(None)
    }
  }

  private[kyuubi] def sessionManager = be.sessionManager.asInstanceOf[KyuubiSessionManager]

  private val batchChecker = ThreadUtils.newDaemonSingleThreadScheduledExecutor("batch-checker")

  lazy val host: String = conf.get(FRONTEND_REST_BIND_HOST)
    .getOrElse {
      if (Utils.isWindows || Utils.isMac) {
        warn(s"Kyuubi Server run in Windows or Mac environment, binding $getName to 0.0.0.0")
        "0.0.0.0"
      } else if (conf.get(KyuubiConf.FRONTEND_CONNECTION_URL_USE_HOSTNAME)) {
        Utils.findLocalInetAddress.getCanonicalHostName
      } else {
        Utils.findLocalInetAddress.getHostAddress
      }
    }

  private lazy val port: Int = conf.get(FRONTEND_REST_BIND_PORT)

  private lazy val securityEnabled = {
    val authTypes = conf.get(AUTHENTICATION_METHOD).map(AuthTypes.withName)
    KyuubiAuthenticationFactory.getValidPasswordAuthMethod(authTypes) != AuthMethods.NONE
  }

  private lazy val administrators: Set[String] =
    conf.get(KyuubiConf.SERVER_ADMINISTRATORS) + Utils.currentUser

  def isAdministrator(userName: String): Boolean =
    if (securityEnabled) administrators.contains(userName) else true

  override def initialize(conf: KyuubiConf): Unit = synchronized {
    this.conf = conf
    server = JettyServer(
      getName,
      host,
      port,
      conf.get(FRONTEND_REST_MAX_WORKER_THREADS))
    super.initialize(conf)
  }

  override def connectionUrl: String = {
    checkInitialized()
    conf.get(FRONTEND_ADVERTISED_HOST) match {
      case Some(advertisedHost) => s"$advertisedHost:$port"
      case None => server.getServerUri
    }
  }

  private def startInternal(): Unit = {
    val contextHandler = ApiRootResource.getServletHandler(this)
    val holder = new FilterHolder(new AuthenticationFilter(conf))
    contextHandler.addFilter(holder, "/v1/*", EnumSet.allOf(classOf[DispatcherType]))
    val authenticationFactory = new KyuubiHttpAuthenticationFactory(conf)
    server.addHandler(authenticationFactory.httpHandlerWrapperFactory.wrapHandler(contextHandler))

    val proxyHandler = ApiRootResource.getEngineUIProxyHandler(this)
    server.addHandler(authenticationFactory.httpHandlerWrapperFactory.wrapHandler(proxyHandler))

    installWebUI()
  }

  private def installWebUI(): Unit = {
    // redirect root path to Web UI home page
    server.addRedirectHandler("/", "/ui")

    val servletHandler = JettyUtils.createStaticHandler("dist", "/ui")
    // HTML5 Web History Mode requires redirect any url path under Web UI Servlet to the main page.
    // See more details at https://router.vuejs.org/guide/essentials/history-mode.html#html5-mode
    val errorHandler = new ErrorPageErrorHandler
    errorHandler.addErrorPage(404, "/")
    servletHandler.setErrorHandler(errorHandler)
    server.addHandler(servletHandler)
  }

  private def startBatchChecker(): Unit = {
    val interval = conf.get(KyuubiConf.BATCH_CHECK_INTERVAL)
    val task = new Runnable {
      override def run(): Unit = {
        try {
          sessionManager.getPeerInstanceClosedBatchSessions(connectionUrl).foreach { batch =>
            Utils.tryLogNonFatalError {
              val sessionHandle = SessionHandle.fromUUID(batch.identifier)
              sessionManager.getBatchSession(sessionHandle).foreach(_.close())
            }
          }
        } catch {
          case e: Throwable => error("Error checking batch sessions", e)
        }
      }
    }

    batchChecker.scheduleWithFixedDelay(task, interval, interval, TimeUnit.MILLISECONDS)
  }

  @VisibleForTesting
  private[kyuubi] def recoverBatchSessions(): Unit = {
    val recoveryNumThreads = conf.get(METADATA_RECOVERY_THREADS)
    val batchRecoveryExecutor =
      ThreadUtils.newDaemonFixedThreadPool(recoveryNumThreads, "batch-recovery-executor")
    try {
      val batchSessionsToRecover = sessionManager.getBatchSessionsToRecover(connectionUrl)
      val pendingRecoveryTasksCount = new AtomicInteger(0)
      val tasks = batchSessionsToRecover.flatMap { batchSession =>
        val batchId = batchSession.batchJobSubmissionOp.batchId
        try {
          val task: Future[Unit] = batchRecoveryExecutor.submit(() =>
            Utils.tryLogNonFatalError(sessionManager.openBatchSession(batchSession)))
          Some(task -> batchId)
        } catch {
          case e: Throwable =>
            error(s"Error while submitting batch[$batchId] for recovery", e)
            None
        }
      }

      pendingRecoveryTasksCount.addAndGet(tasks.size)

      tasks.foreach { case (task, batchId) =>
        try {
          task.get()
        } catch {
          case e: Throwable =>
            error(s"Error while recovering batch[$batchId]", e)
        } finally {
          val pendingTasks = pendingRecoveryTasksCount.decrementAndGet()
          info(s"Batch[$batchId] recovery task terminated, current pending tasks $pendingTasks")
        }
      }
    } finally {
      ThreadUtils.shutdown(batchRecoveryExecutor)
    }
  }

  override def start(): Unit = synchronized {
    if (!isStarted.get) {
      try {
        server.start()
        isStarted.set(true)
        startBatchChecker()
        startInternal()
        // block until the HTTP server is started, otherwise, we may get
        // the wrong HTTP server port -1
        while (server.getState != "STARTED") {
          info(s"Waiting for $getName's HTTP server getting started")
          Thread.sleep(1000)
        }
        recoverBatchSessions()
      } catch {
        case e: Exception => throw new KyuubiException(s"Cannot start $getName", e)
      }
    }
    super.start()
    info(s"Exposing REST endpoint at: http://${server.getServerUri}")
  }

  override def stop(): Unit = synchronized {
    ThreadUtils.shutdown(batchChecker)
    if (isStarted.getAndSet(false)) {
      server.stop()
    }
    super.stop()
  }

  def checkSessionPermission(userName: String, session: KyuubiSession): Unit = {
    if (userName != session.user && userName != session.realUser) {
      try {
        try {
          KyuubiAuthenticationFactory.verifyProxyAccess(
            userName,
            session.user,
            AuthenticationFilter.getUserIpAddress,
            hadoopConf(session.normalizedConf))
        } catch {
          case e: Throwable =>
            try {
              KyuubiAuthenticationFactory.verifyBatchAccountAccess(
                userName,
                session.user,
                conf)
            } catch {
              case be: Throwable =>
                error("Error fallback to verify batch account access", be)
                throw e
            }
        }
      } catch {
        case e: Throwable => throw new NotAllowedException(e.getMessage)
      }
    }
  }

  def getRealUser(): String = {
    ServiceUtils.getShortName(
      Option(AuthenticationFilter.getUserName).filter(_.nonEmpty).getOrElse("anonymous"))
  }

  def getSessionUser(proxyUser: String): String = {
    // Internally, we use kyuubi.session.proxy.user to unify the key as proxyUser
    val sessionConf = Option(proxyUser).filter(_.nonEmpty).map(proxyUser =>
      Map(PROXY_USER.key -> proxyUser)).getOrElse(Map())
    getSessionUser(sessionConf)
  }

  def getSessionUser(sessionConf: Map[String, String] = Map.empty): String = {
    // using the remote ip address instead of that in proxy http header for authentication
    val ipAddress = AuthenticationFilter.getUserIpAddress
    val realUser: String = getRealUser()
    try {
      getProxyUser(sessionConf, ipAddress, realUser)
    } catch {
      case t: Throwable => throw new WebApplicationException(
          t.getMessage,
          Status.METHOD_NOT_ALLOWED)
    }
  }

  def getIpAddress: String = {
    Option(AuthenticationFilter.getUserProxyHeaderIpAddress).getOrElse(
      AuthenticationFilter.getUserIpAddress)
  }

  private def getProxyUser(
      sessionConf: Map[String, String],
      ipAddress: String,
      realUser: String): String = {
    if (sessionConf == null) {
      realUser
    } else {
      val proxyUser = sessionConf.getOrElse(
        PROXY_USER.key,
        sessionConf.getOrElse(KyuubiAuthenticationFactory.HS2_PROXY_USER, realUser))
      if (!proxyUser.equals(realUser) && !isAdministrator(realUser)) {
        try {
          KyuubiAuthenticationFactory.verifyProxyAccess(
            realUser,
            proxyUser,
            ipAddress,
            hadoopConf(sessionConf))
        } catch {
          case e: Throwable =>
            try {
              KyuubiAuthenticationFactory.verifyBatchAccountAccess(realUser, proxyUser, conf)
            } catch {
              case be: Throwable =>
                error("Error fallback to verify batch account access", be)
                throw e
            }
        }
      }
      proxyUser
    }
  }

  override lazy val discoveryService: Option[Service] = None
}
