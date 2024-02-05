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

package org.apache.kyuubi.ha.client

import java.util.concurrent.TimeUnit

import org.apache.kyuubi.config.KyuubiEbayConf
import org.apache.kyuubi.service.FrontendService

/**
 * A service for service discovery used by kyuubi server side.
 * We add another zk watch so that we can stop server more genteelly.
 *
 * @param fe the frontend service to publish for service discovery
 */
class KyuubiServiceDiscovery(
    fe: FrontendService) extends ServiceDiscovery("KyuubiServiceDiscovery", fe) {

  override def stop(): Unit = synchronized {
    if (!isServerLost.get()) {
      discoveryClient.deregisterService()
      discoveryClient.closeClient()
      gracefulShutdownLatch.await() // wait for graceful shutdown triggered by watcher
    } else {
      warn(s"The Zookeeper ensemble is LOST")
    }
    super.stop()
  }

  override def stopGracefully(isLost: Boolean): Unit = {
    val gracefulPeriod = conf.get(KyuubiEbayConf.SERVER_DEREGISTER_GRACEFUL_PERIOD)
    val startTime = System.currentTimeMillis()
    while (fe.be.sessionManager.getOpenSessionCount > 0 &&
      System.currentTimeMillis() - startTime < gracefulPeriod) {
      info(
        s"${fe.be.sessionManager.getOpenSessionCount} connection(s) are active, delay shutdown")
      Thread.sleep(TimeUnit.SECONDS.toMillis(5))
    }
    if (fe.be.sessionManager.getOpenSessionCount > 0) {
      warn(
        s"${fe.be.sessionManager.getOpenSessionCount} connection(s) are still active," +
          s" force shutdown")
      fe.be.sessionManager.allSessions().foreach { session =>
        warn(s"Closing session ${session.handle.identifier} forcibly" +
          s" after server deregister graceful period: ${gracefulPeriod}ms.")
        try {
          fe.be.sessionManager.closeSession(session.handle)
        } catch {
          case e: Throwable =>
            error(s"Error closing session ${session.handle.identifier}", e)
        }
      }
    }
    isServerLost.set(isLost)
    gracefulShutdownLatch.countDown()
    fe.serverable.stop()
  }
}
