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

package org.apache.kyuubi.ebay.carmel.gateway.session

import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import scala.collection.JavaConverters._

import org.apache.kyuubi.{KyuubiException, Logging}
import org.apache.kyuubi.config.{KyuubiConf, KyuubiEbayConf}
import org.apache.kyuubi.ebay.carmel.gateway.config.CarmelConfig
import org.apache.kyuubi.ebay.carmel.gateway.endpoint.EndpointManager
import org.apache.kyuubi.service.AbstractService
import org.apache.kyuubi.session.SessionManager
import org.apache.kyuubi.util.ThreadUtils

class CarmelEndpointManager(sessionMgr: SessionManager)
  extends AbstractService("CarmelEndpointManager") with Logging {
  private val clusterEndpointManager =
    new ConcurrentHashMap[Option[String], EndpointManager]().asScala
  private val carmelSessionChecker =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("carmel-session-checker")

  override def initialize(conf: KyuubiConf): Unit = {
    KyuubiEbayConf.getCarmelClusterOptList(conf).foreach { clusterOpt =>
      val carmelConfig =
        clusterOpt.map(CarmelConfig.getClusterInstance).getOrElse(CarmelConfig.getInstance)
      clusterEndpointManager.put(clusterOpt, new EndpointManager(carmelConfig))
    }
    super.initialize(conf)
  }

  override def start(): Unit = {
    startCarmelSessionChecker()
    super.start()
  }

  override def stop(): Unit = {
    ThreadUtils.shutdown(carmelSessionChecker)
    super.stop()
  }

  def getClusterEndpointManager(clusterOpt: Option[String]): EndpointManager = {
    clusterEndpointManager.get(clusterOpt).getOrElse(
      throw new KyuubiException(s"No carmel endpoint manager registered for cluster $clusterOpt"))
  }

  private def startCarmelSessionChecker(): Unit = {
    carmelSessionChecker.scheduleWithFixedDelay(
      new CarmelSessionStateChecker(sessionMgr),
      60,
      60,
      TimeUnit.SECONDS)
  }
}
