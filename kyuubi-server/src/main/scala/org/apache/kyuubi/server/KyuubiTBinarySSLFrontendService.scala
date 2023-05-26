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

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.FRONTEND_SSL_KEYSTORE_TYPE
import org.apache.kyuubi.ebay.SSLUtils
import org.apache.kyuubi.metrics.MetricsSystem
import org.apache.kyuubi.service.{Serverable, Service}

class KyuubiTBinarySSLFrontendService(
    override val serverable: Serverable) extends KyuubiTBinaryFrontendService(serverable) {
  override protected lazy val portNum: Int =
    conf.get(KyuubiConf.FRONTEND_THRIFT_BINARY_SSL_BIND_PORT)
  override protected def sslEnabled: Boolean = true

  override lazy val discoveryService: Option[Service] = None

  private def traceKeyStoreExpiration(): Unit = {
    val keyStorePath = conf.get(KyuubiConf.FRONTEND_SSL_KEYSTORE_PATH)
    val keyStorePassword = conf.get(KyuubiConf.FRONTEND_SSL_KEYSTORE_PASSWORD)
    val keyStoreType = conf.get(FRONTEND_SSL_KEYSTORE_TYPE)
    keyStorePath.zip(keyStorePassword) match {
      case Seq((path, password)) =>
        SSLUtils.getKeyStoreExpirationTime(path, password, keyStoreType).foreach { expiration =>
          def getExpirationInMs(): Long = {
            expiration - System.currentTimeMillis()
          }
          MetricsSystem.tracing { ms =>
            ms.registerGauge("kyuubi.keystore_expiration", getExpirationInMs(), 0L)
          }
        }
      case _ =>
    }
  }

  override def start(): Unit = {
    super.start()
    traceKeyStoreExpiration()
  }
}
