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

package org.apache.kyuubi.service.authentication

import java.nio.charset.StandardCharsets

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.ha.HighAvailabilityConf.HA_ZK_ENGINE_SECRET_NODE
import org.apache.kyuubi.ha.client.ZooKeeperClientProvider

class ZooKeeperSecureAccessProviderImpl extends SecureAccessProvider {
  import ZooKeeperClientProvider._
  import ZooKeeperSecureAccessProviderImpl._

  private var conf: KyuubiConf = _

  override def initialize(conf: KyuubiConf): Unit = {
    this.conf = conf
  }

  override def supportSecureAccess: Boolean = {
    conf.get(HA_ZK_ENGINE_SECRET_NODE).nonEmpty
  }

  override def getSecretAndCipher(): (String, String) = {
    var secretInfo: String = ""
    conf.get(HA_ZK_ENGINE_SECRET_NODE).foreach { zkNode =>
      withZkClient(conf) { zkClient =>
        secretInfo = new String(zkClient.getData.forPath(zkNode), StandardCharsets.UTF_8)
      }
    }
    parseSecretCipher(secretInfo)
  }
}

object ZooKeeperSecureAccessProviderImpl {
  private def parseSecretCipher(secretInfo: String): (String, String) = {
    val maybeInfos = secretInfo.split("\\s+")
    if (maybeInfos.size != 2) {
      throw new IllegalArgumentException(
        "The secret info should be formatted like <secret> <cipher>")
    }
    maybeInfos.head -> maybeInfos.last
  }
}
