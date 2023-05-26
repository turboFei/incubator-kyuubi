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

package org.apache.kyuubi.ebay

import org.apache.kyuubi.config.{KyuubiConf, KyuubiEbayConf}
import org.apache.kyuubi.ha.client.KyuubiServiceDiscovery
import org.apache.kyuubi.service.FrontendService

class KyuubiBinarySSLServiceDiscovery(fe: FrontendService) extends KyuubiServiceDiscovery(fe) {
  private var _binarySSLNamespace: String = _
  override def namespace: String = _binarySSLNamespace

  override def initialize(conf: KyuubiConf): Unit = {
    super.initialize(conf)
    _binarySSLNamespace = conf.get(KyuubiEbayConf.BINARY_SSL_HA_NAMESPACE)
  }
}
