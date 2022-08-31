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

import java.security.Security

import org.apache.thrift.transport.{TSaslServerTransport, TSocket}

import org.apache.kyuubi.{KYUUBI_VERSION, KyuubiFunSuite}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.SemanticVersion
import org.apache.kyuubi.service.{NoopTBinaryFrontendServer, TBinaryFrontendService}
import org.apache.kyuubi.service.authentication.PlainSASLServer.SaslPlainProvider

class PlainSASLHelperSuite extends KyuubiFunSuite {

  test("PlainSASLHelper") {
    val server = new NoopTBinaryFrontendServer()
    val conf = KyuubiConf().set(KyuubiConf.FRONTEND_THRIFT_BINARY_BIND_PORT, 0)
    server.initialize(conf)
    val service = server.frontendServices.head.asInstanceOf[TBinaryFrontendService]
    val tProcessorFactory = PlainSASLHelper.getProcessFactory(service)
    val tSocket = new TSocket("0.0.0.0", 0)

    val tProcessor = tProcessorFactory.getProcessor(tSocket)
    assert(tProcessor.isInstanceOf[TSetIpAddressProcessor[_]])
    val e = intercept[IllegalArgumentException] {
      PlainSASLHelper.getTransportFactory("KERBEROS", conf)
    }
    assert(e.getMessage === "Illegal authentication type KERBEROS for plain transport")
    val e2 = intercept[IllegalArgumentException] {
      PlainSASLHelper.getTransportFactory("NOSASL", conf)
    }
    assert(e2.getMessage === "Illegal authentication type NOSASL for plain transport")

    val e3 = intercept[IllegalArgumentException] {
      PlainSASLHelper.getTransportFactory("ELSE", conf)
    }
    assert(e3.getMessage === "Illegal authentication type ELSE for plain transport")

    val tTransportFactory = PlainSASLHelper.getTransportFactory("NONE", conf)
    assert(tTransportFactory.isInstanceOf[TSaslServerTransport.Factory])
    Security.getProviders.exists(_.isInstanceOf[SaslPlainProvider])
  }

  test("Sasl Plain Provider") {
    val saslPlainProvider = new SaslPlainProvider()
    assert(saslPlainProvider.containsKey("SaslServerFactory.PLAIN"))
    assert(saslPlainProvider.getName === "KyuubiSaslPlain")
    val version: Double = {
      val ver = SemanticVersion(KYUUBI_VERSION)
      ver.majorVersion + ver.minorVersion.toDouble / 10
    }
    assert(saslPlainProvider.getVersion === version)
  }
}
