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

package org.apache.kyuubi.ha.client.zookeeper

import java.io.{File, IOException}
import java.net.InetAddress
import java.util
import java.util.concurrent.atomic.AtomicBoolean
import javax.security.auth.login.Configuration

import scala.collection.JavaConverters._

import org.apache.hadoop.util.StringUtils
import org.apache.zookeeper.ZooDefs
import org.apache.zookeeper.data.ACL
import org.scalatest.time.SpanSugar._

import org.apache.kyuubi.KerberizedTestHelper
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.ha.HighAvailabilityConf._
import org.apache.kyuubi.ha.client.AuthTypes
import org.apache.kyuubi.ha.client.DiscoveryClientTests
import org.apache.kyuubi.ha.client.EngineServiceDiscovery
import org.apache.kyuubi.service._
import org.apache.kyuubi.zookeeper.{EmbeddedZookeeper, ZookeeperConf}

class ZookeeperDiscoveryClientSuite extends DiscoveryClientTests with KerberizedTestHelper {

  val zkServer = new EmbeddedZookeeper()
  override val conf: KyuubiConf = KyuubiConf()

  override def getConnectString: String = zkServer.getConnectString

  override def beforeAll(): Unit = {
    conf.set(ZookeeperConf.ZK_CLIENT_PORT, 0)
    zkServer.initialize(conf)
    zkServer.start()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    conf.unset(KyuubiConf.SERVER_KEYTAB)
    conf.unset(KyuubiConf.SERVER_PRINCIPAL)
    conf.unset(HA_ADDRESSES)
    zkServer.stop()
    super.afterAll()
  }

  test("acl for zookeeper") {
    val expectedNoACL = new util.ArrayList[ACL](ZooDefs.Ids.OPEN_ACL_UNSAFE)
    val expectedEnableACL = new util.ArrayList[ACL](ZooDefs.Ids.READ_ACL_UNSAFE)
    expectedEnableACL.addAll(ZooDefs.Ids.CREATOR_ALL_ACL)

    def assertACL(expected: util.List[ACL], actual: util.List[ACL]): Unit = {
      assert(actual.size() == expected.size())
      assert(actual === expected)
    }

    val acl = new ZookeeperACLProvider(conf).getDefaultAcl
    assertACL(expectedNoACL, acl)

    val serverConf = conf.clone.set(HA_ZK_AUTH_TYPE, AuthTypes.KERBEROS.toString)
    val serverACL = new ZookeeperACLProvider(serverConf).getDefaultAcl
    assertACL(expectedEnableACL, serverACL)

    val engineConf = serverConf.clone.set(HA_ENGINE_REF_ID, "ref")
    engineConf.set(HA_ZK_ENGINE_AUTH_TYPE, AuthTypes.NONE.toString)
    val engineACL = new ZookeeperACLProvider(engineConf).getDefaultAcl
    assertACL(expectedNoACL, engineACL)

    val enableEngineACLConf = serverConf.clone.set(HA_ENGINE_REF_ID, "ref")
    enableEngineACLConf.set(HA_ZK_ENGINE_AUTH_TYPE, AuthTypes.KERBEROS.toString)
    val enableEngineACL = new ZookeeperACLProvider(enableEngineACLConf).getDefaultAcl
    assertACL(expectedEnableACL, enableEngineACL)
  }

  test("set up zookeeper auth") {
    tryWithSecurityEnabled {
      val keytab = File.createTempFile("kentyao", ".keytab")
      val principal = "kentyao/_HOST@apache.org"

      conf.set(HA_ZK_AUTH_KEYTAB.key, keytab.getCanonicalPath)
      conf.set(HA_ZK_AUTH_PRINCIPAL.key, principal)
      conf.set(HA_ZK_AUTH_TYPE.key, AuthTypes.KERBEROS.toString)

      ZookeeperClientProvider.setUpZooKeeperAuth(conf)
      val configuration = Configuration.getConfiguration
      val entries = configuration.getAppConfigurationEntry("KyuubiZooKeeperClient")

      assert(entries.head.getLoginModuleName === "com.sun.security.auth.module.Krb5LoginModule")
      val options = entries.head.getOptions.asScala.toMap

      val hostname = StringUtils.toLowerCase(InetAddress.getLocalHost.getCanonicalHostName)
      assert(options("principal") === s"kentyao/$hostname@apache.org")
      assert(options("useKeyTab").toString.toBoolean)

      conf.set(HA_ZK_AUTH_KEYTAB.key, s"${keytab.getName}")
      val e = intercept[IOException](ZookeeperClientProvider.setUpZooKeeperAuth(conf))
      assert(e.getMessage === s"${HA_ZK_AUTH_KEYTAB.key} does not exists")
    }
  }

  test("stop engine in time while zk ensemble terminates") {
    val zkServer = new EmbeddedZookeeper()
    val conf = KyuubiConf()
      .set(ZookeeperConf.ZK_CLIENT_PORT, 0)
    try {
      zkServer.initialize(conf)
      zkServer.start()
      var serviceDiscovery: EngineServiceDiscovery = null
      val server = new NoopTBinaryFrontendServer() {
        override val frontendServices: Seq[NoopTBinaryFrontendService] = Seq(
          new NoopTBinaryFrontendService(this) {
            override val discoveryService: Option[Service] = {
              serviceDiscovery = new EngineServiceDiscovery(this)
              Some(serviceDiscovery)
            }
          })
      }
      conf.set(HA_ZK_CONN_RETRY_POLICY, "ONE_TIME")
        .set(HA_ZK_CONN_BASE_RETRY_WAIT, 1)
        .set(HA_ADDRESSES, zkServer.getConnectString)
        .set(HA_ZK_SESSION_TIMEOUT, 2000)
        .set(KyuubiConf.FRONTEND_THRIFT_BINARY_BIND_PORT, 0)
      server.initialize(conf)
      server.start()
      assert(server.getServiceState === ServiceState.STARTED)

      zkServer.stop()
      val isServerLostM = serviceDiscovery.getClass.getSuperclass.getDeclaredField("isServerLost")
      isServerLostM.setAccessible(true)
      val isServerLost = isServerLostM.get(serviceDiscovery)

      eventually(timeout(10.seconds), interval(100.millis)) {
        assert(isServerLost.asInstanceOf[AtomicBoolean].get())
        assert(serviceDiscovery.getServiceState === ServiceState.STOPPED)
        assert(server.getServiceState === ServiceState.STOPPED)
      }
    } finally {
      zkServer.stop()
    }
  }
}
