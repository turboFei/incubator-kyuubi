/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package yaooqinn.kyuubi.yarn

import java.nio.ByteBuffer

import org.apache.curator.test.TestingServer
import org.apache.hadoop.io.Text
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.security.token.Token
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.AllocateResponsePBImpl
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus
import org.apache.hadoop.yarn.api.records.impl.pb.TokenPBImpl
import org.apache.hadoop.yarn.client.api.AMRMClient
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.exceptions.ApplicationAttemptNotFoundException
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier
import org.apache.spark.{KyuubiConf, KyuubiSparkUtil, SparkConf, SparkFunSuite}
import org.apache.spark.KyuubiConf._
import org.mockito.Mockito.{doNothing, when}
import org.scalatest.Matchers
import org.scalatest.mock.MockitoSugar

import yaooqinn.kyuubi.ha.HighAvailableService
import yaooqinn.kyuubi.server.KyuubiServer
import yaooqinn.kyuubi.service.State
import yaooqinn.kyuubi.utils.ReflectUtils


class KyuubiAppMasterSuite extends SparkFunSuite with MockitoSugar with Matchers {

  private val propertyFile = this.getClass.getClassLoader.getResource("kyuubi-test.conf").getPath
  private val args = Array("--properties-file", propertyFile)

  private val appMasterArguments = AppMasterArguments(args)
  private val conf = new SparkConf().set(KyuubiConf.FRONTEND_BIND_PORT.key, "0")
      .set("spark.hadoop.yarn.am.liveness-monitor.expiry-interval-ms", "0")

  override def beforeAll(): Unit = {
    appMasterArguments.propertiesFile.map(KyuubiSparkUtil.getPropertiesFromFile) match {
      case Some(props) => props.foreach { case (k, v) =>
        conf.set(k, v)
        sys.props(k) = v
      }
      case _ =>
    }
    KyuubiSparkUtil.setupCommonConfig(conf)
    super.beforeAll()
  }

  test("new application master") {
    val appMaster = new KyuubiAppMaster()
    assert(appMaster.getServiceState === State.NOT_INITED)
    assert(appMaster.getName === classOf[KyuubiAppMaster].getSimpleName)
    assert(appMaster.getStartTime === 0)
    assert(appMaster.getConf === null)
  }

  test("init application master") {
    val appMaster = new KyuubiAppMaster()
    appMaster.init(conf)
    appMaster.getServiceState should be(State.INITED)
    appMaster.getStartTime should be(0)
    appMaster.getConf should be(conf)
    appMaster.getConf.get("spark.kyuubi.test") should be("1")
  }

  test("start application master") {
    val appMaster = new KyuubiAppMaster()
    appMaster.init(conf)
    val amClient = mock[AMRMClient[AMRMClient.ContainerRequest]]
    ReflectUtils.setFieldValue(appMaster,
      "yaooqinn$kyuubi$yarn$KyuubiAppMaster$$amRMClient", amClient)
    doNothing().when(amClient).start()
    appMaster.start()
    appMaster.getServiceState should be(State.STARTED)
    appMaster.getStartTime should not be 0
    appMaster.getConf should be(conf)
    appMaster.stop()
    appMaster.getServices.foreach( _.getServiceState should be(State.STOPPED))
  }

  test("start application master in ha mode") {
    val zkServer = new TestingServer(2181, true)

    val connectString = zkServer.getConnectString
    val conf1 = conf.clone()
    conf1.set(KyuubiConf.HA_ENABLED.key, "true")
    conf1.set(HA_ZOOKEEPER_QUORUM.key, connectString)
    conf1.set(HA_ZOOKEEPER_CONNECTION_BASESLEEPTIME.key, "100ms")
    conf1.set(HA_ZOOKEEPER_SESSION_TIMEOUT.key, "15s")
    conf1.set(HA_ZOOKEEPER_CONNECTION_MAX_RETRIES.key, "1")

    val appMaster = new KyuubiAppMaster()
    appMaster.init(conf1)
    val amClient = mock[AMRMClient[AMRMClient.ContainerRequest]]
    ReflectUtils.setFieldValue(appMaster,
      "yaooqinn$kyuubi$yarn$KyuubiAppMaster$$amRMClient", amClient)
    doNothing().when(amClient).start()
    appMaster.start()
    appMaster.getServiceState should be(State.STARTED)
    appMaster.getStartTime should not be 0
    appMaster.getConf should be(conf1)
    appMaster.stop()
    appMaster.getServices.foreach( _.getServiceState should be(State.STOPPED))
    zkServer.stop()
  }

  test("heartbeat throws InterruptedException") {
    val appMaster = new KyuubiAppMaster()
    appMaster.init(conf)
    val amClient = mock[AMRMClient[AMRMClient.ContainerRequest]]
    ReflectUtils.setFieldValue(appMaster,
      "yaooqinn$kyuubi$yarn$KyuubiAppMaster$$amRMClient", amClient)
    doNothing().when(amClient).start()
    when(amClient.allocate(0.1f)).thenThrow(classOf[InterruptedException])
    appMaster.start()
    val failureCount = "yaooqinn$kyuubi$yarn$KyuubiAppMaster$$failureCount"
    ReflectUtils.getFieldValue(appMaster, failureCount) should be(0)
    appMaster.getServiceState should be(State.STARTED)
    appMaster.getStartTime should not be 0
    appMaster.getConf should be(conf)
  }

  test("heartbeat throws ApplicationAttemptNotFoundException") {
    val appMaster = new KyuubiAppMaster()
    appMaster.init(conf)
    val amClient = mock[AMRMClient[AMRMClient.ContainerRequest]]
    ReflectUtils.setFieldValue(appMaster,
      "yaooqinn$kyuubi$yarn$KyuubiAppMaster$$amRMClient", amClient)
    doNothing().when(amClient).start()
    when(amClient.allocate(0.1f)).thenThrow(classOf[ApplicationAttemptNotFoundException])
    appMaster.start()
    Thread.sleep(150)
    appMaster.getServiceState should be(State.STARTED)
    appMaster.getStartTime should not be 0
    appMaster.getConf should be(conf)
    ReflectUtils.getFieldValue(appMaster, "yaooqinn$kyuubi$yarn$KyuubiAppMaster$$amStatus")should
      be(FinalApplicationStatus.FAILED)
    val failureCount = "yaooqinn$kyuubi$yarn$KyuubiAppMaster$$failureCount"
    ReflectUtils.getFieldValue(appMaster, failureCount) should be(1)
  }

  test("heartbeat throws other exceptions") {
    val appMaster = new KyuubiAppMaster()
    appMaster.init(conf)
    val amClient = mock[AMRMClient[AMRMClient.ContainerRequest]]
    ReflectUtils.setFieldValue(appMaster,
      "yaooqinn$kyuubi$yarn$KyuubiAppMaster$$amRMClient", amClient)
    doNothing().when(amClient).start()
    when(amClient.allocate(0.1f)).thenThrow(classOf[Throwable])
    appMaster.start()
    Thread.sleep(500) // enough time to scheduling heartbeat task
    appMaster.getServiceState should be(State.STARTED)
    appMaster.getStartTime should not be 0
    appMaster.getConf should be(conf)
    ReflectUtils.getFieldValue(appMaster, "yaooqinn$kyuubi$yarn$KyuubiAppMaster$$amStatus") should
      be(FinalApplicationStatus.FAILED)
    val failureCount = "yaooqinn$kyuubi$yarn$KyuubiAppMaster$$failureCount"
    ReflectUtils.getFieldValue(appMaster, failureCount) should be(
      YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS + 2)
  }

  test("heartbeat throws fatal") {
    val appMaster = new KyuubiAppMaster()
    appMaster.init(conf)
    val amClient = mock[AMRMClient[AMRMClient.ContainerRequest]]
    ReflectUtils.setFieldValue(appMaster,
      "yaooqinn$kyuubi$yarn$KyuubiAppMaster$$amRMClient", amClient)
    doNothing().when(amClient).start()
    when(amClient.allocate(0.1f)).thenThrow(classOf[OutOfMemoryError])
    appMaster.start()
    Thread.sleep(500)
    appMaster.getServiceState should be(State.STARTED)
    appMaster.getStartTime should not be 0
    appMaster.getConf should be(conf)
    ReflectUtils.getFieldValue(appMaster, "yaooqinn$kyuubi$yarn$KyuubiAppMaster$$amStatus") should
      be(FinalApplicationStatus.FAILED)
    val failureCount = "yaooqinn$kyuubi$yarn$KyuubiAppMaster$$failureCount"
    ReflectUtils.getFieldValue(appMaster, failureCount) should be(1)
  }

  test("start application master without initialization") {
    val appMaster = new KyuubiAppMaster()
    // TODO:(Kent Yao) should throw illegal state exception
    // for contain_id is null in ut
    intercept[NullPointerException](appMaster.start())
  }

  test("heartbeat with amRmToken") {
    val ugi = UserGroupInformation.getCurrentUser
    val token = new Token[AMRMTokenIdentifier]("Identifier".getBytes(),
      "passwd".getBytes(), new Text("YARN_AM_RM_TOKEN"), new Text("service"))
    ugi.addToken(token)
    val appMaster = new KyuubiAppMaster()
    appMaster.init(conf)
    val allocateResp = new AllocateResponsePBImpl()
    val amRmToken = new TokenPBImpl()
    amRmToken.setIdentifier(ByteBuffer.wrap("Identifier".getBytes()) )
    amRmToken.setPassword(ByteBuffer.wrap("passwd".getBytes()))
    amRmToken.setKind("AMRMTOKEN")
    amRmToken.setService("master")
    allocateResp.setAMRMToken(amRmToken)
    val amClient = mock[AMRMClient[AMRMClient.ContainerRequest]]
    ReflectUtils.setFieldValue(appMaster,
      "yaooqinn$kyuubi$yarn$KyuubiAppMaster$$amRMClient", amClient)
    doNothing().when(amClient).start()
    when(amClient.allocate(0.1f)).thenReturn(allocateResp)
    appMaster.start()
    Thread.sleep(500)
    appMaster.getServiceState should be(State.STARTED)
    appMaster.getStartTime should not be 0
    appMaster.getConf should be(conf)
    ReflectUtils.getFieldValue(appMaster, "yaooqinn$kyuubi$yarn$KyuubiAppMaster$$amStatus") should
      be(FinalApplicationStatus.SUCCEEDED)
    val failureCount = "yaooqinn$kyuubi$yarn$KyuubiAppMaster$$failureCount"
    ReflectUtils.getFieldValue(appMaster, failureCount) should be(0)
  }

  test("offline ha service first") {
    val zkServer = new TestingServer()
    conf.set(KyuubiConf.HA_ZOOKEEPER_QUORUM.key, zkServer.getConnectString)
    conf.set(KyuubiConf.HA_MODE.key, "failover")
    val server = new KyuubiServer()
    val haService = new HighAvailableService("test", server) {
      override protected def reset(): Unit = {}
    }
    try {
      haService.init(conf)
      haService.start()
      ReflectUtils.setFieldValue(server, "_haService", haService)
      assert(haService.getServiceState === State.STARTED)
      server.offlineHaServiceFirst()
      assert(haService.getServiceState === State.STOPPED)
      ReflectUtils.setFieldValue(server, "_haService", null)
      server.offlineHaServiceFirst()
    } finally {
      zkServer.stop()
      server.stop()
    }
  }

  test("test get principal") {
    val appMaster = new KyuubiAppMaster()
    val principal = "username/hostName@realm"
    assert(appMaster.getPrincipal(principal) === "username/_HOST@realm")
    val invalidPrincipal = "username@realm"
    intercept[Exception](appMaster.getPrincipal(invalidPrincipal))
  }

  test("stop the kyuubiAppMaster with states and msg") {
    KyuubiYarnClientSuite.withTempDir { dir =>
      KyuubiYarnClientSuite.setEnv("KYUUBI_YARN_STAGING_DIR", Some(dir.getAbsolutePath))
      val ugi = UserGroupInformation.getCurrentUser
      val token = new Token[AMRMTokenIdentifier]("Identifier".getBytes(),
        "passwd".getBytes(), new Text("YARN_AM_RM_TOKEN"), new Text("service"))
      ugi.addToken(token)
      val appMaster = new KyuubiAppMaster()
      appMaster.init(conf)
      val amClient = mock[AMRMClient[AMRMClient.ContainerRequest]]
      ReflectUtils.setFieldValue(appMaster,
        "yaooqinn$kyuubi$yarn$KyuubiAppMaster$$amRMClient", amClient)
      doNothing().when(amClient).start()
      when(amClient.allocate(0.1f)).thenThrow(classOf[OutOfMemoryError])
      when(amClient.stop()).thenThrow(classOf[RuntimeException])
      appMaster.start()

      KyuubiYarnClientSuite.setEnv("CONTAINER_ID",
        Some("container_e73_1553171221511_2225568_01_000001"))
      ReflectUtils.setFieldValue(appMaster,
        "yaooqinn$kyuubi$yarn$KyuubiAppMaster$$amMaxAttempts", 1)
      intercept[RuntimeException](appMaster.stop(FinalApplicationStatus.FAILED, "stop"))
      intercept[RuntimeException](appMaster.stop(FinalApplicationStatus.SUCCEEDED, "stop"))
      KyuubiYarnClientSuite.setEnv("CONTAINER_ID", None)
    }
  }
}
