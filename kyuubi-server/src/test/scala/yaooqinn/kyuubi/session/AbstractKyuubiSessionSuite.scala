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

package yaooqinn.kyuubi.session

import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
import org.apache.hive.service.cli.thrift.TProtocolVersion
import org.apache.spark.{KyuubiConf, KyuubiSparkUtil, SparkFunSuite}
import org.scalatest.mock.MockitoSugar

import yaooqinn.kyuubi.KyuubiSQLException
import yaooqinn.kyuubi.auth.KyuubiAuthFactory
import yaooqinn.kyuubi.cli.GetInfoType
import yaooqinn.kyuubi.server.KyuubiServer
import yaooqinn.kyuubi.utils.ReflectUtils

abstract class AbstractKyuubiSessionSuite extends SparkFunSuite with MockitoSugar {

  var server: KyuubiServer = _
  var session: IKyuubiSession = _

  override def beforeAll(): Unit = {
    System.setProperty(KyuubiConf.FRONTEND_BIND_PORT.key, "0")
    System.setProperty("spark.master", "local")
    server = KyuubiServer.startKyuubiServer()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    System.clearProperty(KyuubiConf.FRONTEND_BIND_PORT.key)
    System.clearProperty("spark.master")
    if (session != null) {
      session.close()
    }
    if (server != null) server.stop()
    super.afterAll()
  }

  test("test session ugi") {
    assert(session.ugi.getAuthenticationMethod === AuthenticationMethod.SIMPLE)
  }

  test("session handle") {
    val handle = session.getSessionHandle
    assert(handle.getProtocolVersion === TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V8)
  }

  test("get last access time") {
    session.getInfo(GetInfoType.SERVER_NAME)
    assert(session.getLastAccessTime !== 0L)
  }

  test("get password") {
    assert(session.getPassword === "")
  }

  test("get no operation time") {
    assert(session.getNoOperationTime !== 0L)
  }

  test("get delegation token for non secured") {
    val authFactory =
      ReflectUtils.getFieldValue(server.feService, "authFactory").asInstanceOf[KyuubiAuthFactory]
    val e = intercept[KyuubiSQLException](
      session.getDelegationToken(authFactory, session.getUserName, session.getUserName))
    assert(e.getMessage === "Delegation token only supported over kerberos authentication")
    assert(e.toTStatus.getSqlState === "08S01")
  }

  test("cancel delegation token for non secured") {
    val authFactory =
      ReflectUtils.getFieldValue(server.feService, "authFactory").asInstanceOf[KyuubiAuthFactory]
    val e = intercept[KyuubiSQLException](
      session.cancelDelegationToken(authFactory, ""))
    assert(e.getMessage === "Delegation token only supported over kerberos authentication")
    assert(e.toTStatus.getSqlState === "08S01")
  }

  test("renew delegation token for non secured") {
    val authFactory =
      ReflectUtils.getFieldValue(server.feService, "authFactory").asInstanceOf[KyuubiAuthFactory]
    val e = intercept[KyuubiSQLException](
      session.renewDelegationToken(authFactory, ""))
    assert(e.getMessage === "Delegation token only supported over kerberos authentication")
    assert(e.toTStatus.getSqlState === "08S01")
  }
}
