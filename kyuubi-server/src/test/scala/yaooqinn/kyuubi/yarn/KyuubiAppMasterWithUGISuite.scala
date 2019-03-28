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

import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.{KyuubiConf, KyuubiSparkUtil}
import org.scalatest.mock.MockitoSugar

import yaooqinn.kyuubi.KyuubiSQLException
import yaooqinn.kyuubi.ha.{HighAvailableService, ZookeeperFunSuite}
import yaooqinn.kyuubi.server.KyuubiServer
import yaooqinn.kyuubi.utils.ReflectUtils

class KyuubiAppMasterWithUGISuite extends ZookeeperFunSuite with MockitoSugar {

  import yaooqinn.kyuubi.yarn.KyuubiYarnClientSuite._

  val user = UserGroupInformation.getCurrentUser
  val username = user.getUserName
  var server: KyuubiServer = _
  var haService: HighAvailableService = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    conf.remove(KyuubiSparkUtil.CATALOG_IMPL)
    conf.set(KyuubiConf.YARN_CONTAINER_TIMEOUT.key, "10s")
    conf.set(KyuubiConf.YARN_KYUUBIAPPMASTER_MEMORY.key, "10m")
    conf.set(KyuubiConf.YARN_KYUUBIAPPMASTER_MEMORY_OVERHEAD.key, "1m")
    conf.set(KyuubiConf.YARN_KYUUBIAPPMASTER_CORES.key, "1")
    server = new KyuubiServer()
    haService = new HighAvailableService("test", server) {
      override protected def reset(): Unit = {}
    }
  }

  override def afterAll(): Unit = {
    if (server != null) {
      server.stop()
    }

    if (haService != null) {
      haService.stop()
    }
    conf.remove(KyuubiSparkUtil.CATALOG_IMPL)
    conf.remove(KyuubiConf.YARN_CONTAINER_TIMEOUT.key)
    super.afterAll()
  }

  test("test create kyuubiAppMaster") {
    withTempDir { dir =>
      withTempJarsDir(dir) { (_, _, _) =>
        withMiniCluster(dir) { _ =>
          val kyuubiAppMasterWithUGI = new KyuubiAppMasterWithUGI(user, conf)
          val sessionConf = Map("spark.executor.instances" -> "1",
            "set:hivevar:spark.executor.cores"-> "1",
            "set:hivevar:key" -> "value")
          val e = intercept[KyuubiSQLException](kyuubiAppMasterWithUGI.init(sessionConf))
          assert(e.getMessage === s"Get KyuubiAppMaster for [$username] failed")
          assert(kyuubiAppMasterWithUGI.kyuubiAmInstance === None)
        }
      }
    }
  }

  test("test getInstance") {
    haService.init(conf)
    val kyuubiAppMasterWithUGI = new KyuubiAppMasterWithUGI(user, conf)
    val serviceNameSpace = haService.getServiceNameSpace
    val zkClient = HighAvailableService.newZookeeperClient(conf)
    val instanceURI = "localhost:10009"
    val pathPrefix = serviceNameSpace + "/" +
      "serverUri=" + instanceURI + ";" +
      "version=" + "TEST" + ";" +
      "sequence="
    zkClient.create().forPath(pathPrefix, instanceURI.getBytes())

    ReflectUtils.setFieldValue(kyuubiAppMasterWithUGI,
      "yaooqinn$kyuubi$yarn$KyuubiAppMasterWithUGI$$instanceParentPath",
      serviceNameSpace)
    ReflectUtils.setFieldValue(kyuubiAppMasterWithUGI, "zookeeperClient", zkClient)
    var instanceExists = ReflectUtils.invokeMethod(kyuubiAppMasterWithUGI,
      "yaooqinn$kyuubi$yarn$KyuubiAppMasterWithUGI$$isIntanceExists")
    assert(instanceExists === true)
    var instance = ReflectUtils.invokeMethod(kyuubiAppMasterWithUGI, "getInstance")
        .asInstanceOf[Option[String]]
    assert(instance.get === instanceURI)

    ReflectUtils.setFieldValue(kyuubiAppMasterWithUGI,
      "yaooqinn$kyuubi$yarn$KyuubiAppMasterWithUGI$$instanceParentPath",
      "notExistsPath")
    instanceExists = ReflectUtils.invokeMethod(kyuubiAppMasterWithUGI,
      "yaooqinn$kyuubi$yarn$KyuubiAppMasterWithUGI$$isIntanceExists")
    assert(instanceExists === false)
    instance = ReflectUtils.invokeMethod(kyuubiAppMasterWithUGI, "getInstance")
      .asInstanceOf[Option[String]]
    assert(instance === None)
  }

  test("test get lock path and instancePath") {
    val prefix = "/" + conf.get(KyuubiConf.YARN_KYUUBIAPPMASTER_NAMESPACE.key) + "/" +
      username + "/"
    assert(KyuubiAppMasterWithUGI.getLockPath(username, conf) === prefix + "LOCK")
    assert(KyuubiAppMasterWithUGI.getInstanceNameSpace(username, conf) ===
      prefix + HighAvailableService.URIPATH)
  }
}
