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

package org.apache.kyuubi.config

import org.apache.kyuubi.{KyuubiException, KyuubiFunSuite}
import org.apache.kyuubi.config.KyuubiEbayConf.{SESSION_CLUSTER, SESSION_CLUSTER_MODE_ENABLED}
import org.apache.kyuubi.session.NoopSessionManager

class KyuubiEbayConfSuite extends KyuubiFunSuite {
  test("get cluster list") {
    assert(KyuubiEbayConf.getClusterList() == Seq.empty)

    val conf = KyuubiConf()
    conf.set(KyuubiEbayConf.SESSION_CLUSTER_MODE_ENABLED, true)
    conf.set(KyuubiEbayConf.SESSION_CLUSTER_LIST, Seq("test1", "test2", "carmel"))
    conf.set(KyuubiEbayConf.CARMEL_CLUSTER_LIST, Seq("carmel"))
    assert(KyuubiEbayConf.getClusterList(conf) === Seq("test1", "test2", "carmel"))
    assert(KyuubiEbayConf.getClusterOptList(conf) ==
      Seq(Option("test1"), Option("test2"), Option("carmel")))
    assert(KyuubiEbayConf.getNonCarmelClusterOptList(conf) == Seq(Option("test1"), Option("test2")))
    assert(KyuubiEbayConf.getCarmelClusterOptList(conf) == Seq(Option("carmel")))

    conf.set(KyuubiEbayConf.SESSION_CLUSTER_MODE_ENABLED, false)
    assert(KyuubiEbayConf.getClusterList(conf) == Seq.empty)
    assert(KyuubiEbayConf.getClusterOptList(conf) == Seq(None))
    assert(KyuubiEbayConf.getNonCarmelClusterOptList(conf) == Seq(None))
    assert(KyuubiEbayConf.getCarmelClusterOptList(conf) == Seq(None))
  }

  test("getDefaultPropertiesFileForCluster") {
    assert(KyuubiEbayConf.getDefaultPropertiesFileForCluster(Option("test")).nonEmpty)
  }

  test("get session cluster") {
    val conf = KyuubiConf()
    val sessionManager = new NoopSessionManager()
    sessionManager.initialize(conf)
    conf.set(KyuubiEbayConf.SESSION_CLUSTER_MODE_ENABLED, true)
    intercept[KyuubiException] {
      KyuubiEbayConf.getSessionCluster(sessionManager, Map.empty[String, String])
    }
    assert(KyuubiEbayConf.getSessionCluster(
      sessionManager,
      Map(KyuubiEbayConf.SESSION_CLUSTER.key -> "test"))
      === Some("test"))
    assert(KyuubiEbayConf.getSessionCluster(
      sessionManager,
      Map("set:" + KyuubiEbayConf.SESSION_CLUSTER.key -> "test"))
      === Some("test"))
    assert(KyuubiEbayConf.getSessionCluster(
      sessionManager,
      Map("set:hiveconf:" + KyuubiEbayConf.SESSION_CLUSTER.key -> "test"))
      === Some("test"))
    assert(KyuubiEbayConf.getSessionCluster(
      sessionManager,
      Map("set:hivevar:" + KyuubiEbayConf.SESSION_CLUSTER.key -> "test"))
      === Some("test"))
    sessionManager.stop()
  }

  test("test get conf") {
    val conf = KyuubiConf().loadFileDefaults()
    assert(KyuubiEbayConf.loadClusterConf(conf, None).getOption(
      "spark.sql.kyuubi.session.cluster.test").isEmpty)
    assert(KyuubiEbayConf.loadClusterConf(conf, Some("test")).getOption(
      "spark.sql.kyuubi.session.cluster.test")
      === Some("yes"))
  }

  test("test SESSION_CLUSTER_MODE_ENABLED and SESSION_CLUSTER") {
    val kyuubiConf = KyuubiConf()
    assert(!kyuubiConf.get(SESSION_CLUSTER_MODE_ENABLED))
    kyuubiConf.set(SESSION_CLUSTER_MODE_ENABLED, true)
    assert(kyuubiConf.get(SESSION_CLUSTER_MODE_ENABLED))
    assert(kyuubiConf.get(SESSION_CLUSTER).isEmpty)
    kyuubiConf.set(SESSION_CLUSTER, "test")
    assert(kyuubiConf.get(SESSION_CLUSTER) == Option("test"))
  }

  test("only enable move queue for zeta connection") {
    val kyuubiConf = KyuubiConf()
    kyuubiConf.set(KyuubiEbayConf.SESSION_TAG, "zeta")
    kyuubiConf.set(KyuubiEbayConf.SESSION_ENGINE_LAUNCH_MOVE_QUEUE_ENABLED, true)
    assert(KyuubiEbayConf.moveQueueEnabled(kyuubiConf))
    kyuubiConf.set(KyuubiEbayConf.SESSION_TAG, "other")
    assert(!KyuubiEbayConf.moveQueueEnabled(kyuubiConf))
    kyuubiConf.unset(KyuubiEbayConf.SESSION_TAG)
    assert(!KyuubiEbayConf.moveQueueEnabled(kyuubiConf))
  }

  test("getServerLimiterTagsAndLimit") {
    val kyuubiConf = KyuubiConf()
    kyuubiConf.set(KyuubiEbayConf.SERVER_LIMIT_CONNECTIONS_TAG_LIMITS.key, "zeta=100,etl=50,test=0")
    assert(KyuubiEbayConf.getServerLimiterTagsAndLimits(kyuubiConf) ==
      Map("zeta" -> 100, "etl" -> 50))
  }
}
