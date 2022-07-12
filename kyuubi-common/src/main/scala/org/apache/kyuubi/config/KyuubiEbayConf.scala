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

import java.time.Duration

object KyuubiEbayConf {
  private def buildConf(key: String): ConfigBuilder = KyuubiConf.buildConf(key)

  val SESSION_CLUSTER_MODE_ENABLED: ConfigEntry[Boolean] =
    buildConf("kyuubi.session.cluster.mode.enabled")
      .doc("Whether to enable session with cluster specify mode. If it is enabled," +
        " the cluster for session connection is must to be specified.")
      .version("1.4.0")
      .booleanConf
      .createWithDefault(false)

  val SESSION_CLUSTER: OptionalConfigEntry[String] =
    buildConf("kyuubi.session.cluster")
      .doc("The cluster to access, such as apollo-rno, hercules-lvs.  For each cluster," +
        " there should be a defined properties file, whose name is formatted like" +
        " kyuubi-defaults.conf.<cluster>")
      .version("1.4.0")
      .stringConf
      .createOptional

  val AUTHENTICATION_BATCH_ACCOUNT_CLASS: OptionalConfigEntry[String] =
    buildConf("kyuubi.authentication.batchAccount.class")
      .doc("The authentication class name for batch account authentication," +
        " eBay internal requirement")
      .version("1.5.0")
      .stringConf
      .createOptional

  val SESSION_ENGINE_LAUNCH_MOVE_QUEUE_ENABLED: ConfigEntry[Boolean] =
    buildConf("kyuubi.session.engine.launch.moveQueue.enabled")
      .doc("When opening kyuubi session, whether to launch engine at first and then move queue." +
        " Note that, it is only for yarn resource manger.")
      .version("1.4.0")
      .booleanConf
      .createWithDefault(false)

  val SESSION_ENGINE_LAUNCH_MOVE_QUEUE_INIT_QUEUE: OptionalConfigEntry[String] =
    buildConf("kyuubi.session.engine.launch.moveQueue.initQueue")
      .doc("When launch engine and move queue, the init queue.")
      .version("1.6.0")
      .stringConf
      .createOptional

  val SESSION_ENGINE_LAUNCH_MOVE_QUEUE_TIMEOUT: ConfigEntry[Long] =
    buildConf("kyuubi.session.engine.launch.moveQueue.timeout")
      .doc("When launch engine and move queue, the final queue.")
      .version("1.6.0")
      .timeConf
      .createWithDefaultString("PT2M")

  @deprecated(s"using ${KyuubiConf.OPERATION_RESULT_MAX_ROWS} instead", "1.6.0")
  val EBAY_OPERATION_MAX_RESULT_COUNT: ConfigEntry[Int] =
    buildConf("kyuubi.operation.max.result.count")
      .doc(s"(deprecated) Ebay legacy conf, please use the community conf" +
        s" `${KyuubiConf.OPERATION_RESULT_MAX_ROWS.key}`.")
      .version("1.3.0")
      .intConf
      .createWithDefault(0)

  val AUTHENTICATION_BATCH_ACCOUNT_ENDPOINT: ConfigEntry[String] =
    buildConf("kyuubi.authentication.batch.account.endpoint")
      .internal
      .doc("The endpoint for batch account verification.")
      .version("1.6.0")
      .stringConf
      .checkValue(_.contains("$serviceAccount"), "the endpoint should contains `$serviceAccount`")
      .createWithDefault("https://bdp.vip.ebay.com/product/batch/$serviceAccount/service/mapping?")

  val AUTHENTICATION_BATCH_ACCOUNT_LOAD_ALL_ENABLED: ConfigEntry[Boolean] =
    buildConf("kyuubi.authentication.batch.account.load.all.enabled")
      .internal
      .doc("Whether to enable to load all service account and batch account mapping.")
      .version("1.6.0")
      .booleanConf
      .createWithDefault(false)

  val AUTHENTICATION_BATCH_ACCOUNT_LOAD_ALL_ENDPOINT: ConfigEntry[String] =
    buildConf("kyuubi.authentication.batch.account.load.all.endpoint")
      .internal
      .doc("The endpoint for loading all service account and batch account mapping.")
      .version("1.6.0")
      .stringConf
      .createWithDefault("https://bdp.vip.ebay.com/product/batch/service-account-mappings")

  val AUTHENTICATION_BATCH_ACCOUNT_LOAD_ALL_INTERVAL: ConfigEntry[Long] =
    buildConf("kyuubi.authentication.batch.account.load.all.interval")
      .doc("The interval for loading all service account and batch account mapping.")
      .version("1.6.0")
      .timeConf
      .createWithDefault(Duration.ofHours(1).toMillis)

  val AUTHENTICATION_KEYSTONE_ENDPOINT: ConfigEntry[String] =
    buildConf("kyuubi.authentication.keystone.endpoint")
      .internal
      .doc("The endpoint for keystone authentication.")
      .version("1.6.0")
      .stringConf
      .createWithDefault("https://os-identity.vip.ebayc3.com/v2.0/tokens")

  val BATCH_SPARK_FILES: ConfigEntry[Seq[String]] =
    buildConf("kyuubi.batch.spark.files")
      .doc("The spark files for batch job, it will be combined with the customer spark.files.")
      .version("1.6.0")
      .stringConf
      .toSequence()
      .createWithDefault(Nil)

  val BATCH_SPARK_JARS: ConfigEntry[Seq[String]] =
    buildConf("kyuubi.batch.spark.jars")
      .doc("The spark jars for batch job, it will be combined with the customer spark.jars.")
      .version("1.6.0")
      .stringConf
      .toSequence()
      .createWithDefault(Nil)

  val METADATA_STORE_JDBC_TABLE: OptionalConfigEntry[String] =
    buildConf("kyuubi.metadata.store.jdbc.table")
      .internal
      .doc("The table name for jdbc metadata, which is used to isolate the prod and pre-prod env.")
      .version("1.6.0")
      .stringConf
      .createOptional

  val HTTP_PROXY_CLIENT_IP_HEADER: ConfigEntry[String] =
    buildConf("kyuubi.http.proxy.client.ip.header")
      .internal
      .version("1.6.0")
      .stringConf
      .createWithDefault("X-eBay-Web-Tier-IP")
}
