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

  val KYUUBI_SESSION_SPARK_FILES: ConfigEntry[Seq[String]] =
    buildConf("kyuubi.session.spark.files")
      .doc("The spark files for kyuubi session, it will be combined with the customer spark.files.")
      .version("1.6.0")
      .stringConf
      .toSequence()
      .createWithDefault(Nil)

  val KYUUBI_SESSION_SPARK_JARS: ConfigEntry[Seq[String]] =
    buildConf("kyuubi.session.spark.jars")
      .doc("The spark jars for kyuubi session, it will be combined with the customer spark.jars.")
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

  val BATCH_SPARK_HBASE_ENABLED: ConfigEntry[Boolean] =
    buildConf("kyuubi.batch.spark.hbase.enabled")
      .doc("Whether to enable the spark batch job with hbase environment, if it is true, kyuubi" +
        " will inject hbase lib into batch application classpath and upload the hbase" +
        " configuration file hbase-site.xml")
      .version("1.6.0")
      .booleanConf
      .createWithDefault(false)

  val BATCH_SPARK_HBASE_CONFIG_TAG: ConfigEntry[String] =
    buildConf("kyuubi.batch.spark.hbase.config.tag")
      .internal
      .doc("The config tag for batch spark hbase conf.")
      .version("1.6.0")
      .stringConf
      .createWithDefault("spark_hbase")

  val DATA_UPLOAD_ENABLED: ConfigEntry[Boolean] =
    buildConf("kyuubi.data.upload.enabled")
      .doc("Enable data upload")
      .internal
      .booleanConf
      .createWithDefault(true)

  val DATA_UPLOAD_DYNAMIC_PARTITION_ENABLED: ConfigEntry[Boolean] =
    buildConf("kyuubi.data.upload.dynamic.partition.enabled")
      .doc("Enable data upload for dynamic partition insert")
      .internal
      .booleanConf
      .createWithDefault(true)

  val DATA_UPLOAD_TEMPORARY_FILE_MAX_SIZE: ConfigEntry[Long] =
    buildConf("kyuubi.data.upload.temporary.file.max.size")
      .doc("The max size of file uploaded in bytes for data uploading.")
      .internal
      .longConf
      .createWithDefault(512 * 1024 * 1024)

  val DATA_UPLOAD_NOT_ALLOWED_CSV_OPTIONS =
    buildConf("kyuubi.data.upload.not.allowed.csv.options")
      .doc("The not allowed csv options for data uploading.")
      .internal
      .stringConf
      .createWithDefault("path")

  val DATA_DOWNLOAD_MAX_SIZE: ConfigEntry[Long] =
    buildConf("kyuubi.data.download.max.size")
      .doc("The maximum data size allowed downloaded.")
      .internal
      .longConf
      .createWithDefault(100L * 1024 * 1024 * 1024) // 100G

  val LOG_AGG_ENABLED: ConfigEntry[Boolean] =
    buildConf("kyuubi.log.agg.enabled")
      .internal
      .booleanConf
      .createWithDefault(false)

  val LOG_AGG_THREADS_NUM: ConfigEntry[Int] =
    buildConf("kyuubi.log.agg.threads.num")
      .internal
      .intConf
      .createWithDefault(10)

  val LOG_AGG_CLUSTER_DIR: ConfigEntry[String] =
    buildConf("kyuubi.log.agg.cluster.dir")
      .internal
      .stringConf
      .createWithDefault("/kyuubi-logs")

  val LOG_AGG_FETCH_TIMEOUT: ConfigEntry[Long] =
    buildConf("kyuubi.log.agg.fetch.timeout")
      .internal
      .longConf
      .createWithDefault(120000)
}
