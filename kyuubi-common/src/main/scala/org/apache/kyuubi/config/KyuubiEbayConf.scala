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

import java.io.File
import java.time.Duration

import scala.collection.JavaConverters._

import org.apache.kyuubi.{KyuubiException, KyuubiSQLException, Logging, Utils}
import org.apache.kyuubi.config.KyuubiConf.{KYUUBI_CONF_DIR, KYUUBI_CONF_FILE_NAME, KYUUBI_HOME, OPERATION_INCREMENTAL_COLLECT, SESSION_IDLE_TIMEOUT}
import org.apache.kyuubi.session.SessionManager

object KyuubiEbayConf extends Logging {
  private def buildConf(key: String): ConfigBuilder = KyuubiConf.buildConf(key)

  val SESSION_CLUSTER_MODE_ENABLED: ConfigEntry[Boolean] =
    buildConf("kyuubi.session.cluster.mode.enabled")
      .doc("Whether to enable session with cluster specify mode. If it is enabled," +
        " the cluster for session connection is must to be specified.")
      .version("1.4.0")
      .serverOnly
      .booleanConf
      .createWithDefault(false)

  val SESSION_CLUSTER_CONF_REFRESH_INTERVAL: ConfigEntry[Long] =
    buildConf("kyuubi.session.cluster.conf.refresh.interval")
      .doc("The session cluster conf refresh interval.")
      .internal
      .serverOnly
      .timeConf
      .createWithDefaultString("PT10M")

  val SESSION_CLUSTER_LIST: OptionalConfigEntry[Seq[String]] =
    buildConf("kyuubi.session.cluster.list")
      .doc("The session cluster list.")
      .internal
      .serverOnly
      .stringConf
      .toSequence()
      .createOptional

  val SESSION_CLUSTER: OptionalConfigEntry[String] =
    buildConf("kyuubi.session.cluster")
      .doc("The cluster to access, such as apollo-rno, hercules-lvs.  For each cluster," +
        " there should be a defined properties file, whose name is formatted like" +
        " kyuubi-defaults.conf.<cluster>")
      .version("1.4.0")
      .stringConf
      .createOptional

  val ACCESS_BDP_QUEUE_AUTO_SELECTION: ConfigEntry[Boolean] =
    buildConf("kyuubi.access.bdp.queue.auto.selection")
      .serverOnly
      .internal
      .booleanConf
      .createWithDefault(false)

  val ACCESS_BDP_QUEUE_AUTO_SELECTION_IGNORE_LIST: ConfigEntry[Seq[String]] =
    buildConf("kyuubi.access.bdp.queue.auto.selection.ignore.list")
      .serverOnly
      .internal
      .stringConf
      .toSequence()
      .createWithDefault(Seq("hdmi-reserve-test", "hdmi-staging", "hdmi-test"))

  val AUTHENTICATION_BATCH_ACCOUNT_CLASS: OptionalConfigEntry[String] =
    buildConf("kyuubi.authentication.batchAccount.class")
      .doc("The authentication class name for batch account authentication," +
        " eBay internal requirement")
      .version("1.5.0")
      .serverOnly
      .stringConf
      .createOptional

  val SESSION_TAG: OptionalConfigEntry[String] =
    buildConf("kyuubi.session.tag")
      .internal
      .stringConf
      .createOptional

  val SESSION_ENGINE_LAUNCH_MOVE_QUEUE_ENABLED: ConfigEntry[Boolean] =
    buildConf("kyuubi.session.engine.launch.moveQueue.enabled")
      .internal
      .doc("When opening kyuubi session, whether to launch engine at first and then move queue." +
        " Note that, it is only for yarn resource manger.")
      .version("1.4.0")
      .booleanConf
      .createWithDefault(false)

  val SESSION_ENGINE_LAUNCH_MOVE_QUEUE_INIT_QUEUE: OptionalConfigEntry[String] =
    buildConf("kyuubi.session.engine.launch.moveQueue.initQueue")
      .internal
      .doc("When launch engine and move queue, the init queue.")
      .version("1.6.0")
      .stringConf
      .createOptional

  val SESSION_ENGINE_LAUNCH_MOVE_QUEUE_TIMEOUT: ConfigEntry[Long] =
    buildConf("kyuubi.session.engine.launch.moveQueue.timeout")
      .internal
      .doc("When launch engine and move queue, the final queue.")
      .version("1.6.0")
      .timeConf
      .createWithDefaultString("PT2M")

  @deprecated(s"using ${KyuubiConf.OPERATION_RESULT_MAX_ROWS} instead", "1.6.0")
  val EBAY_OPERATION_MAX_RESULT_COUNT: ConfigEntry[Int] =
    buildConf("kyuubi.operation.max.result.count")
      .internal
      .doc(s"(deprecated) Ebay legacy conf, please use the community conf" +
        s" `${KyuubiConf.OPERATION_RESULT_MAX_ROWS.key}`.")
      .version("1.3.0")
      .intConf
      .createWithDefault(0)

  val ACCESS_BDP_DEFAULT_CLUSTER: ConfigEntry[String] =
    buildConf("kyuubi.access.bdp.default.cluster")
      .internal
      .serverOnly
      .stringConf
      .createWithDefault("")

  val ACCESS_BDP_QUEUE_REFRESH_INTERVAL: ConfigEntry[Long] =
    buildConf("kyuubi.access.bdp.queue.refresh.interval")
      .internal
      .serverOnly
      .timeConf
      .checkValue(_ > 0, "must be positive number")
      .createWithDefaultString("PT5M")

  val ACCESS_BDP_QUEUE_CACHE_MAX: ConfigEntry[Int] =
    buildConf("kyuubi.access.bdp.queue.cache.max")
      .internal
      .serverOnly
      .intConf
      .checkValue(_ > 0, "must be positive number")
      .createWithDefault(1000)

  val ACCESS_BDP_URL: ConfigEntry[String] =
    buildConf("kyuubi.access.bdp.url")
      .internal
      .serverOnly
      .stringConf
      .createWithDefault("https://bdp.vip.ebay.com")

  val ACCESS_BDP_CLUSTER: ConfigEntry[Option[String]] =
    buildConf("kyuubi.access.bdp.cluster")
      .internal
      .serverOnly
      .fallbackConf(SESSION_CLUSTER)

  val AUTHENTICATION_BATCH_ACCOUNT_LOAD_ALL_INTERVAL: ConfigEntry[Long] =
    buildConf("kyuubi.authentication.batch.account.load.all.interval")
      .doc("The interval for loading all service account and batch account mapping.")
      .version("1.6.0")
      .serverOnly
      .timeConf
      .createWithDefault(Duration.ofHours(1).toMillis)

  val AUTHENTICATION_KEYSTONE_ENDPOINT: ConfigEntry[String] =
    buildConf("kyuubi.authentication.keystone.endpoint")
      .internal
      .serverOnly
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

  val KYUUBI_SESSION_BATCH_SPARK_FILES: ConfigEntry[Seq[String]] =
    buildConf("kyuubi.session.batch.spark.files")
      .internal
      .fallbackConf(KYUUBI_SESSION_SPARK_FILES)

  val KYUUBI_SESSION_BATCH_SPARK_JARS: ConfigEntry[Seq[String]] =
    buildConf("kyuubi.session.batch.spark.jars")
      .internal
      .fallbackConf(KYUUBI_SESSION_SPARK_JARS)

  val METADATA_STORE_JDBC_TABLE: OptionalConfigEntry[String] =
    buildConf("kyuubi.metadata.store.jdbc.table")
      .internal
      .serverOnly
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
      .serverOnly
      .doc("The config tag for batch spark hbase conf.")
      .version("1.6.0")
      .stringConf
      .createWithDefault("spark_hbase")

  val ENGINE_SPARK_TESS_ENABLED: ConfigEntry[Boolean] =
    buildConf("kyuubi.engine.spark.tess.enabled")
      .internal
      .doc("Whether to enable the spark job on tess.")
      .booleanConf
      .createWithDefault(false)

  val ENGINE_SPARK_TESS_CONFIG_TAG: ConfigEntry[String] =
    buildConf("kyuubi.engine.spark.tess.config.tag")
      .internal
      .serverOnly
      .doc("The config tag for spark tess conf.")
      .stringConf
      .createWithDefault("spark_tess")

  val SESSION_METADATA_RESERVE: ConfigEntry[Boolean] =
    buildConf("kyuubi.session.metadata.reserve")
      .serverOnly
      .doc("Whether to reserve the metadata of session in server side." +
        " If not, batch recovery is not supported for batch session.")
      .booleanConf
      .createWithDefault(true)

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

  val KYUUBI_DESCRIBE_PATH_DATA_SOURCES: ConfigEntry[Seq[String]] =
    buildConf("kyuubi.describe.path.datasource.providers")
      .doc("The supported datasource providers for KYUUBI DESCRIBE PATH Command.")
      .internal
      .stringConf
      .toSequence()
      .createWithDefault(Seq("parquet", "avro", "orc"))

  val LOG_AGG_ENABLED: ConfigEntry[Boolean] =
    buildConf("kyuubi.log.agg.enabled")
      .internal
      .serverOnly
      .booleanConf
      .createWithDefault(false)

  val LOG_AGG_THREADS_NUM: ConfigEntry[Int] =
    buildConf("kyuubi.log.agg.threads.num")
      .internal
      .serverOnly
      .intConf
      .createWithDefault(10)

  val LOG_AGG_CLUSTER_DIR: ConfigEntry[String] =
    buildConf("kyuubi.log.agg.cluster.dir")
      .internal
      .serverOnly
      .stringConf
      .createWithDefault("/kyuubi-logs")

  val LOG_AGG_FETCH_TIMEOUT: ConfigEntry[Long] =
    buildConf("kyuubi.log.agg.fetch.timeout")
      .internal
      .serverOnly
      .longConf
      .createWithDefault(120000)

  val OPERATION_TEMP_TABLE_DATABASE: ConfigEntry[String] =
    buildConf("kyuubi.operation.temp.table.database")
      .internal
      .doc("The database used for the temp tables.")
      .version("1.7.0")
      .stringConf
      .createWithDefault("default")

  val OPERATION_TEMP_TABLE_COLLECT: ConfigEntry[Boolean] =
    buildConf("kyuubi.operation.temp.table.collect")
      .internal
      .doc(s"When true and ${OPERATION_INCREMENTAL_COLLECT.key} is true," +
        s" engine will try to save the result into a temp table first.")
      .version("1.7.0")
      .booleanConf
      .createWithDefault(false)

  val OPERATION_TEMP_TABLE_COLLECT_MIN_FILE_SIZE: ConfigEntry[Long] =
    buildConf("kyuubi.operation.temp.table.collect.minFileSize")
      .internal
      .longConf
      .createWithDefault(10 * 1024 * 1024)

  val OPERATION_TEMP_TABLE_COLLECT_FILE_COALESCE_NUM_THRESHOLD: ConfigEntry[Long] =
    buildConf("kyuubi.operation.temp.table.collect.fileCoalesceNumThreshold")
      .internal
      .longConf
      .checkValue(_ > 0, "must be positive")
      .createWithDefault(10)

  val OPERATION_TEMP_TABLE_COLLECT_PARTITION_BYTES: ConfigEntry[Long] =
    buildConf("kyuubi.operation.temp.table.collect.partitionBytes")
      .internal
      .longConf
      .checkValue(_ > 0, "must be positive")
      .createWithDefault(100 * 1024 * 1024)

  val OPERATION_TEMP_TABLE_COLLECT_SORT_LIMIT_ENABLED: ConfigEntry[Boolean] =
    buildConf("kyuubi.operation.temp.table.collect.sortLimitEnabled")
      .internal
      .booleanConf
      .createWithDefault(true)

  val OPERATION_TEMP_TABLE_COLLECT_SORT_LIMIT_SIZE: ConfigEntry[Int] =
    buildConf("kyuubi.operation.temp.table.collect.sortLimitSize")
      .internal
      .intConf
      .createWithDefault(5000000)

  val SESSION_PROGRESS_PLAN_ENABLE: ConfigEntry[Boolean] =
    buildConf("kyuubi.operation.progress.plan.enabled")
      .doc("Whether to enable the operation progress plan. When true," +
        " the operation progress plan will be returned in `GetOperationStatus`.")
      .internal
      .version("1.7.0")
      .booleanConf
      .createWithDefault(false)

  val OPERATION_INTERCEPT_ENABLED: ConfigEntry[Boolean] =
    buildConf("kyuubi.operation.intercept.enabled")
      .internal
      .serverOnly
      .booleanConf
      .createWithDefault(false)

  val LOG_AGG_CLEANER_ENABLED: ConfigEntry[Boolean] =
    buildConf("kyuubi.log.agg.cleaner.enabled")
      .internal
      .serverOnly
      .booleanConf
      .createWithDefault(false)

  val LOG_AGG_CLEANER_INTERVAL: ConfigEntry[Long] =
    buildConf("kyuubi.log.agg.cleaner.interval")
      .internal
      .serverOnly
      .timeConf
      .createWithDefaultString("PT6H")

  val LOG_AGG_CLEANER_MAX_LOG_AGE: ConfigEntry[Long] =
    buildConf("kyuubi.log.agg.cleaner.maxAge")
      .internal
      .serverOnly
      .timeConf
      .createWithDefaultString("PT336H")

  val SESSION_TAG_CONF_FILE: ConfigEntry[String] =
    buildConf("kyuubi.session.tag.conf.file")
      .internal
      .serverOnly
      .stringConf
      .createWithDefault("kyuubi-overwrite.conf.tag")

  val SESSION_TESS_CONF_FILE: ConfigEntry[String] =
    buildConf("kyuubi.session.tess.conf.file")
      .internal
      .serverOnly
      .stringConf
      .createWithDefault("kyuubi-overwrite.conf.spark_tess")

  val SESSION_TESS_SPARK_DRIVER_CORES_DEFAULT: ConfigEntry[String] =
    buildConf("kyuubi.session.tess.spark.driver.cores.default")
      .internal
      .serverOnly
      .stringConf
      .createWithDefaultString("2")

  val SESSION_TESS_SPARK_EXECUTOR_CORES_DEFAULT: ConfigEntry[String] =
    buildConf("kyuubi.session.tess.spark.executor.cores.default")
      .internal
      .serverOnly
      .stringConf
      .createWithDefaultString("2")

  val SESSION_TESS_SCRATCH_DIR: OptionalConfigEntry[String] =
    buildConf("kyuubi.session.tess.scratchdir")
      .internal
      .stringConf
      .createOptional

  val ELASTIC_SEARCH_CREDENTIAL_PROVIDER_CLASS: ConfigEntry[String] =
    buildConf("kyuubi.elastic.search.credential.provider.class")
      .internal
      .serverOnly
      .stringConf
      .createWithDefault("org.apache.kyuubi.ebay.server.events.FileBasedCredentialsProvider")

  val ELASTIC_SEARCH_CREDENTIAL_FILE: ConfigEntry[String] =
    buildConf("kyuubi.elastic.search.credential.file")
      .internal
      .serverOnly
      .stringConf
      .createWithDefault("/etc/security/keytabs/elasticsearch.key")

  val ELASTIC_SEARCH_CREDENTIAL_REFRESH_INTERVAL: ConfigEntry[Long] =
    buildConf("kyuubi.elastic.search.credential.refresh.interval")
      .internal
      .serverOnly
      .timeConf
      .createWithDefault(Duration.ofMinutes(10).toMillis)

  val ELASTIC_SEARCH_CREDENTIAL_MAX_ATTEMPTS: ConfigEntry[Int] =
    buildConf("kyuubi.elastic.search.credential.max.attempts")
      .internal
      .serverOnly
      .intConf
      .createWithDefault(3)

  val ELASTIC_SEARCH_CREDENTIAL_RETRY_WAIT: ConfigEntry[Long] =
    buildConf("kyuubi.elastic.search.credential.retry.wait")
      .internal
      .serverOnly
      .timeConf
      .createWithDefault(Duration.ofSeconds(5).toMillis)

  val ELASTIC_SEARCH_HOST: ConfigEntry[String] =
    buildConf("kyuubi.elastic.search.host")
      .internal
      .serverOnly
      .stringConf
      .createWithDefault("localhost")

  val ELASTIC_SEARCH_PORT: ConfigEntry[Int] =
    buildConf("kyuubi.elastic.search.port")
      .internal
      .serverOnly
      .intConf
      .createWithDefault(9200)

  val ELASTIC_SEARCH_SCHEMA: ConfigEntry[String] =
    buildConf("kyuubi.elastic.search.schema")
      .internal
      .serverOnly
      .stringConf
      .createWithDefault("http")

  val ELASTIC_SEARCH_REQUEST_MAX_ATTEMPTS: ConfigEntry[Int] =
    buildConf("kyuubi.elastic.search.request.max.attempts")
      .internal
      .serverOnly
      .intConf
      .createWithDefault(3)

  val ELASTIC_SEARCH_REQUEST_RETRY_WAIT: ConfigEntry[Long] =
    buildConf("kyuubi.elastic.search.request.retry.wait")
      .internal
      .serverOnly
      .timeConf
      .createWithDefault(Duration.ofSeconds(5).toMillis)

  val ELASTIC_SEARCH_SESSION_EVENT_INDEX: ConfigEntry[String] =
    buildConf("kyuubi.elastic.search.session.event.index")
      .internal
      .serverOnly
      .stringConf
      .createWithDefault("kyuubi-session-events")

  val ELASTIC_SEARCH_OPERATION_EVENT_INDEX: ConfigEntry[String] =
    buildConf("kyuubi.elastic.search.operation.event.index")
      .internal
      .serverOnly
      .stringConf
      .createWithDefault("kyuubi-operation-events")

  val ELASTIC_SEARCH_SERVER_EVENT_INDEX: ConfigEntry[String] =
    buildConf("kyuubi.elastic.search.server.event.index")
      .internal
      .serverOnly
      .stringConf
      .createWithDefault("kyuubi-server-events")

  val ELASTIC_SEARCH_SESSION_EVENT_ALIAS: ConfigEntry[String] =
    buildConf("kyuubi.elastic.search.session.event.alias")
      .internal
      .serverOnly
      .stringConf
      .createWithDefault("kyuubi_session_events")

  val ELASTIC_SEARCH_OPERATION_EVENT_ALIAS: ConfigEntry[String] =
    buildConf("kyuubi.elastic.search.operation.event.alias")
      .internal
      .serverOnly
      .stringConf
      .createWithDefault("kyuubi_operation_events")

  val ELASTIC_SEARCH_SERVER_EVENT_ALIAS: ConfigEntry[String] =
    buildConf("kyuubi.elastic.search.server.event.alias")
      .internal
      .serverOnly
      .stringConf
      .createWithDefault("kyuubi_server_events")

  val ELASTIC_SEARCH_SERVER_EVENT_PURGE_ENABLED: ConfigEntry[Boolean] =
    buildConf("kyuubi.elastic.search.server.event.purge.enabled")
      .internal
      .serverOnly
      .booleanConf
      .createWithDefault(false)

  val ELASTIC_SEARCH_SESSION_EVENT_PURGE_ENABLED: ConfigEntry[Boolean] =
    buildConf("kyuubi.elastic.search.session.event.purge.enabled")
      .internal
      .serverOnly
      .booleanConf
      .createWithDefault(false)

  val ELASTIC_SEARCH_OPERATION_EVENT_PURGE_ENABLED: ConfigEntry[Boolean] =
    buildConf("kyuubi.elastic.search.operation.event.purge.enabled")
      .internal
      .serverOnly
      .booleanConf
      .createWithDefault(false)

  val ELASTIC_SEARCH_SERVER_EVENT_MAX_DAYS: ConfigEntry[Int] =
    buildConf("kyuubi.elastic.search.server.event.max.days")
      .internal
      .serverOnly
      .intConf
      .createWithDefault(1095) // 3 years

  val ELASTIC_SEARCH_SESSION_EVENT_MAX_DAYS: ConfigEntry[Int] =
    buildConf("kyuubi.elastic.search.session.event.max.days")
      .internal
      .serverOnly
      .intConf
      .createWithDefault(90) // 3 months

  val ELASTIC_SEARCH_OPERATION_EVENT_MAX_DAYS: ConfigEntry[Int] =
    buildConf("kyuubi.elastic.search.operation.event.max.days")
      .internal
      .serverOnly
      .intConf
      .createWithDefault(90) // 3 months

  val ELASTIC_SEARCH_DAILY_SESSION_INDEX: ConfigEntry[String] =
    buildConf("kyuubi.elastic.search.daily.session.index")
      .internal
      .serverOnly
      .stringConf
      .createWithDefault("kyuubi_daily_session")

  val ELASTIC_SEARCH_DAILY_OPERATION_INDEX: ConfigEntry[String] =
    buildConf("kyuubi.elastic.search.daily.operation.index")
      .internal
      .serverOnly
      .stringConf
      .createWithDefault("kyuubi_daily_operation")

  val ELASTIC_SEARCH_USER_DAILY_SESSION_INDEX: ConfigEntry[String] =
    buildConf("kyuubi.elastic.search.user.daily.session.index")
      .internal
      .serverOnly
      .stringConf
      .createWithDefault("kyuubi_user_daily_session")

  val ELASTIC_SEARCH_USER_DAILY_OPERATION_INDEX: ConfigEntry[String] =
    buildConf("kyuubi.elastic.search.user.daily.operation.index")
      .internal
      .serverOnly
      .stringConf
      .createWithDefault("kyuubi_user_daily_operation")

  val ELASTIC_SEARCH_AGG_ENABLED: ConfigEntry[Boolean] =
    buildConf("kyuubi.elastic.search.agg.enabled")
      .internal
      .serverOnly
      .booleanConf
      .createWithDefault(false)

  val ELASTIC_SEARCH_AGG_DAYS: ConfigEntry[Int] =
    buildConf("kyuubi.elastic.search.agg.days")
      .internal
      .serverOnly
      .intConf
      .createWithDefault(2)

  val ELASTIC_SEARCH_AGG_INTERVAL: ConfigEntry[Long] =
    buildConf("kyuubi.elastic.search.agg.interval")
      .internal
      .serverOnly
      .timeConf
      .createWithDefaultString("PT1H")

  val ELASTIC_SEARCH_AGG_SIZE: ConfigEntry[Int] =
    buildConf("kyuubi.elastic.search.agg.size")
      .internal
      .serverOnly
      .intConf
      .createWithDefault(Int.MaxValue)

  val ELASTIC_SEARCH_AGG_MIN_DOC_COUNT: ConfigEntry[Int] =
    buildConf("kyuubi.elastic.search.agg.min.doc.count")
      .internal
      .serverOnly
      .intConf
      .createWithDefault(1)

  val CARMEL_CLUSTER_LIST: ConfigEntry[Seq[String]] =
    buildConf("kyuubi.carmel.cluster.list")
      .internal
      .serverOnly
      .stringConf
      .toSequence()
      .createWithDefault(Nil)

  val CARMEL_SESSION_IDLE_TIME: ConfigEntry[Long] =
    buildConf("kyuubi.carmel.session.idle.timeout")
      .internal
      .serverOnly
      .fallbackConf(SESSION_IDLE_TIMEOUT)

  val CARMEL_ENGINE_URL_KEY: ConfigEntry[String] =
    buildConf("kyuubi.carmel.engine.url.key")
      .internal
      .serverOnly
      .stringConf
      .createWithDefault(
        "spark.org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter.param.PROXY_URI_BASES")

  val SERVER_DEREGISTER_GRACEFUL_PERIOD: ConfigEntry[Long] =
    buildConf("kyuubi.server.deregister.gracefulPeriod")
      .internal
      .serverOnly
      .doc("The graceful period to deregister server from discovery service.")
      .timeConf
      .createWithDefaultString("PT5M")

  val HADOOP_PATH_QUALIFY_CONFIGS: ConfigEntry[Seq[String]] =
    buildConf("kyuubi.hadoop.path.qualify.configs")
      .internal
      .serverOnly
      .doc("The path configs to make qualified with hadoop defaultFS.")
      .stringConf
      .toSequence()
      .createWithDefault(Seq("spark.files", "spark.jars", "spark.submit.pyFiles"))

  val HADOOP_PATH_QUALIFY_ENABLED: ConfigEntry[Boolean] =
    buildConf("kyuubi.hadoop.path.qualify.enabled")
      .internal
      .doc("Whether to qualify the path configs with hadoop defaultFS.")
      .booleanConf
      .createWithDefault(false)

  val SERVER_LIMIT_CONNECTIONS_TAG_LIMITS: ConfigEntry[Seq[String]] =
    buildConf("kyuubi.server.limit.connections.tagLimits")
      .internal
      .serverOnly
      .doc("The session tag and limit for server connections limit, such as `tag1=10,tag2=10`.")
      .stringConf
      .toSequence()
      .createWithDefault(Nil)

  def getDefaultPropertiesFileForCluster(
      clusterOpt: Option[String],
      conf: KyuubiConf = KyuubiConf().loadFileDefaults(),
      env: Map[String, String] = sys.env): Option[File] = {
    clusterOpt.map { cluster =>
      val clusterPropertiesFileName = KYUUBI_CLUSTER_CONF_FILE_NAME_PREFIX + cluster
      env.get(KYUUBI_CONF_DIR)
        .orElse(env.get(KYUUBI_HOME).map(_ + File.separator + "conf"))
        .map(d => new File(d + File.separator + clusterPropertiesFileName))
        .filter(_.exists())
        .orElse {
          Option(getClass.getClassLoader.getResource(clusterPropertiesFileName)).map { url =>
            new File(url.getFile)
          }.filter(_.exists())
        }.orElse(throw KyuubiSQLException(
          s"""
             |Failed to get properties file[$clusterPropertiesFileName] for cluster[$cluster].
             |It should be one of ${KyuubiEbayConf.getClusterList(conf).mkString("[", ",", "]")}.
        """.stripMargin))
    }.getOrElse(None)
  }

  def loadClusterConf(conf: KyuubiConf, clusterOpt: Option[String]): KyuubiConf = {
    val clusterConf = conf.clone
    getDefaultPropertiesFileForCluster(clusterOpt, conf).foreach { clusterPropertiesFile =>
      Utils.getPropertiesFromFile(Option(clusterPropertiesFile)).foreach {
        case (key, value) => clusterConf.set(key, value)
      }
    }
    clusterConf
  }

  /** the cluster default file name prefix */
  final val KYUUBI_CLUSTER_CONF_FILE_NAME_PREFIX = KYUUBI_CONF_FILE_NAME + "."

  private def getDefaultPropertiesClusterList(confDir: File): Seq[String] = {
    confDir.listFiles().map(_.getName).filter(_.startsWith(KYUUBI_CLUSTER_CONF_FILE_NAME_PREFIX))
      .map(_.stripPrefix(KYUUBI_CLUSTER_CONF_FILE_NAME_PREFIX))
  }

  private def getDefinedPropertiesClusterList(env: Map[String, String] = sys.env): Seq[String] = {
    env.get(KYUUBI_CONF_DIR)
      .orElse(env.get(KYUUBI_HOME).map(_ + File.separator + "conf"))
      .map(d => new File(d))
      .filter(_.isDirectory())
      .map(getDefaultPropertiesClusterList)
      .getOrElse {
        var clusterList: Seq[String] = Seq.empty[String]
        val classPathUrls = getClass.getClassLoader.getResources(".").asScala
        for (classPathUrl <- classPathUrls if clusterList.isEmpty) {
          val classPathDir = new File(classPathUrl.getFile)
          if (classPathDir.isDirectory) {
            clusterList = getDefaultPropertiesClusterList(classPathDir)
          }
        }
        clusterList
      }
  }

  def isCarmelCluster(conf: KyuubiConf, sessionCluster: Option[String]): Boolean = {
    sessionCluster.exists(conf.get(CARMEL_CLUSTER_LIST).contains)
  }

  def getClusterOptList(conf: KyuubiConf): Seq[Option[String]] = {
    if (conf.get(SESSION_CLUSTER_MODE_ENABLED)) {
      getClusterList(conf).map(Option(_))
    } else {
      Seq(None)
    }
  }

  def getCarmelClusterOptList(conf: KyuubiConf): Seq[Option[String]] = {
    if (conf.get(SESSION_CLUSTER_MODE_ENABLED)) {
      conf.get(CARMEL_CLUSTER_LIST).map(Option(_))
    } else {
      Seq(None)
    }
  }

  def getNonCarmelClusterOptList(conf: KyuubiConf): Seq[Option[String]] = {
    if (conf.get(SESSION_CLUSTER_MODE_ENABLED)) {
      getClusterList(conf).filterNot(conf.get(CARMEL_CLUSTER_LIST).contains).map(Option(_))
    } else {
      Seq(None)
    }
  }

  def getClusterList(conf: KyuubiConf = KyuubiConf().loadFileDefaults()): Seq[String] = {
    if (conf.get(SESSION_CLUSTER_MODE_ENABLED)) {
      conf.get(SESSION_CLUSTER_LIST).getOrElse(getDefinedPropertiesClusterList())
    } else {
      Seq.empty
    }
  }

  private[kyuubi] def checkClusterOpt(conf: KyuubiConf, clusterOpt: Option[String]): Unit = {
    if (conf.get(SESSION_CLUSTER_MODE_ENABLED)) {
      if (clusterOpt.isEmpty || !getClusterList(conf).contains(clusterOpt.get)) {
        throw new KyuubiException(
          s"Please specify the cluster to access with session conf[${SESSION_CLUSTER.key}]" +
            s" or restful request cluster param, which should be one of" +
            s" ${getClusterList(conf).mkString("[", ",", "]")}," +
            s" current value is $clusterOpt")
      }
    }
  }

  def getSessionCluster(sessionMgr: SessionManager, conf: Map[String, String]): Option[String] = {
    if (sessionMgr.getConf.get(SESSION_CLUSTER_MODE_ENABLED)) {
      val clusterOpt = sessionMgr.validateAndNormalizeConf(conf).get(SESSION_CLUSTER.key).orElse(
        sessionMgr.getConf.get(SESSION_CLUSTER))
      checkClusterOpt(sessionMgr.getConf, clusterOpt)
      clusterOpt
    } else {
      None
    }
  }

  def getTagConfOnly(conf: KyuubiConf, tag: String): Map[String, String] = {
    conf.getAllWithPrefix(s"___${tag}___", "")
  }

  def getTagDefaultConf(
      conf: Map[String, String],
      tagEnabledKey: ConfigEntry[Boolean],
      configTagKey: ConfigEntry[String],
      sessionConf: KyuubiConf,
      userDefaultConf: KyuubiConf): Map[String, String] = {
    if (conf.get(tagEnabledKey.key).map(_.toBoolean).getOrElse(
        userDefaultConf.get(tagEnabledKey))) {
      getTagConfOnly(sessionConf, sessionConf.get(configTagKey)).filter(c => conf.get(c._1).isEmpty)
    } else {
      Map.empty
    }
  }

  def toBatchConf(conf: Map[String, String], batchType: String = "spark"): Map[String, String] = {
    conf.map { case (key, value) =>
      if (key.startsWith("kyuubi.")) {
        key -> value
      } else {
        s"${KyuubiConf.KYUUBI_BATCH_CONF_PREFIX}.$batchType.$key" -> value
      }
    }
  }

  def isKyuubiBatch(sessionConf: Map[String, String]): Boolean = {
    sessionConf.get(KYUUBI_SESSION_TYPE_KEY).exists(_.equalsIgnoreCase("BATCH"))
  }

  def confOverlayForSessionType(
      sessionConf: Map[String, String],
      confOverlay: Map[String, String]): Map[String, String] = {
    confOverlayForSessionType(isKyuubiBatch(sessionConf), confOverlay)
  }

  def confOverlayForSessionType(
      isBatch: Boolean,
      confOverlay: Map[String, String]): Map[String, String] = {
    if (isBatch) {
      KyuubiEbayConf.toBatchConf(confOverlay)
    } else {
      confOverlay
    }
  }

  def getBatchTagDefaultConf(
      conf: Map[String, String],
      tagEnabledKey: ConfigEntry[Boolean],
      configTagKey: ConfigEntry[String],
      sessionConf: KyuubiConf,
      userDefaultConf: KyuubiConf): Map[String, String] = {
    toBatchConf(getTagDefaultConf(conf, tagEnabledKey, configTagKey, sessionConf, userDefaultConf))
  }

  def getSessionTag(conf: KyuubiConf): Option[String] = {
    conf.get(SESSION_TAG).orElse(conf.getOption(org.apache.kyuubi.session.SESSION_TAG))
  }

  def getSessionTag(conf: Map[String, String]): Option[String] = {
    conf.get(SESSION_TAG.key).orElse(conf.get(org.apache.kyuubi.session.SESSION_TAG))
  }

  def getSessionTag(sessionMgr: SessionManager, conf: Map[String, String]): Option[String] = {
    sessionMgr.validateAndNormalizeConf(conf).get(SESSION_TAG.key)
      .orElse(conf.get(org.apache.kyuubi.session.SESSION_TAG))
  }

  def moveQueueEnabled(conf: KyuubiConf): Boolean = {
    getSessionTag(conf) == Some(ZETA_TAG_KEY) && conf.get(
      SESSION_ENGINE_LAUNCH_MOVE_QUEUE_ENABLED) && !conf.get(
      KyuubiEbayConf.ENGINE_SPARK_TESS_ENABLED)
  }

  /**
   * Get the server connection limiter tags and limit.
   */
  def getServerLimiterTagsAndLimits(conf: KyuubiConf): Map[String, Int] = {
    conf.get(KyuubiEbayConf.SERVER_LIMIT_CONNECTIONS_TAG_LIMITS).flatMap { tagLimit =>
      tagLimit.split("=") match {
        case Array(tag, limit) if limit.toInt > 0 => Some(tag -> limit.toInt)
        case _ => None
      }
    }.toMap
  }

  case class ServerConnectionLimitScope(cluster: Option[String], tag: Option[String])
  def normalizeServerConnectionLimitScope(
      limitScope: ServerConnectionLimitScope,
      limiterTags: Seq[String]): ServerConnectionLimitScope = {
    limitScope.copy(tag = limitScope.tag.filter(limiterTags.contains))
  }

  private[kyuubi] lazy val _kyuubiConf = KyuubiConf().loadFileDefaults()

  final val KYUUBI_SESSION_ID_KEY = "kyuubi.session.id"
  final val KYUUBI_SESSION_TYPE_KEY = "kyuubi.session.type"
  final val KYUUBI_BATCH_MAIN_CLASS = "kyuubi.batch.mainClass"
  final val ZETA_TAG_KEY = "zeta"
}
