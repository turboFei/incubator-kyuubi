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

package org.apache.kyuubi.ebay.carmel.gateway.config;

import java.io.IOException;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang3.StringUtils;

public class CarmelConfig {

  public static final String CARMEL_CONFIG_FILE = "carmel.properties";
  private final Properties properties;

  private CarmelConfig() {
    this(Optional.empty());
  }

  private CarmelConfig(String cluster) {
    this(Optional.ofNullable(cluster));
  }

  private CarmelConfig(Optional<String> clusterOpt) {
    String cluster = clusterOpt.orElse("");
    this.properties = new Properties();
    try {
      ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
      if (classLoader == null) {
        classLoader = CarmelConfig.class.getClassLoader();
      }
      if (classLoader.getResource(CARMEL_CONFIG_FILE) != null) {
        this.properties.load(classLoader.getResourceAsStream(CARMEL_CONFIG_FILE));
      }
      if (!StringUtils.isBlank(cluster)) {
        if (classLoader.getResource(CARMEL_CONFIG_FILE + "." + cluster) != null) {
          this.properties.load(classLoader.getResourceAsStream(CARMEL_CONFIG_FILE + "." + cluster));
        }
      }
      configCheck();
    } catch (IOException e) {
      throw new RuntimeException("Failed to init GatewayConfig with cluster " + cluster, e);
    }
  }

  public static CarmelConfig getInstance() {
    return GatewayConfigHolder.INSTANCE;
  }

  public static CarmelConfig getClusterInstance(String cluster) {
    return GatewayConfigHolder.CLUSTER_INSTANCES.computeIfAbsent(cluster, c -> new CarmelConfig(c));
  }

  private void configCheck() throws IOException {
    for (ConfVars var : ConfVars.values()) {
      String varName = var.name;
      if (properties.containsKey(varName) && var.validator != null) {
        String result = var.validator.validate(properties.getProperty(varName));
        if (result != null) {
          throw new IOException(result);
        }
      }
    }
  }

  public String getVar(ConfVars var) {
    String value = properties.getProperty(var.name);
    if (value != null) {
      return value;
    }
    if (var.defaultValue != null) {
      return var.defaultValue;
    }
    return null;
  }

  /**
   * get dynamic property value
   *
   * @param var specified confVar
   * @param dynName dynamic property name
   * @return
   */
  public String getVar(ConfVars var, String dynName) {
    String value = properties.getProperty(var.name + "." + dynName);
    if (value != null) {
      return value;
    } else {
      return getVar(var);
    }
  }

  public boolean getBoolVar(ConfVars var) {
    String value = properties.getProperty(var.name);
    if (value != null) {
      return Boolean.parseBoolean(value);
    }
    if (var.defaultValue != null) {
      return Boolean.parseBoolean(var.defaultValue);
    }
    return false;
  }

  public int getIntVar(ConfVars var) {
    String value = properties.getProperty(var.name);
    if (value != null) {
      return Integer.parseInt(value);
    }
    return Integer.parseInt(var.defaultValue);
  }

  public long getLongVar(ConfVars var) {
    String value = properties.getProperty(var.name);
    if (value != null) {
      return Long.parseLong(value);
    }
    return Long.parseLong(var.defaultValue);
  }

  // for test purpose
  public void setVar(ConfVars var, String value) {
    if (var.validator != null) {
      var.validator.validate(value);
    }
    properties.setProperty(var.name, value);
  }

  public enum ConfVars {

    // Carmel gateway server configurations
    CARMEL_GATEWAY_AUTH_SASL_ENABLED(
        "carmel.gateway.auth.sasl.enabled", "true", new StringSet("true", "false")),
    CARMEL_GATEWAY_AUTH_SASL_KERBEROS_ENABLED(
        "carmel.gateway.auth.sasl.kerberos.enabled", "true", new StringSet("true", "false")),
    CARMEL_GATEWAY_AUTH_SASL_PLAIN_AUTH_TYPE(
        "carmel.gateway.auth.sasl.plain.auth.type",
        "ldap",
        new StringSet("none", "ldap", "keystone")),
    CARMEL_GATEWAY_AUTH_KEYSTONE_ENDPOINT(
        "carmel.gateway.auth.keystone.endpoint",
        "https://os-identity.vip.ebayc3.com/v2.0/tokens",
        null),
    CARMEL_GATEWAY_HTTP_AUTH_MODE(
        "carmel.gateway.http.auth.mode", "none", new StringSet("none", "ldap", "kerberos")),
    CARMEL_GATEWAY_HTTP_AUTH_LADP_URL(
        "carmel.gateway.http.auth.ldap.url", "ldap://sl-addc03.prod.ebay.com:389", null),
    CARMEL_GATEWAY_HTTP_AUTH_LADP_DOMAIN(
        "carmel.gateway.http.auth.ldap.domain", "prod.ebay.com", null),
    CARMEL_GATEWAY_HTTP_AUTH_LADP_GROUPDNPATTERN(
        "carmel.gateway.http.auth.ldap.groupDNPattern", null, null),
    CARMEL_GATEWAY_HTTP_AUTH_LADP_USERDNPATTERN(
        "carmel.gateway.http.auth.ldap.userDNPattern", null, null),
    CARMEL_GATEWAY_HTTP_AUTH_LADP_GROUPCLASSKEY(
        "carmel.gateway.http.auth.ldap.groupClassKey", "groupOfNames", null),
    CARMEL_GATEWAY_HTTP_AUTH_LDAP_GROUPMEMBERSHIPKEY(
        "carmel.gateway.http.auth.ldap.groupMembershipKey", "member", null),
    CARMEL_GATEWAY_ADMIN_SSL_ENABLED(
        "carmel.gateway.admin.ssl.enabled", "false", new StringSet("true", "false")),

    CARMEL_GATEWAY_THRIFT_DISCOVERY(
        "carmel.gateway.thrift.discovery", "file", new StringSet("file", "zookeeper")),

    CARMEL_GATEWAY_ENDPOINT_SELECT_STRATEGY(
        "carmel.gateway.endpoint.select.policy", "tag", new StringSet("rnd", "tag")),

    CARMEL_GATEWAY_THRIFT_ZOOKEEPER_NAMESPACE(
        "carmel.gateway.thrift.zookeeper.namespace", "hiveserver2", null),
    CARMEL_GATEWAY_THRIFT_FILE("carmel.gateway.thrift.file", "carmel-server-url.json", null),
    CARMEL_GATEWAY_THRIFT_TRANSPORT_MODE(
        "carmel.gateway.thrift.transport.mode", "binary", new StringSet("binary", "http")),
    CARMEL_GATEWAY_THRIFT_HTTP_PATH(
        "carmel.gateway.thrift.http.path", "/cliservice", new StringSet("/cliservice")),
    CARMEL_GATEWAY_THRIFT_HTTP_COOKIE_ENABLED(
        "carmel.gateway.thrift.http.cookie.enabled", "false", new StringSet("true", "false")),

    CARMEL_GATEWAY_ZOOKEEPER_PATH("carmel.gateway.zookeeper.path", "gateway", null),
    CARMEL_GATEWAY_SESSION_TIMEOUT_SECS("carmel.gateway.session.timeout.secs", "7200", null),
    CARMEL_GATEWAY_SESSION_MAX_TIMEOUT_SECS(
        "carmel.gateway.session.max.timeout.secs", "14400", null),
    CARMEL_GATEWAY_SESSION_MAX_NUM("carmel.gateway.session.max.num", "100", null),
    CARMEL_GATEWAY_SESSION_MAX_NUM_PER_USER("carmel.gateway.session.max.num.per.user", "20", null),
    CARMEL_GATEWAY_SESSION_MAX_NUM_WHITE_LIST(
        "carmel.gateway.session.max.num.white.list", "b_carmel", null),
    CARMEL_GATEWAY_STATEMENT_MAX_NUM_PER_USER(
        "carmel.gateway.statement.max.num.per.user", "50", null),
    CARMEL_GATEWAY_SESSION_CREATE_MAX_TRY("carmel.gateway.session.create.max.try.cnt", "3", null),
    CARMEL_GATEWAY_SESSION_FLY_SUPPORT(
        "carmel.gateway.session.fly.support", "false", new StringSet("true", "false")),

    CARMEL_GATEWAY_SSL_PORT_ENABLED(
        "carmel.gateway.ssl.port.enabled", "false", new StringSet("true", "false")),
    CARMEL_GATEWAY_NON_SSL_PORT_ENABLED(
        "carmel.gateway.non-ssl.port.enabled", "true", new StringSet("true", "false")),
    CARMEL_GATEWAY_TRANSPORT_MODE(
        "carmel.gateway.transport.mode", "http", new StringSet("http", "binary")),
    CARMEL_GATEWAY_TRANSPORT_PORT("carmel.gateway.transport.port", "10000", null),
    CARMEL_GATEWAY_TRANSPORT_SSL_PORT("carmel.gateway.transport.ssl.port", "10443", null),
    CARMEL_GATEWAY_HTTP_PATH("carmel.gateway.http.path", "/*", null),
    CARMEL_GATEWAY_MAX_MSG_SIZE("carmel.gateway.max.msg.size", "104857600", null),
    CARMEL_GATEWAY_REQ_TIMEOUT_SEC("carmel.gateway.request.timeout.sec", "30", null),
    CARMEL_GATEWAY_MIN_WORKER_THREADS("carmel.gateway.worker.threads.min", "8", null),
    CARMEL_GATEWAY_MAX_WORKER_THREADS("carmel.gateway.worker.threads.max", "200", null),
    CARMEL_GATEWAY_WORKER_KEEP_LIVE_SEC("carmel.gateway.worker.keep.live.sec", "60", null),
    CARMEL_GATEWAY_TO_SERVER_TIMEOUT_SEC("carmel.gateway.thrift.rpc.timeout.sec", "300", null),

    CARMEL_GATEWAY_CREDENTIAL_CACHE_TIME_SEC("carmel.gateway.credential.cache.sec", "60", null),

    CARMEL_GATEWAY_SUPER_USER("carmel.gateway.super.user", null, null),
    CARMEL_GATEWAY_USER_ALLOW_PROXY_GROUP("carmel.gateway.user.allowProxyGroup", null, null),
    CARMEL_GATEWAY_ACCESS_MODE("carmel.gateway.access.mode", "file", new StringSet("file", "bdp")),
    CARMEL_GATEWAY_ACCESS_FILE("carmel.gateway.access.file", "carmel-user.json", null),
    CARMEL_GATEWAY_ACCESS_BDP_URL(
        "carmel.gateway.access.bdp.url", "http://bdp-site.vip.ebay.com", null),
    CARMEL_GATEWAY_ACCESS_BDP_CLUSTER_NAME(
        "carmel.gateway.access.bdp.cluster.name", "hermesrno", null),

    CARMEL_GATEWAY_AUDIT_MODE("carmel.gateway.audit.mode", "file", null),
    CARMEL_GATEWAY_AUDIT_FILE_NAME("carmel.gateway.audit.file.name", null, null),
    CARMEL_GATEWAY_AUDIT_MYSQL_URL("carmel.gateway.audit.mysql.url", null, null),
    CARMEL_GATEWAY_AUDIT_MYSQL_NAME("carmel.gateway.audit.mysql.name", null, null),
    CARMEL_GATEWAY_AUDIT_MYSQL_PASSWORD("carmel.gateway.audit.mysql.password", null, null),

    CARMEL_GATEWAY_MONITORING_PUSH_ENDPOINT("carmel.gateway.monitoring.push.endpoint", null, null),
    CARMEL_GATEWAY_MONITORING_PUSH_JOB_NAME(
        "carmel.gateway.monitoring.push.job.name", "carmel-gateway", null),
    CARMEL_GATEWAY_MONITORING_PUSH_PERIOD("carmel.gateway.monitoring.push.period", "60000", null),
    CARMEL_GATEWAY_CLUSTER_DEFAULT_QUEUE(
        "carmel.gateway.cluster.default.queue", "hdmi-default", null),
    CARMEL_GATEWAY_AUTO_QUEUE_IGNORE_LIST(
        "carmel.gateway.cluster.auto.selection.ignore.queues", "hdmi-reserve-test", null),

    // Carmel admin server configurations
    CARMEL_ADMIN_ZK_PATH("carmel.admin.zookeeper.path", "admin", null),
    CARMEL_ADMIN_ZK_SERVER_PATH("carmel.admin.zookeeper.server.path", "server", null),
    CARMEL_ADMIN_ZK_QUEUE_PATH("carmel.admin.zookeeper.queue.path", "queue", null),
    CARMEL_ADMIN_HTTP_PORT("carmel.admin.http.port", "8090", null),
    CARMEL_ADMIN_SPARK_LOCAL_DIR("carmel.admin.spark.local.dir", "/apache/spark-release", null),
    CARMEL_ADMIN_SPARK_HDFS_DIR(
        "carmel.admin.spark.hdfs.dir", "hdfs://hermes-rno/user/b_carmel/spark", null),
    CARMEL_ADMIN_SPARK_EXECUTOR_MEM("carmel.admin.spark.executor.mem", "33792", null),
    CARMEL_ADMIN_SPARK_EXECUTOR_CORES("carmel.admin.spark.executor.cores", "8", null),
    CARMEL_ADMIN_SPARK_DRIVER_MEM("carmel.admin.spark.driver.mem", "192512", null),
    CARMEL_ADMIN_HIGH_USAGE_QUEUE(
        "carmel.admin.high.usage.queue",
        "hdmi-fin,hdmi-reserved,hdmi-default,hdmi-cac,hdmi-prodanalyst",
        null),
    CARMEL_ADMIN_YARN_URL(
        "carmel.admin.yarn.url", "https://hermes-rno-rm-1.vip.hadoop.ebay.com:50030", null),
    CARMEL_ADMIN_HERMES_URL("carmel.admin.hermes.url", null, null),

    CARMEL_ADMIN_HERMES_TEST_QUERY(
        "carmel.admin.hermes.test.query",
        "select count(*) from access_views.ssa_curncy_plan_rate_dim",
        null),
    CARMEL_ADMIN_MONITOR_EMAIL_ENABLED(
        "carmel.admin.monitor.email.enabled", "true", new StringSet("true", "false")),
    CARMEL_ADMIN_EMAIL_HOST("carmel.admin.email.host", "mx.vip.phx.ebay.com", null),
    CARMEL_ADMIN_EMAIL_SENDER("carmel.admin.email.sender", "dl-ebay-ccoe-btd@ebay.com", null),
    CARMEL_ADMIN_EMAIL_RECEIVER("carmel.admin.email.receiver", "dl-ebay-ccoe-btd@ebay.com", null),
    CARMEL_ADMIN_MONITOR_PAGER_ENABLED(
        "carmel.admin.monitor.pager.enabled", "false", new StringSet("true", "false")),
    CARMEL_ADMIN_PAGER_WHITE_LIST(
        "carmel.admin.pager.white.list", "hdmi-staging,hdmi-reserved-test", null),
    CARMEL_ADMIN_AVAILABLE_WHITE_LIST(
        "carmel.admin.available.white.list", "hdmi-reserved,hdmi-staging,hdmi-reserved-test", null),
    CARMEL_ADMIN_STORAGE_LEAK_AUDIT_ENABLED(
        "carmel.admin.storage.leak.audit.enabled", "true", new StringSet("true", "false")),
    CARMEL_ADMIN_STORAGE_LEAK_AUDIT_INTERVAL("carmel.admin.storage.leak.audit.interval", "7", null),
    CARMEL_ADMIN_STORAGE_LEAK_AUDIT_TRASH_DIR(
        "carmel.admin.storage.leak.audit.trash.dir", "/tmp/spark/storage_leak/", null),
    CARMEL_ADMIN_CACHE_ENABLED(
        "carmel.admin.cache.enabled", "true", new StringSet("true", "false")),
    CARMEL_ADMIN_CACHE_MANAGER_QUEUE("carmel.admin.cache.manager.queue", "hdmi-reserved", null),
    CARMEL_ADMIN_CACHE_INDEX_ENABLED(
        "carmel.admin.cache.index.enabled", "true", new StringSet("true", "false")),
    CARMEL_ADMIN_CACHE_MV_ENABLED(
        "carmel.admin.cache.mv.enabled", "false", new StringSet("true", "false")),
    CARMEL_ADMIN_CACHE_META_ENABLED(
        "carmel.admin.cache.meta.enabled", "false", new StringSet("true", "false")),
    CARMEL_ADMIN_CACHE_META_LEGACY_ENABLED(
        "carmel.admin.cache.meta.legacy.enabled", "false", new StringSet("true", "false")),
    CARMEL_ADMIN_CACHE_META_QUEUE("carmel.admin.cache.meta.queue", null, null),
    CARMEL_ADMIN_CACHE_META_LEGACY_QUEUE("carmel.admin.cache.meta.legacy.queue", null, null),
    CARMEL_ADMIN_INDEX_META_PATH(
        "carmel.admin.index.meta.patch", "hdfs://hermes-rno/user/b_carmel/index", null),
    CARMEL_ADMIN_KAFKA_BOOTSTRAP_SERVERS("carmel.admin.kafka.bootstrap.servers", null, null),
    CARMEL_ADMIN_KAFKA_GROUP_ID("carmel.admin.kafka.group.id", null, null),
    CARMEL_ADMIN_KAFKA_TOPIC("carmel.admin.kafka.topic", null, null),
    CARMEL_ADMIN_ZK_KAFKA_PATH("carmel.admin.zookeeper.kafka.path", "kafka", null),
    CARMEL_ADMIN_INDEX_CONFIG_PARTITIONS(
        "carmel.admin.index.config.partitions", "ubi_t.ubi_event:dt,type;", null),
    CARMEL_ADMIN_INDEX_CONFIG_IGNORE_TABLES(
        "carmel.admin.index.config.ignore.tables", "ubi_t.ubi_event;", null),

    // Carmel development configurations
    CARMEL_DEV_ZOOKEEPER_ENABLED("carmel.dev.zookeeper.enabled", "false", null),
    CARMEL_DEV_THRIFT_SERVER_ENABLED("carmel.dev.thrift.server.enabled", "false", null),
    CARMEL_DEV_THRIFT_SERVER_ENDPOINT("carmel.dev.thrift.server.endpoint", null, null),
    CARMEL_DEV_YARN_QUEUE("carmel.dev.yarn.queue", null, null),

    // Carmel common configurations
    CARMEL_COMMON_SSL_ENABLED("carmel.common.ssl.enabled", "false", new StringSet("true", "false")),
    CARMEL_COMMON_KEYSTORE_PATH("carmel.common.keystore.path", null, null),
    CARMEL_COMMON_KEYSTORE_PWD("carmel.common.keystore.pwd", null, null),
    CARMEL_COMMON_KERBEROS_PRINCIPAL("carmel.common.kerberos.principal", null, null),
    CARMEL_COMMON_KERBEROS_KEYTAB("carmel.common.kerberos.keytab", null, null),
    CARMEL_COMMON_ZK_QUORUM("carmel.common.zookeeper.quorum", null, null),
    CARMEL_COMMON_ZK_NAMESPACE("carmel.common.zookeeper.namespace", "carmel", null),
    CARMEL_COMMON_ENV(
        "carmel.common.env", "hermes-rno", new StringSet("hermes-rno", "carmel-rno", "hermes-lvs"));

    private final String name;
    private final String defaultValue;
    private final Validator validator;

    ConfVars(String name, String defaultValue, Validator validator) {
      this.name = name;
      this.defaultValue = defaultValue;
      this.validator = validator;
    }
  }

  private static class GatewayConfigHolder {
    private static final CarmelConfig INSTANCE = new CarmelConfig();
    private static final ConcurrentHashMap<String, CarmelConfig> CLUSTER_INSTANCES =
        new ConcurrentHashMap<>();
  }
}
