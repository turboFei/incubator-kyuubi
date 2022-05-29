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

package org.apache.kyuubi.ha

import java.time.Duration

import org.apache.hadoop.security.UserGroupInformation

import org.apache.kyuubi.config.{ConfigBuilder, ConfigEntry, KyuubiConf, OptionalConfigEntry}
import org.apache.kyuubi.ha.client.AuthTypes
import org.apache.kyuubi.ha.client.RetryPolicies

object HighAvailabilityConf {

  private def buildConf(key: String): ConfigBuilder = KyuubiConf.buildConf(key)

  val SERVER_HA_ZK_ENABLED: ConfigEntry[Boolean] = buildConf("kyuubi.server.ha.zookeeper.enabled")
    .doc("Whether to enable the ha zookeeper discovery in server side")
    .version("1.4.0")
    .booleanConf
    .createWithDefault(true)

  val HA_ZK_QUORUM: ConfigEntry[String] = buildConf("kyuubi.ha.zookeeper.quorum")
    .doc("The connection string for the zookeeper ensemble")
    .version("1.0.0")
    .stringConf
    .createWithDefault("")

  val HA_ZK_NAMESPACE: ConfigEntry[String] = buildConf("kyuubi.ha.zookeeper.namespace")
    .doc("The root directory for the service to deploy its instance uri")
    .version("1.0.0")
    .stringConf
    .createWithDefault("kyuubi")

  @deprecated(s"using ${HA_ZK_AUTH_TYPE.key} and ${HA_ZK_ENGINE_AUTH_TYPE.key} instead", "1.3.2")
  val HA_ZK_ACL_ENABLED: ConfigEntry[Boolean] =
    buildConf("kyuubi.ha.zookeeper.acl.enabled")
      .doc("Set to true if the zookeeper ensemble is kerberized")
      .version("1.0.0")
      .booleanConf
      .createWithDefault(UserGroupInformation.isSecurityEnabled)

  val HA_ZK_AUTH_TYPE: ConfigEntry[String] =
    buildConf("kyuubi.ha.zookeeper.auth.type")
      .doc("The type of zookeeper authentication, all candidates are " +
        s"${AuthTypes.values.mkString("<ul><li>", "</li><li> ", "</li></ul>")}")
      .version("1.3.2")
      .stringConf
      .checkValues(AuthTypes.values.map(_.toString))
      .createWithDefault(AuthTypes.NONE.toString)

  val HA_ZK_ENGINE_AUTH_TYPE: ConfigEntry[String] =
    buildConf("kyuubi.ha.zookeeper.engine.auth.type")
      .doc("The type of zookeeper authentication for engine, all candidates are " +
        s"${AuthTypes.values.mkString("<ul><li>", "</li><li> ", "</li></ul>")}")
      .version("1.3.2")
      .fallbackConf(HA_ZK_AUTH_TYPE)

  val HA_ZK_AUTH_PRINCIPAL: ConfigEntry[Option[String]] =
    buildConf("kyuubi.ha.zookeeper.auth.principal")
      .doc("Name of the Kerberos principal is used for zookeeper authentication.")
      .version("1.3.2")
      .fallbackConf(KyuubiConf.SERVER_PRINCIPAL)

  val HA_ZK_AUTH_KEYTAB: ConfigEntry[Option[String]] = buildConf("kyuubi.ha.zookeeper.auth.keytab")
    .doc("Location of Kyuubi server's keytab is used for zookeeper authentication.")
    .version("1.3.2")
    .fallbackConf(KyuubiConf.SERVER_KEYTAB)

  val HA_ZK_AUTH_DIGEST: OptionalConfigEntry[String] = buildConf("kyuubi.ha.zookeeper.auth.digest")
    .doc("The digest auth string is used for zookeeper authentication, like: username:password.")
    .version("1.3.2")
    .stringConf
    .createOptional

  val HA_ZK_CONN_MAX_RETRIES: ConfigEntry[Int] =
    buildConf("kyuubi.ha.zookeeper.connection.max.retries")
      .doc("Max retry times for connecting to the zookeeper ensemble")
      .version("1.0.0")
      .intConf
      .createWithDefault(3)

  val HA_ZK_CONN_BASE_RETRY_WAIT: ConfigEntry[Int] =
    buildConf("kyuubi.ha.zookeeper.connection.base.retry.wait")
      .doc("Initial amount of time to wait between retries to the zookeeper ensemble")
      .version("1.0.0")
      .intConf
      .createWithDefault(1000)

  val HA_ZK_CONN_MAX_RETRY_WAIT: ConfigEntry[Int] =
    buildConf("kyuubi.ha.zookeeper.connection.max.retry.wait")
      .doc(s"Max amount of time to wait between retries for" +
        s" ${RetryPolicies.BOUNDED_EXPONENTIAL_BACKOFF} policy can reach, or max time until" +
        s" elapsed for ${RetryPolicies.UNTIL_ELAPSED} policy to connect the zookeeper ensemble")
      .version("1.0.0")
      .intConf
      .createWithDefault(30 * 1000)

  val HA_ZK_CONN_TIMEOUT: ConfigEntry[Int] = buildConf("kyuubi.ha.zookeeper.connection.timeout")
    .doc("The timeout(ms) of creating the connection to the zookeeper ensemble")
    .version("1.0.0")
    .intConf
    .createWithDefault(15 * 1000)

  val HA_ZK_SESSION_TIMEOUT: ConfigEntry[Int] = buildConf("kyuubi.ha.zookeeper.session.timeout")
    .doc("The timeout(ms) of a connected session to be idled")
    .version("1.0.0")
    .intConf
    .createWithDefault(60 * 1000)

  val HA_ZK_CONN_RETRY_POLICY: ConfigEntry[String] =
    buildConf("kyuubi.ha.zookeeper.connection.retry.policy")
      .doc("The retry policy for connecting to the zookeeper ensemble, all candidates are:" +
        s" ${RetryPolicies.values.mkString("<ul><li>", "</li><li> ", "</li></ul>")}")
      .version("1.0.0")
      .stringConf
      .checkValues(RetryPolicies.values.map(_.toString))
      .createWithDefault(RetryPolicies.EXPONENTIAL_BACKOFF.toString)

  val HA_ZK_NODE_TIMEOUT: ConfigEntry[Long] =
    buildConf("kyuubi.ha.zookeeper.node.creation.timeout")
      .doc("Timeout for creating zookeeper node")
      .version("1.2.0")
      .timeConf
      .checkValue(_ > 0, "Must be positive")
      .createWithDefault(Duration.ofSeconds(120).toMillis)

  val HA_ZK_ENGINE_REF_ID: OptionalConfigEntry[String] =
    buildConf("kyuubi.ha.engine.ref.id")
      .doc("The engine reference id will be attached to zookeeper node when engine started, " +
        "and the kyuubi server will check it cyclically.")
      .internal
      .version("1.3.2")
      .stringConf
      .createOptional

  val HA_ZK_PUBLISH_CONFIGS: ConfigEntry[Boolean] =
    buildConf("kyuubi.ha.zookeeper.publish.configs")
      .doc("When set to true, publish Kerberos configs to Zookeeper." +
        "Note that the Hive driver needs to be greater than 1.3 or 2.0 or apply HIVE-11581 patch.")
      .version("1.4.0")
      .booleanConf
      .createWithDefault(false)

  val HA_ZK_ENGINE_SECURE_SECRET_NODE: OptionalConfigEntry[String] =
    buildConf("kyuubi.ha.zookeeper.engine.secure.secret.node")
      .internal
      .doc("The zk node contains the secret that used for internal secure, please make sure " +
        "that it is only visible for Kyuubi.")
      .version("1.5.0")
      .stringConf
      .createOptional

  val HA_DISCOVERY_CLIENT_CLASS: ConfigEntry[String] =
    buildConf("kyuubi.ha.service.discovery.client.class")
      .doc("Class name for service discovery client.")
      .version("1.6.0")
      .stringConf
      .checkValue(_.nonEmpty, "must not be empty")
      .createWithDefault("org.apache.kyuubi.ha.client.zookeeper.ZookeeperDiscoveryClient")
}
