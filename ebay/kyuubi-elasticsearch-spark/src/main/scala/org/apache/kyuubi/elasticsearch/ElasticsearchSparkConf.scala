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

package org.apache.kyuubi.elasticsearch

import org.apache.kyuubi.config.{ConfigEntry, OptionalConfigEntry}
import org.apache.kyuubi.config.KyuubiConf.buildConf

object ElasticsearchSparkConf {
  val ELASTICSEARCH_ENABLED: ConfigEntry[Boolean] =
    buildConf("kyuubi.spark.elasticsearch.enabled")
      .internal
      .booleanConf
      .createWithDefault(false)

  val ELASTICSEARCH_VIP: OptionalConfigEntry[String] =
    buildConf("kyuubi.spark.elasticsearch.nodes")
      .internal
      .stringConf
      .createOptional

  val ELASTICSEARCH_PORT: ConfigEntry[Int] =
    buildConf("kyuubi.spark.elasticsearch.port")
      .internal
      .intConf
      .createWithDefault(443)

  val ELASTICSEARCH_SSL: ConfigEntry[Boolean] =
    buildConf("kyuubi.spark.elasticsearch.ssl")
      .internal
      .booleanConf
      .createWithDefault(true)

  val ELASTICSEARCH_AUTH_USERNAME: OptionalConfigEntry[String] =
    buildConf("kyuubi.spark.elasticsearch.auth.username")
      .internal
      .stringConf
      .createOptional

  val ELASTICSEARCH_AUTH_PASSWORD: OptionalConfigEntry[String] =
    buildConf("kyuubi.spark.elasticsearch.auth.password")
      .internal
      .stringConf
      .createOptional

  val ELASTICSEARCH_PROPERTIES_FILE: OptionalConfigEntry[String] =
    buildConf("kyuubi.spark.elasticsearch.properties.file")
      .internal
      .stringConf
      .createOptional

  val ELASTICSEARCH_INITIALIZE_SQL: ConfigEntry[Seq[String]] =
    buildConf("kyuubi.spark.elasticsearch.initialize.sql")
      .internal
      .stringConf
      .toSequence(";")
      .createWithDefault(Nil)
}
