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

package org.apache.kyuubi.ebay

import scala.collection.JavaConverters._

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiEbayConf

class TessConfAdvisorSuite extends KyuubiFunSuite {
  import TessConfAdvisorSuite._

  test("test tess conf advisor") {
    val advisor = new TessConfAdvisor()
    assert(advisor.getConfOverlay("demo", SESSION_CONF_DEMO.asJava).asScala ==
      OVERLAY_CONF_DEMO)

    assert(advisor.getConfOverlay("demo", Map.empty[String, String].asJava).asScala == Map.empty)

    assert(advisor.getConfOverlay(
      "demo",
      (SESSION_CONF_DEMO.asJava.asScala ++ Map(
        KyuubiEbayConf.KYUUBI_SESSION_TYPE_KEY -> "BATCH")).asJava).asScala ==
      KyuubiEbayConf.toBatchConf(OVERLAY_CONF_DEMO))
  }
}

object TessConfAdvisorSuite {
  val SESSION_CONF_DEMO = Map(
    KyuubiEbayConf.ENGINE_SPARK_TESS_ENABLED.key -> "true",
    "kyuubi.hadoop.adlc.app" -> "adlc",
    "kyuubi.hadoop.adlc.ai" -> "adlc-ai",
    "spark.kubernetes.context" -> "28")

  val OVERLAY_CONF_DEMO = Map(
    "spark.kubernetes.executor.label.applicationinstance.tess.io/name" -> "adlc-ai",
    "spark.kubernetes.executor.request.cores" -> "2",
    "spark.kubernetes.executor.annotation.application.tess.io/name" -> "adlc",
    "spark.kubernetes.driver.request.cores" -> "2",
    "spark.kubernetes.driver.annotation.application.tess.io/name" -> "adlc",
    "spark.kubernetes.executor.limit.cores" -> "2",
    "spark.kubernetes.driver.limit.cores" -> "2",
    "spark.kubernetes.driver.annotation.io.sherlock.logs/namespace" -> "adlc",
    "spark.kubernetes.executor.annotation.io.sherlock.logs/namespace" -> "adlc",
    "spark.kubernetes.driver.annotation.io.sherlock.metrics/namespace" -> "adlc",
    "spark.kubernetes.executor.annotation.io.sherlock.metrics/namespace" -> "adlc",
    "spark.kubernetes.driver.label.applicationinstance.tess.io/name" -> "adlc-ai",
    "spark.tess.overwrite" -> "true",
    "kyuubi.kubernetes.context" -> "28")
}
