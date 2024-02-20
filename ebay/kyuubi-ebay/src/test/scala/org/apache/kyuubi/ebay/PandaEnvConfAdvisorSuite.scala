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

class PandaEnvConfAdvisorSuite extends KyuubiFunSuite {
  import PandaEnvConfAdvisorSuite._

  test("test panda env conf advisor") {
    var sessionConf = SESSION_CONF_DEMO.asJava
    var overlayConf = OVERLAY_CONF_DEMO.asJava
    val sessionConfAdvisor = new PandaEnvConfAdvisor()
    assert(sessionConfAdvisor.getConfOverlay("b_stf", sessionConf) == overlayConf)

    sessionConf = Map(KyuubiEbayConf.KYUUBI_SESSION_TYPE_KEY -> "BATCH").asJava
    overlayConf = Map("kyuubi.batchConf.spark." + PandaEnvConfAdvisor.SPARK_SUBMIT_ENV ->
      PandaEnvConfAdvisor.SUBMIT_ENV_CLI).asJava
    assert(sessionConfAdvisor.getConfOverlay("b_stf", sessionConf) == overlayConf)

    sessionConf = Map(
      KyuubiEbayConf.KYUUBI_SESSION_TYPE_KEY -> "BATCH",
      KyuubiEbayConf.KYUUBI_BATCH_MAIN_CLASS -> PandaEnvConfAdvisor.ETL_SQL_DRIVER).asJava
    overlayConf = Map("kyuubi.batchConf.spark." + PandaEnvConfAdvisor.SPARK_SUBMIT_ENV ->
      PandaEnvConfAdvisor.SUBMIT_ENV_ETL).asJava
    assert(sessionConfAdvisor.getConfOverlay("b_stf", sessionConf) == overlayConf)

    sessionConf = Map.empty[String, String].asJava
    overlayConf =
      Map(PandaEnvConfAdvisor.SPARK_SUBMIT_ENV -> PandaEnvConfAdvisor.SUBMIT_ENV_KYUUBI).asJava
    assert(sessionConfAdvisor.getConfOverlay("b_stf", sessionConf) == overlayConf)

    sessionConf = Map(KyuubiEbayConf.SESSION_TAG.key -> PandaEnvConfAdvisor.ZETA_TAG).asJava
    overlayConf =
      Map(PandaEnvConfAdvisor.SPARK_SUBMIT_ENV -> PandaEnvConfAdvisor.ZETA_TAG).asJava
    assert(sessionConfAdvisor.getConfOverlay("b_stf", sessionConf) == overlayConf)

    sessionConf =
      Map(KyuubiEbayConf.SESSION_TAG.key -> PandaEnvConfAdvisor.ETL_HANDLER_BEELINE_TAG).asJava
    overlayConf =
      Map(PandaEnvConfAdvisor.SPARK_SUBMIT_ENV -> PandaEnvConfAdvisor.SUBMIT_ENV_ETL).asJava
    assert(sessionConfAdvisor.getConfOverlay("b_stf", sessionConf) == overlayConf)
  }

}

object PandaEnvConfAdvisorSuite {
  val SESSION_CONF_DEMO = Map(KyuubiEbayConf.SESSION_TAG.key -> PandaEnvConfAdvisor.ZETA_TAG)

  val OVERLAY_CONF_DEMO = Map(
    PandaEnvConfAdvisor.SPARK_SUBMIT_ENV -> PandaEnvConfAdvisor.SUBMIT_ENV_ZETA)
}
