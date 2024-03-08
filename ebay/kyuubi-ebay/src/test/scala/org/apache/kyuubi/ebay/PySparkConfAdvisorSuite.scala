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
import org.apache.kyuubi.config.{KyuubiConf, KyuubiEbayConf}
import org.apache.kyuubi.config.KyuubiConf.OperationLanguages

class PySparkConfAdvisorSuite extends KyuubiFunSuite {

  test("test pyspark conf advisor") {
    val sessionConfAdvisor = new PySparkConfAdvisor()

    var sessionConf = PySparkConfAdvisorSuite.SESSION_CONF_DEMO.asJava
    var confOverlay = PySparkConfAdvisorSuite.OVERLAY_CONF_DEMO.asJava
    assert(sessionConfAdvisor.getConfOverlay("b_stf", sessionConf) == confOverlay)

    sessionConf = Map.empty[String, String].asJava
    confOverlay = Map.empty[String, String].asJava
    assert(sessionConfAdvisor.getConfOverlay("b_stf", sessionConf) == confOverlay)

    sessionConf =
      Map(KyuubiEbayConf.SESSION_TAG.key -> PandaEnvConfAdvisor.ETL_HANDLER_BEELINE_TAG).asJava
    confOverlay = Map(PySparkConfAdvisor.SPARK_IS_PYTHON -> false.toString).asJava
    assert(sessionConfAdvisor.getConfOverlay("b_stf", sessionConf) == confOverlay)
  }
}

object PySparkConfAdvisorSuite {
  val SESSION_CONF_DEMO =
    Map(KyuubiConf.OPERATION_LANGUAGE.key -> OperationLanguages.PYTHON.toString)

  val OVERLAY_CONF_DEMO = Map("spark.yarn.isPython" -> "true")
}