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

class ChainedSessionConfAdvisorSuite extends KyuubiFunSuite {
  test("test chained session conf advisor") {
    val advisor = new ChainedSessionConfAdvisor()
    val sessionConf = SparkMajorVersionAdvisorSuite.SESSION_CONF_DEMO ++
      TagBasedSessionConfAdvisorSuite.SESSION_CONF_DEMO ++
      TessConfAdvisorSuite.SESSION_CONF_DEMO ++
      MiscConfAdvisorSuite.SESSION_CONF_DEMO ++
      PandaEnvConfAdvisorSuite.SESSION_CONF_DEMO ++
      PySparkConfAdvisorSuite.SESSION_CONF_DEMO

    val overlayConf = SparkMajorVersionAdvisorSuite.OVERLAY_CONF_DEMO ++
      TagBasedSessionConfAdvisorSuite.OVERLAY_CONF_DEMO ++
      TessConfAdvisorSuite.OVERLAY_CONF_DEMO ++
      MiscConfAdvisorSuite.OVERLAY_CONF_DEMO ++
      PandaEnvConfAdvisorSuite.OVERLAY_CONF_DEMO ++
      PySparkConfAdvisorSuite.OVERLAY_CONF_DEMO ++
      Map("kyuubi.kubernetes.context" -> "28")

    assert(advisor.getConfOverlay("demo", sessionConf.asJava).asScala == overlayConf)
  }
}
