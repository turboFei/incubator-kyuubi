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
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.ebay.FileSessionConfAdvisorSuite.{OVERLAY_CONF_DEMO, SESSION_CONF_DEMO}

class FileSessionConfAdvisorSuite extends KyuubiFunSuite {
  test("test file session conf advisor") {
    val advisor = new FileSessionConfAdvisor()
    assert(advisor.getConfOverlay(
      "demo",
      Map(SESSION_CONF_PROFILE.key -> "profile1").asJava).asScala ==
      Map("kyuubi.profile1" -> "true"))
    assert(advisor.getConfOverlay(
      "demo",
      Map(SESSION_CONF_PROFILE.key -> "tess-28").asJava).asScala ==
      Map("kyuubi.kubernetes.context" -> "28"))
    assert(advisor.getConfOverlay("demo", SESSION_CONF_DEMO.asJava).asScala ==
      OVERLAY_CONF_DEMO)
  }
}

object FileSessionConfAdvisorSuite {
  val SESSION_CONF_DEMO = Map(SESSION_CONF_PROFILE.key -> "profile1,tess-28")
  val OVERLAY_CONF_DEMO = Map("kyuubi.profile1" -> "true", "kyuubi.kubernetes.context" -> "28")
}
