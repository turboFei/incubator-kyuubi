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
import org.apache.kyuubi.ebay.SparkMajorVersionAdvisorSuite.{OVERLAY_CONF_DEMO, SESSION_CONF_DEMO}

class SparkMajorVersionAdvisorSuite extends KyuubiFunSuite {
  test("test major minor version") {
    assert(SparkMajorVersionAdvisor.majorMinorVersionTag("3.4") == None)
    assert(SparkMajorVersionAdvisor.majorMinorVersionTag("3.4.1") == Some("spark3_4"))
    assert(SparkMajorVersionAdvisor.majorMinorVersionTag("3.4.11") == Some("spark3_4"))
    assert(SparkMajorVersionAdvisor.majorMinorVersionTag("3.4.1.1") == None)
  }

  test("test spark major version conf advisor") {
    val sessionConfAdvisor = new SparkMajorVersionAdvisor()
    assert(sessionConfAdvisor.getConfOverlay("b_stf", SESSION_CONF_DEMO.asJava)
      == OVERLAY_CONF_DEMO.asJava)
  }
}

object SparkMajorVersionAdvisorSuite {
  import SparkMajorVersionAdvisor._
  val SESSION_CONF_DEMO = Map(SPARK_MAJOR_VERSION -> "3.4.1")
  val OVERLAY_CONF_DEMO = Map(
    "spark.version" -> "3.4.1",
    "spark.majorVersion.overwrite" -> "true",
    "kyuubi.session.engine.launch.moveQueue.enabled" -> "false")
}
