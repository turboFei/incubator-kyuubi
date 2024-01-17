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

class MiscConfAdvisorSuite extends KyuubiFunSuite {
  import MiscConfAdvisorSuite._
  test("test misc advisor") {
    val miscConfAdvisor = new MiscConfAdvisor()
    assert(miscConfAdvisor.getConfOverlay("b_stf", SESSION_CONF_DEMO.asJava) ==
      OVERLAY_CONF_DEMO.asJava)
    assert(miscConfAdvisor.getConfOverlay("b_stf", SESSION_CONF_DEMO_2.asJava) ==
      OVERLAY_CONF_DEMO_2.asJava)
  }
}

object MiscConfAdvisorSuite {
  val SESSION_CONF_DEMO = Map(
    KyuubiEbayConf.EBAY_OPERATION_MAX_RESULT_COUNT.key -> "100",
    KyuubiConf.OPERATION_RESULT_SAVE_TO_FILE.key -> "true")
  val OVERLAY_CONF_DEMO = Map(
    KyuubiConf.OPERATION_RESULT_MAX_ROWS.key -> "100",
    KyuubiConf.OPERATION_INCREMENTAL_COLLECT.key -> "false")

  val SESSION_CONF_DEMO_2 = Map(
    KyuubiEbayConf.EBAY_OPERATION_MAX_RESULT_COUNT.key -> "100",
    KyuubiConf.OPERATION_RESULT_MAX_ROWS.key -> "100",
    KyuubiConf.OPERATION_RESULT_SAVE_TO_FILE.key -> "true")
  val OVERLAY_CONF_DEMO_2 = Map(KyuubiConf.OPERATION_INCREMENTAL_COLLECT.key -> "false")
}
