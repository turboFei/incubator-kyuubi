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

class TagBasedSessionConfAdvisorSuite extends KyuubiFunSuite {
  test("test tag based session conf advisor") {
    val sessionConfAdvisor = new TagBasedSessionConfAdvisor()
    val zetaConf = Map("spark.dynamicAllocation.minExecutors" -> "0")
    val bigResultConf = Map(
      "kyuubi.operation.incremental.collect" -> "true",
      "kyuubi.operation.temp.table.collect" -> "true")
    val defaultConf = Map("kyuubi.session.tag" -> TagBasedSessionConfAdvisor.KYUUBI_DEFAULT_TAG)
    val downGradeConf = Map("kyuubi.session.engine.launch.moveQueue.enabled" -> "false")

    assert(sessionConfAdvisor.getConfOverlay("b_stf", Map("kyuubi.session.tag" -> "zeta").asJava)
      === (zetaConf ++ downGradeConf).asJava)
    assert(sessionConfAdvisor.getConfOverlay("kyuubi", Map("kyuubi.session.tag" -> "zeta").asJava)
      === (zetaConf ++ downGradeConf).asJava)
    assert(sessionConfAdvisor.getConfOverlay(
      "b_stf",
      Map("kyuubi.session.tag" -> "big_result").asJava)
      === (bigResultConf ++ downGradeConf).asJava)
    assert(sessionConfAdvisor.getConfOverlay("b_stf", Map("kyuubi.session.tag" -> "other").asJava)
      === downGradeConf.asJava)
    assert(sessionConfAdvisor.getConfOverlay("b_stf", Map.empty[String, String].asJava)
      === (defaultConf ++ downGradeConf).asJava)
  }
}
