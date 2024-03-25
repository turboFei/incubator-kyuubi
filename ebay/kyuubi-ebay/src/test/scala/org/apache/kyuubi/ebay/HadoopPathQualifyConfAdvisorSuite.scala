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

class HadoopPathQualifyConfAdvisorSuite extends KyuubiFunSuite {
  import HadoopPathQualifyConfAdvisorSuite._
  test("zeta path qualify") {
    val advisor = new HadoopPathQualifyConfAdvisor()
    assert(advisor.getConfOverlay("zeta", SESSION_CONF_DEMO.asJava) ==
      OVERLAY_CONF_DEMO.asJava)

    assert(advisor.getConfOverlay("zeta", SESSION_CONF_DEMO2.asJava) ==
      OVERLAY_CONF_DEMO2.asJava)
  }
}

object HadoopPathQualifyConfAdvisorSuite {
  val SESSION_CONF_DEMO = Map(
    "kyuubi.hadoop.path.qualify.enabled" -> "true",
    "kyuubi.session.cluster" -> "apollorno",
    "spark.jars" -> "/apps/kyuubi/zeta1.jar,/apps/kyuubi/zeta2.jar",
    "spark.files" -> "/apps/kyuubi/zeta1.properties,/apps/kyuubi/zeta2.properties",
    "spark.submit.pyFiles" -> "/apps/kyuubi/zeta1.py,/apps/kyuubi/zeta2.py")

  val SESSION_CONF_DEMO2 = Map(
    "kyuubi.hadoop.path.qualify.enabled" -> "true",
    "kyuubi.session.cluster" -> "herculeslvs",
    "spark.jars" -> "/apps/kyuubi/zeta1.jar,/apps/kyuubi/zeta2.jar",
    "spark.files" -> "/apps/kyuubi/zeta1.properties,/apps/kyuubi/zeta2.properties",
    "spark.submit.pyFiles" -> "/apps/kyuubi/zeta1.py,/apps/kyuubi/zeta2.py")

  // scalastyle:off line.size.limit
  val OVERLAY_CONF_DEMO = Map(
    "spark.jars" ->
      "viewfs://apollo-rno/apps/kyuubi/zeta1.jar,viewfs://apollo-rno/apps/kyuubi/zeta2.jar",
    "spark.files" ->
      "viewfs://apollo-rno/apps/kyuubi/zeta1.properties,viewfs://apollo-rno/apps/kyuubi/zeta2.properties",
    "spark.submit.pyFiles" ->
      "viewfs://apollo-rno/apps/kyuubi/zeta1.py,viewfs://apollo-rno/apps/kyuubi/zeta2.py")

  val OVERLAY_CONF_DEMO2 = Map(
    "spark.jars" ->
      "hdfs://hercules/apps/kyuubi/zeta1.jar,hdfs://hercules/apps/kyuubi/zeta2.jar",
    "spark.files" ->
      "hdfs://hercules/apps/kyuubi/zeta1.properties,hdfs://hercules/apps/kyuubi/zeta2.properties",
    "spark.submit.pyFiles" ->
      "hdfs://hercules/apps/kyuubi/zeta1.py,hdfs://hercules/apps/kyuubi/zeta2.py")
  // scalastyle:on line.size.limit
}
