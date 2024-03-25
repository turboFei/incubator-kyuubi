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

import java.util.{Map => JMap}

import scala.collection.JavaConverters._

import org.apache.kyuubi.Logging
import org.apache.kyuubi.plugin.SessionConfAdvisor

class ChainedSessionConfAdvisor extends SessionConfAdvisor with Logging {
  val sessionConfAdvisorChain =
    List(
      new SparkMajorVersionAdvisor(),
      new TagBasedSessionConfAdvisor(),
      new HadoopPathQualifyConfAdvisor(),
      new TessConfAdvisor(),
      new MiscConfAdvisor(),
      new PandaEnvConfAdvisor(),
      new PySparkConfAdvisor())

  override def getConfOverlay(
      user: String,
      sessionConf: JMap[String, String]): JMap[String, String] = {
    var baseConf = sessionConf.asScala
    var confOverlay: JMap[String, String] = null
    sessionConfAdvisorChain.foreach { advisor =>
      val subConfOverlay = advisor.getConfOverlay(
        user,
        baseConf.asJava)
      if (subConfOverlay != null) {
        baseConf = baseConf ++ subConfOverlay.asScala
        if (confOverlay == null) {
          confOverlay = subConfOverlay
        } else {
          confOverlay = (confOverlay.asScala ++ subConfOverlay.asScala).asJava
        }
      } else {
        warn(s"the server plugin[${advisor.getClass.getSimpleName}]" +
          s" return null value for user: $user, ignore it")
      }
    }
    confOverlay
  }
}
