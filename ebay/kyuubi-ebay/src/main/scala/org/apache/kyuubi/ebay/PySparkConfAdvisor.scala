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
import org.apache.kyuubi.config.{KyuubiConf, KyuubiEbayConf}
import org.apache.kyuubi.config.KyuubiConf.OperationLanguages
import org.apache.kyuubi.plugin.SessionConfAdvisor

class PySparkConfAdvisor extends SessionConfAdvisor with Logging {
  override def getConfOverlay(
      user: String,
      sessionConf: JMap[String, String]): JMap[String, String] = {
    if (KyuubiEbayConf.isKyuubiBatch(sessionConf.asScala.toMap)) {
      Map.empty[String, String].asJava
    } else {
      val opLanguage =
        sessionConf.getOrDefault(KyuubiConf.OPERATION_LANGUAGE.key, OperationLanguages.SQL.toString)
      if (OperationLanguages.PYTHON.toString.equalsIgnoreCase(opLanguage)) {
        Map(PySparkConfAdvisor.SPARK_IS_PYTHON -> "true").asJava
      } else {
        Map.empty[String, String].asJava
      }
    }
  }
}

object PySparkConfAdvisor {
  val SPARK_IS_PYTHON = "spark.yarn.isPython"
}
