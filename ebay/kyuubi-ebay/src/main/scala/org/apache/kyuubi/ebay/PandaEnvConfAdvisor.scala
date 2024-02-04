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
import org.apache.kyuubi.config.KyuubiEbayConf
import org.apache.kyuubi.plugin.SessionConfAdvisor

class PandaEnvConfAdvisor extends SessionConfAdvisor with Logging {
  import PandaEnvConfAdvisor._
  override def getConfOverlay(
      user: String,
      sessionConf: JMap[String, String]): JMap[String, String] = {
    val isBatch = KyuubiEbayConf.isKyuubiBatch(sessionConf.asScala.toMap)
    val sessionTag = sessionConf.get(KyuubiEbayConf.SESSION_TAG.key)

    val env = (isBatch, sessionTag) match {
      case (true, _) => SUBMIT_ENV_ETL // for batch, always etl env
      case (false, ETL_HANDLER_BEELINE_TAG) => SUBMIT_ENV_ETL
      case (false, ZETA_TAG) => SUBMIT_ENV_ZETA
      case _ => SUBMIT_ENV_KYUUBI
    }

    KyuubiEbayConf.confOverlayForSessionType(isBatch, Map(SPARK_SUBMIT_ENV -> env)).asJava
  }
}

object PandaEnvConfAdvisor {
  val SPARK_SUBMIT_ENV = "spark.submit.env"

  val SUBMIT_ENV_ETL = "etl"
  val SUBMIT_ENV_ZETA = "zeta"
  val SUBMIT_ENV_KYUUBI = "kyuubi"

  val ZETA_TAG = "zeta"

  val ETL_HANDLER_BEELINE_TAG = "etl-ttl-handler-kyuubi-beeline"
}
