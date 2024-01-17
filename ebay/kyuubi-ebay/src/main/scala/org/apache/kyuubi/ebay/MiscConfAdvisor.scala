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

import org.apache.kyuubi.config.{KyuubiConf, KyuubiEbayConf}
import org.apache.kyuubi.plugin.SessionConfAdvisor

class MiscConfAdvisor extends SessionConfAdvisor {

  override def getConfOverlay(
      user: String,
      sessionConf: JMap[String, String]): JMap[String, String] = {
    (getBigResultSetConfOverlay(sessionConf) ++ getResultMaxRowsConfOverlay(sessionConf)).asJava
  }

  def getBigResultSetConfOverlay(sessionConf: JMap[String, String]): Map[String, String] = {
    if ("true".equalsIgnoreCase(sessionConf.get(KyuubiConf.OPERATION_RESULT_SAVE_TO_FILE.key))) {
      Map(KyuubiConf.OPERATION_INCREMENTAL_COLLECT.key -> "false")
    } else {
      Map.empty[String, String]
    }
  }

  def getResultMaxRowsConfOverlay(sessionConf: JMap[String, String]): Map[String, String] = {
    if (sessionConf.get(KyuubiConf.OPERATION_RESULT_MAX_ROWS.key) != null) {
      return Map.empty[String, String]
    }
    sessionConf.asScala.get(KyuubiEbayConf.EBAY_OPERATION_MAX_RESULT_COUNT.key).map { maxRows =>
      Map(KyuubiConf.OPERATION_RESULT_MAX_ROWS.key -> maxRows)
    }.getOrElse {
      Map.empty[String, String]
    }
  }
}
