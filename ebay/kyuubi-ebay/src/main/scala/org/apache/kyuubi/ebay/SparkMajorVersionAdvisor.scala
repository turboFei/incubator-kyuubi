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

import java.util.{Collections, Map => JMap}

import scala.collection.JavaConverters._

import org.apache.commons.lang3.StringUtils

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiEbayConf.SESSION_TAG

class SparkMajorVersionAdvisor extends TagBasedSessionConfAdvisor {
  import SparkMajorVersionAdvisor._

  override def getConfOverlay(
      user: String,
      sessionConf: JMap[String, String]): JMap[String, String] = {
    majorMinorVersion(sessionConf.get(SPARK_MAJOR_VERSION)).map { version =>
      super.getConfOverlay(
        user,
        (sessionConf.asScala ++ Map(SESSION_TAG.key -> version)).asJava)
    }.getOrElse(Collections.emptyMap())
  }
}

object SparkMajorVersionAdvisor extends Logging {
  final val SPARK_MAJOR_VERSION = "spark.binary.majorVersion"
  private val majorVersionRegex = """^(\d+)\.(\d+)\.(\d+)$""".r

  /**
   * Given a Spark version string, return the short version string.
   * E.g., for 3.4.1, return '3.4'.
   */
  def majorMinorVersion(version: String): Option[String] = {
    if (StringUtils.isBlank(version)) return None
    majorVersionRegex.findFirstMatchIn(version) match {
      case Some(m) => Some(s"${m.group(1)}.${m.group(2)}")
      case None =>
        warn(s"Tried to parse '$version' as $SPARK_MAJOR_VERSION," +
          s" but it could not find the major/minor version numbers.")
        None
    }
  }
}
