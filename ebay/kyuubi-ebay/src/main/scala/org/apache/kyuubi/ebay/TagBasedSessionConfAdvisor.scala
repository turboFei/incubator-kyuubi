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

import java.io.File
import java.util.{Map => JMap}
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._

import org.apache.kyuubi.{Logging, Utils}
import org.apache.kyuubi.config.{KyuubiConf, KyuubiEbayConf}
import org.apache.kyuubi.config.KyuubiConf.KYUUBI_HOME
import org.apache.kyuubi.plugin.SessionConfAdvisor
import org.apache.kyuubi.util.ThreadUtils

class TagBasedSessionConfAdvisor extends SessionConfAdvisor with Logging {
  import TagBasedSessionConfAdvisor._

  private val kyuubiConf = KyuubiConf()
  private val tagConfFile = kyuubiConf.getOption(SESSION_TAG_CONF_FILE_KEY)
    .getOrElse(DEFAULT_SESSION_TAG_CONF_FILE)
  private val refreshInterval = kyuubiConf.getOption(SESSION_TAG_REFRESH_INTERVAL_KEY)
    .map(_.toLong).getOrElse(DEFAULT_SESSION_TAG_REFRESH_INTERVAL)

  @volatile
  private var tagConf: KyuubiConf = KyuubiConf(false)

  private def loadTagConf(env: Map[String, String] = sys.env): Unit = {
    try {
      val configFile = env.get(KyuubiConf.KYUUBI_CONF_DIR)
        .orElse(env.get(KYUUBI_HOME).map(_ + File.separator + "conf"))
        .map(d => new File(d + File.separator + tagConfFile))
        .filter(_.exists())
        .orElse {
          Option(getClass.getClassLoader.getResource(tagConfFile)).map { url =>
            new File(url.getFile)
          }.filter(_.exists())
        }
      val _tagConf = KyuubiConf(false)
      Utils.getPropertiesFromFile(configFile).foreach { case (k, v) =>
        _tagConf.set(k, v)
      }
      tagConf = _tagConf
    } catch {
      case e: Exception => error("Error loading tag conf", e)
    }
  }

  loadTagConf()

  private val tagConfReNewer =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("tag-conf-renewer")

  tagConfReNewer.scheduleWithFixedDelay(
    () => loadTagConf(),
    refreshInterval,
    refreshInterval,
    TimeUnit.MILLISECONDS)

  override def getConfOverlay(
      user: String,
      sessionConf: JMap[String, String]): JMap[String, String] = {
    val sessionTag = sessionConf.asScala.get(SESSION_TAG_KEY).getOrElse(KYUUBI_DEFAULT_TAG)
    val tagLevelConfOverlay = KyuubiEbayConf.getTagConfOnly(tagConf, sessionTag)
    val serviceOverwriteConfOverlay = KyuubiEbayConf.getTagConfOnly(tagConf, KYUUBI_OVERWRITE_TAG)
    (tagLevelConfOverlay ++ serviceOverwriteConfOverlay).asJava
  }
}

object TagBasedSessionConfAdvisor {
  val SESSION_TAG_KEY = "kyuubi.session.tag"
  val SESSION_TAG_CONF_FILE_KEY = "kyuubi.session.tag.conf.file"
  val SESSION_TAG_REFRESH_INTERVAL_KEY = "kyuubi.session.tag.refresh.interval"
  val DEFAULT_SESSION_TAG_CONF_FILE = KyuubiConf.KYUUBI_CONF_FILE_NAME + ".tag"
  val DEFAULT_SESSION_TAG_REFRESH_INTERVAL = 600 * 1000L

  // for kyuubi service side conf
  val KYUUBI_DEFAULT_TAG = "kyuubi_default"
  val KYUUBI_OVERWRITE_TAG = "kyuubi_overwrite"
}
