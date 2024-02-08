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
import java.util.concurrent.TimeUnit

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}

import org.apache.kyuubi.{Logging, Utils}
import org.apache.kyuubi.config.{KyuubiConf, KyuubiEbayConf}

/** Copied from org.apache.kyuubi.session.FileSessionConfAdvisor */
object TessFileSessionConfCache extends Logging {
  private val reloadInterval: Long =
    KyuubiEbayConf._kyuubiConf.get(KyuubiConf.SESSION_CONF_FILE_RELOAD_INTERVAL)

  private def loadKyuubiConf(propsFile: Option[File]): KyuubiConf = {
    propsFile match {
      case None =>
        error("File not found: $KYUUBI_CONF_DIR/" + propsFile)
        KyuubiConf(false)
      case Some(_) =>
        val conf = KyuubiConf(false)
        Utils.getPropertiesFromFile(propsFile).foreach { case (k, v) =>
          conf.set(k, v)
        }
        conf
    }
  }

  private lazy val sessionTessContextConfCache: LoadingCache[String, (KyuubiConf, KyuubiConf)] =
    CacheBuilder.newBuilder()
      .expireAfterWrite(
        reloadInterval,
        TimeUnit.MILLISECONDS)
      .build(new CacheLoader[String, (KyuubiConf, KyuubiConf)] {
        override def load(context: String): (KyuubiConf, KyuubiConf) = {
          val defaultsPropsFile =
            Utils.getPropertiesFile(s"kyuubi-session-tess-$context-defaults.conf")
          val overwritePropsFile =
            Utils.getPropertiesFile(s"kyuubi-session-tess-$context-overwrite.conf")
              // for backward compatibility
              .orElse(Utils.getPropertiesFile(s"kyuubi-session-tess-$context.conf"))
          loadKyuubiConf(defaultsPropsFile) -> loadKyuubiConf(overwritePropsFile)
        }
      })

  def getTessContextSessionConf(
      context: String,
      namespace: String = null): (Map[String, String], Map[String, String]) = {
    val (defaultConf, overwriteConf) = sessionTessContextConfCache.get(context)
    defaultConf.getUserDefaults(namespace).getAll -> overwriteConf.getUserDefaults(namespace).getAll
  }
}
