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
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}

import org.apache.kyuubi.{Logging, Utils}
import org.apache.kyuubi.config.{KyuubiConf, KyuubiEbayConf}

/** Copied from org.apache.kyuubi.session.FileSessionConfAdvisor */
object TessFileSessionConfCache extends Logging {
  private val reloadInterval: Long =
    KyuubiEbayConf._kyuubiConf.get(KyuubiConf.SESSION_CONF_FILE_RELOAD_INTERVAL)
  private lazy val sessionTessContextConfCache: LoadingCache[String, JMap[String, String]] =
    CacheBuilder.newBuilder()
      .expireAfterWrite(
        reloadInterval,
        TimeUnit.MILLISECONDS)
      .build(new CacheLoader[String, JMap[String, String]] {
        override def load(context: String): JMap[String, String] = {
          val propsFile = Utils.getPropertiesFile(s"kyuubi-session-tess-$context.conf")
          propsFile match {
            case None =>
              error("File not found: $KYUUBI_CONF_DIR/" + s"kyuubi-session-tess-$context.conf")
              Collections.emptyMap()
            case Some(_) =>
              Utils.getPropertiesFromFile(propsFile).asJava
          }
        }
      })

  def getTessContextSessionConf(context: String): Map[String, String] = {
    sessionTessContextConfCache.get(context).asScala.toMap
  }
}
