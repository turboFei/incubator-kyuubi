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

package org.apache.kyuubi.ebay.server.events

import java.io.{FileNotFoundException, IOException}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.{KyuubiConf, KyuubiEbayConf}
import org.apache.kyuubi.elasticsearch.shade.org.apache.http.auth.{AuthScope, Credentials, UsernamePasswordCredentials}
import org.apache.kyuubi.elasticsearch.shade.org.apache.http.client.CredentialsProvider

class FileBasedCredentialsProvider(conf: KyuubiConf) extends CredentialsProvider
  with Logging {
  val keyFilePath = Paths.get(conf.get(KyuubiEbayConf.ELASTIC_SEARCH_CREDENTIAL_FILE))
  val refreshInterval = conf.get(KyuubiEbayConf.ELASTIC_SEARCH_CREDENTIAL_REFRESH_INTERVAL)
  val maxAttempts = conf.get(KyuubiEbayConf.ELASTIC_SEARCH_CREDENTIAL_MAX_ATTEMPTS)
  val retryWait = conf.get(KyuubiEbayConf.ELASTIC_SEARCH_CREDENTIAL_RETRY_WAIT)

  if (!keyFilePath.toFile.exists()) {
    throw new FileNotFoundException(s"The elastic search key file: $keyFilePath does not exists")
  }

  private var lastUpdateTime: Long = _
  private var credentials: Credentials = _

  private def needRefresh(): Boolean = {
    System.currentTimeMillis() - lastUpdateTime > refreshInterval
  }

  private def refresh(): Unit = {
    var attempt = 0
    var shouldRetry = true
    while (attempt < maxAttempts && shouldRetry) {
      try {
        refreshOnce()
        lastUpdateTime = System.currentTimeMillis()
        shouldRetry = false
      } catch {
        case e: IOException if attempt < maxAttempts =>
          warn(
            s"Failed to read elastic search credentials from file after" +
              s" $attempt/$maxAttempts times, retrying",
            e)
          Thread.sleep(retryWait)
          shouldRetry = true
        case e: Throwable =>
          error(s"Failed to read elastic search credentials from file", e)
          throw e
      } finally {
        attempt += 1
      }
    }
  }

  private def refreshOnce(): Unit = {
    val userPassword = new String(Files.readAllBytes(keyFilePath), StandardCharsets.UTF_8).trim
    userPassword.split(":", 2) match {
      case Array(user, password) =>
        credentials = new UsernamePasswordCredentials(user.trim, password.trim)
      case _ =>
        throw new IllegalArgumentException("The user password format should be <USER>:<PASSWORD>")
    }
  }

  override def getCredentials(authScope: AuthScope): Credentials = {
    if (needRefresh()) {
      refresh()
    }
    credentials
  }

  override def setCredentials(authScope: AuthScope, credentials: Credentials): Unit = {}

  override def clear(): Unit = {}
}
