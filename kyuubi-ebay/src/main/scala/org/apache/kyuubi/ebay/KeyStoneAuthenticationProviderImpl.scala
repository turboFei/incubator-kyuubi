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

import java.io.IOException
import javax.security.sasl.AuthenticationException

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.http.client.methods.CloseableHttpResponse
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClients

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.{KyuubiConf, KyuubiEbayConf}
import org.apache.kyuubi.service.authentication.PasswdAuthenticationProvider

class KeyStoneAuthenticationProviderImpl(conf: KyuubiConf)
  extends PasswdAuthenticationProvider with Logging {
  private val endpoint: String = conf.get(KyuubiEbayConf.AUTHENTICATION_KEYSTONE_ENDPOINT)
  private val bodyTemplate: String =
    """
      |{
      | "auth":
      | {
      |   "passwordCredentials":
      |     {
      |       "username": "$username",
      |       "password": "$password"
      |     }
      |  }
      |}
      |""".stripMargin

  override def authenticate(user: String, password: String): Unit = {
    info(s"Using keystone to auth for $user")
    if (user == null || password == null) {
      warn(s"Invalid username or password: $user, $password")
      throw new AuthenticationException("Invalid username or password")
    }

    val maxRetries = 3
    var retryCnt = 0
    val retrySleepTimeInMs = 200
    var authPass = false

    while (retryCnt < maxRetries && !authPass) {
      try {
        doAuth(endpoint, bodyTemplate.replace("$username", user).replace("$password", password))
        authPass = true
      } catch {
        case aue: AuthenticationException =>
          warn(s"Authentication failed for user $user", aue)
          throw new AuthenticationException(s"Authentication failed for user $user", aue)
        case e: Throwable =>
          retryCnt += 1
          if (retryCnt >= maxRetries) {
            throw new AuthenticationException(s"Authentication failed for user $user", e)
          }
          try {
            Thread.sleep(retrySleepTimeInMs)
          } catch {
            case e: InterruptedException =>
              warn("Sleep interrupted", e)
          }
          warn(s"Call keystone api failed with exception, retry $retryCnt/$maxRetries", e)
      }
    }
  }

  @throws[Exception]
  private def parseAuth(resp: CloseableHttpResponse): Unit = {
    if (resp == null) throw new AuthenticationException("Fail to do request to keystone")
    val code = resp.getStatusLine.getStatusCode
    if (code >= 300 || code < 200) {
      val mapper = new ObjectMapper
      val node = mapper.readTree(resp.getEntity.getContent)
      if (node.get("error") != null) {
        throw new AuthenticationException(
          s"Fail to auth in keystone: ${node.get("error").get("message")}")
      } else {
        throw new AuthenticationException(s"Fail to auth in keystone: $code")
      }
    }
  }

  @throws[Exception]
  private def doAuth(url: String, body: String): Unit = {
    val httpClient = HttpClients.createDefault
    var response: CloseableHttpResponse = null
    try {
      val httpPost = new HttpPost(url)
      httpPost.addHeader("Content-Type", "application/json")
      val entity = new StringEntity(body)
      httpPost.setEntity(entity)
      response = httpClient.execute(httpPost)
      parseAuth(response)
    } finally {
      if (response != null) {
        try {
          response.close()
        } catch {
          case e: IOException =>
            error("Fail to close http request", e)
        }
      }
      if (httpClient != null) {
        try {
          httpClient.close()
        } catch {
          case e: IOException =>
            error("Fail to close httpclient", e)
        }
      }
    }
  }
}
