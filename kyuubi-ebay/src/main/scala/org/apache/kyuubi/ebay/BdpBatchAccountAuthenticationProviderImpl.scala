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
import java.security.cert.X509Certificate
import javax.net.ssl.{SSLContext, TrustManager, X509TrustManager}
import javax.security.sasl.AuthenticationException

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet}
import org.apache.http.impl.client.HttpClients

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.service.authentication.BatchAccountAuthenticationProvider

class BdpBatchAccountAuthenticationProviderImpl(conf: KyuubiConf)
  extends BatchAccountAuthenticationProvider with Logging {
  import BdpBatchAccountAuthenticationProviderImpl._
  private val endpoint: String = conf.getOption("kyuubi.authentication.batch.account.endpoint")
    .getOrElse("https://bdp.vip.ebay.com/product/batch/$serviceAccount/service/mapping?")

  override def authenticate(serviceAccount: String, batchAccount: String): Unit = {
    info(s"Using service account:$serviceAccount to auth for batch account:$batchAccount")
    if (serviceAccount == null || batchAccount == null) {
      warn(s"Invalid server account:$serviceAccount or batch account:$batchAccount")
      throw new AuthenticationException("Invalid service account or batch account")
    }

    val maxRetries = 3
    var retryCnt = 0
    val retrySleepTimeInMs = 200
    var authPass = false

    while (retryCnt < maxRetries && !authPass) {
      try {
        doAuth(endpoint, serviceAccount, batchAccount)
        authPass = true
        authPass = true
      } catch {
        case aue: AuthenticationException =>
          warn(s"Authentication failed for $serviceAccount/$batchAccount", aue)
          throw new AuthenticationException(
            s"Authentication failed for $serviceAccount/$batchAccount",
            aue)
        case e: Throwable =>
          retryCnt += 1
          if (retryCnt >= maxRetries) {
            throw new AuthenticationException(
              s"Authentication failed for $serviceAccount/$batchAccount",
              e)
          }
          try {
            Thread.sleep(retrySleepTimeInMs)
          } catch {
            case e: InterruptedException =>
              warn("Sleep interrupted", e)
          }
          warn(s"Call $endpoint api failed with exception, retry $retryCnt/$maxRetries", e)
      }
    }
  }

  @throws[Exception]
  private def parseAuth(
      resp: CloseableHttpResponse,
      serviceAccount: String,
      batchAccount: String): Unit = {
    if (resp == null) throw new AuthenticationException(
      s"Fail to do request verify  $serviceAccount/$batchAccount")
    val code = resp.getStatusLine.getStatusCode
    val mapper = new ObjectMapper
    val node = mapper.readTree(resp.getEntity.getContent)
    if (code >= 300 || code < 200) {
      if (node.get("error") != null) {
        throw new AuthenticationException(
          s"Fail to auth with $endpoint: ${node.get("error").get("message")}")
      } else {
        throw new AuthenticationException(s"Fail to auth with $endpoint: $code")
      }
    }

    val result = node.get("result")
    for (i <- (0 until result.size())) {
      if (batchAccount.equals(result.get(i).textValue())) {
        return
      }
    }
    throw new AuthenticationException(
      s"Service account:$serviceAccount is not allowed to impersonate $batchAccount")
  }

  @throws[Exception]
  private def doAuth(url: String, serviceAccount: String, batchAccount: String): Unit = {
    val httpClient = HttpClients.custom().setSSLContext(sslContext).build()
    var response: CloseableHttpResponse = null
    try {
      val httpGet = new HttpGet(url.replace("$serviceAccount", serviceAccount))
      response = httpClient.execute(httpGet)
      parseAuth(response, serviceAccount, batchAccount)
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

object BdpBatchAccountAuthenticationProviderImpl {
  val UNQUESTIONING_TRUST_MANAGER = Array[TrustManager](new X509TrustManager() {
    override def getAcceptedIssuers: Array[X509Certificate] = null
    override def checkClientTrusted(certs: Array[X509Certificate], authType: String): Unit = {}
    override def checkServerTrusted(certs: Array[X509Certificate], authType: String): Unit = {}
  })

  val sslContext = SSLContext.getInstance("SSL")
  sslContext.init(null, UNQUESTIONING_TRUST_MANAGER, null)
}
