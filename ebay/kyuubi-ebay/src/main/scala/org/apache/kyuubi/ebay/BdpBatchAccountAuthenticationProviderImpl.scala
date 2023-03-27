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

import javax.security.sasl.AuthenticationException

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.http.client.methods.HttpGet

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.{KyuubiConf, KyuubiEbayConf}
import org.apache.kyuubi.service.authentication.BatchAccountAuthenticationProvider

class BdpBatchAccountAuthenticationProviderImpl(conf: KyuubiConf)
  extends BatchAccountAuthenticationProvider with Logging {
  import HttpClientUtils._

  private val bdpUrl = conf.get(KyuubiEbayConf.ACCESS_BDP_URL)
  private val endpoint = bdpUrl + "/product/batch/$serviceAccount/service/mapping"

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
        doAuth(serviceAccount, batchAccount)
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

  private def getServiceAccountMappingBatchAccounts(serviceAccount: String): Set[String] = {
    try {
      val httpGet = new HttpGet(endpoint.replace("$serviceAccount", serviceAccount))
      withHttpResponse(httpGet) { response =>
        if (response == null) throw new AuthenticationException(
          s"Fail to do request to get the batch account mapping of $serviceAccount")
        val code = response.getStatusLine.getStatusCode
        val mapper = new ObjectMapper
        val node = mapper.readTree(response.getEntity.getContent)
        if (code >= 300 || code < 200) {
          if (node.get("error") != null) {
            throw new AuthenticationException(
              s"Fail to auth with $endpoint: ${node.get("error").get("message")}")
          } else {
            throw new AuthenticationException(s"Fail to auth with $endpoint: $code")
          }
        }

        val result = node.get("result")
        val batchAccountMapping = (0 until result.size()).map(i => result.get(i).textValue()).toSet
        BdpServiceAccountMappingCacheManager.getBdpServiceAccountMappingCacheMgr.map(
          _.updateServiceAccountMappingCache(serviceAccount, batchAccountMapping))
        batchAccountMapping
      }
    } catch {
      case e: Throwable =>
        error(s"Error getting service account batch account mapping for $serviceAccount", e)
        BdpServiceAccountMappingCacheManager.getBdpServiceAccountMappingCacheMgr.map(
          _.getServiceAccountBatchAccountsFromCache(serviceAccount)).getOrElse(throw e)
    }
  }

  @throws[Exception]
  private def doAuth(serviceAccount: String, batchAccount: String): Unit = {
    val batchAccounts = getServiceAccountMappingBatchAccounts(serviceAccount)
    if (!batchAccounts.contains(batchAccount)) {
      throw new AuthenticationException(
        s"Service account:$serviceAccount is not allowed to impersonate $batchAccount")
    }
  }
}
