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

import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import javax.security.sasl.AuthenticationException

import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet}

import org.apache.kyuubi.KyuubiException
import org.apache.kyuubi.config.{KyuubiConf, KyuubiEbayConf}
import org.apache.kyuubi.ebay.carmel.gateway.endpoint.QueueInfo
import org.apache.kyuubi.service.AbstractService
import org.apache.kyuubi.util.ThreadUtils

class BdpServiceAccountMappingCacheManager(name: String) extends AbstractService(name) {
  def this() = this(classOf[BdpServiceAccountMappingCacheManager].getSimpleName)

  case class UserQueuesCache(cacheTime: Long, queues: Seq[QueueInfo])

  import HttpClientUtils._

  private val bdpBatchMappingLoader =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("bdp-batch-mapping-loader")
  private var batchMappingLoadInterval: Long = _

  private var serviceAccountMappingCache = new ConcurrentHashMap[String, Set[String]]().asScala

  private var bdpUrl: String = _
  private var bdpDefaultCluster: String = _
  private var userQueuesRefreshInterval: Long = _
  private var bdpUserQueuesCache: LoadingCache[(String, Option[String]), UserQueuesCache] = _

  override def initialize(conf: KyuubiConf): Unit = {
    bdpUrl = conf.get(KyuubiEbayConf.ACCESS_BDP_URL)
    bdpDefaultCluster = conf.get(KyuubiEbayConf.ACCESS_BDP_DEFAULT_CLUSTER)
    userQueuesRefreshInterval = conf.get(KyuubiEbayConf.ACCESS_BDP_QUEUE_REFRESH_INTERVAL)
    bdpUserQueuesCache = CacheBuilder.newBuilder()
      .maximumSize(conf.get(KyuubiEbayConf.ACCESS_BDP_QUEUE_CACHE_MAX))
      .build(new CacheLoader[(String, Option[String]), UserQueuesCache] {
        override def load(key: (String, Option[String])): UserQueuesCache = {
          UserQueuesCache(System.currentTimeMillis(), getUserQueuesFromBdp(key._1, key._2))
        }
      })
    batchMappingLoadInterval =
      conf.get(KyuubiEbayConf.AUTHENTICATION_BATCH_ACCOUNT_LOAD_ALL_INTERVAL)
    super.initialize(conf)
  }

  override def start(): Unit = {
    startBatchMappingLoader()
    super.start()
    BdpServiceAccountMappingCacheManager.bdpServiceAccountMappingCacheMgr = this
  }

  override def stop(): Unit = {
    BdpServiceAccountMappingCacheManager.bdpServiceAccountMappingCacheMgr = null
    super.stop()
    ThreadUtils.shutdown(bdpBatchMappingLoader)
  }

  def updateServiceAccountMappingCache(serviceAccount: String, batchAccounts: Set[String]): Unit =
    synchronized {
      val isNewOne = !serviceAccountMappingCache.contains(serviceAccount)
      if (!isNewOne || batchAccounts.nonEmpty) {
        serviceAccountMappingCache.update(serviceAccount, batchAccounts)
        if (isNewOne) {
          info(s"Added $serviceAccount -> $batchAccounts into service account mapping cache," +
            s" current size is ${serviceAccountMappingCache.size}")
        }
      }
    }

  def getServiceAccountBatchAccountsFromCache(serviceAccount: String): Set[String] = {
    serviceAccountMappingCache.get(serviceAccount).getOrElse(Set.empty)
  }

  private def parseServiceAccountMappings(resp: CloseableHttpResponse): Map[String, Set[String]] = {
    if (resp == null) throw new AuthenticationException(
      "Fail to do request to load all batch account mappings")
    val code = resp.getStatusLine.getStatusCode
    val mapper = new ObjectMapper
    val node = mapper.readTree(resp.getEntity.getContent)
    if (code >= 300 || code < 200) {
      if (node.get("error") != null) {
        throw new AuthenticationException(
          s"Fail to load all batch account mappings: ${node.get("error").get("message")}")
      } else {
        throw new AuthenticationException(s"Fail to load all batch account mappings: $code")
      }
    }

    val result = node.get("result")
    (0 until result.size()).map { i =>
      val mappingNode = result.get(i)
      val serviceAccount = mappingNode.get("serviceAccount").textValue()
      val batchAccountsNode = mappingNode.get("batchAccounts")
      val batchAccountSet = (0 until batchAccountsNode.size()).map { i =>
        batchAccountsNode.get(i).textValue()
      }.toSet
      serviceAccount -> batchAccountSet
    }.filter(_._2.nonEmpty).toMap
  }

  private def startBatchMappingLoader(): Unit = {
    val loadTask = new Runnable {
      override def run(): Unit = {
        try {
          val httpGet = new HttpGet(s"$bdpUrl/product/batch/service-account-mappings")
          withHttpResponse(httpGet) { response =>
            val newServiceAccountMappings = new ConcurrentHashMap[String, Set[String]]().asScala
            parseServiceAccountMappings(response).foreach { case (sa, baSet) =>
              newServiceAccountMappings.put(sa, baSet)
            }
            if (newServiceAccountMappings.nonEmpty) {
              serviceAccountMappingCache = newServiceAccountMappings
            }
            info(s"Loaded all service account mappings, total ${serviceAccountMappingCache.size}.")
          }
        } catch {
          case e: Throwable => error("Error loading all batch account mappings", e)
        }
      }
    }

    bdpBatchMappingLoader.scheduleWithFixedDelay(
      loadTask,
      0,
      batchMappingLoadInterval,
      TimeUnit.MILLISECONDS)
  }

  def getUserQueues(user: String, clusterOpt: Option[String]): Seq[QueueInfo] = {
    val queuesCache = bdpUserQueuesCache.get((user, clusterOpt))
    val currentTime = System.currentTimeMillis()
    if (currentTime - queuesCache.cacheTime < userQueuesRefreshInterval) {
      queuesCache.queues
    } else {
      val latestUserQueues = getUserQueuesFromBdp(user, clusterOpt)
      bdpUserQueuesCache.put((user, clusterOpt), UserQueuesCache(currentTime, latestUserQueues))
      latestUserQueues
    }
  }

  private def getUserQueuesFromBdp(user: String, clusterOpt: Option[String]): Seq[QueueInfo] = {
    val cluster = clusterOpt.getOrElse(bdpDefaultCluster)
    val httpGet = new HttpGet(s"$bdpUrl/product/queue/user/$cluster/$user")
    withHttpResponse(httpGet) { response =>
      parseQueues(response)
    }
  }

  private def parseQueues(resp: CloseableHttpResponse): Seq[QueueInfo] = {
    if (resp == null) throw new KyuubiException(
      "Fail to do request to get user queues")
    val code = resp.getStatusLine.getStatusCode
    val mapper = new ObjectMapper
    val node = mapper.readTree(resp.getEntity.getContent)
    if (code >= 300 || code < 200) {
      if (node.get("error") != null) {
        throw new KyuubiException(
          s"Fail to do request to get user queues: ${node.get("error").get("message")}")
      } else {
        throw new KyuubiException(s"Fail to do request to get user queues: $code")
      }
    }

    val result = node.get("result")
    (0 until result.size()).map { i =>
      val mappingNode = result.get(i)
      val queueName = mappingNode.get("queueName").textValue()
      val defaultQueue = mappingNode.get("defaultQueue").booleanValue()
      new QueueInfo(queueName, defaultQueue)
    }
  }
}

object BdpServiceAccountMappingCacheManager {
  private var bdpServiceAccountMappingCacheMgr: BdpServiceAccountMappingCacheManager = _

  def getBdpServiceAccountMappingCacheMgr: Option[BdpServiceAccountMappingCacheManager] = {
    Option(bdpServiceAccountMappingCacheMgr)
  }
}
