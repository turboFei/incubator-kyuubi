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

package org.apache.kyuubi.server.metadata

import java.util.concurrent.{ConcurrentHashMap, ThreadPoolExecutor, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._

import org.apache.kyuubi.{KyuubiException, Logging}
import org.apache.kyuubi.client.api.v1.dto.Batch
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.METADATA_MAX_AGE
import org.apache.kyuubi.engine.ApplicationOperation._
import org.apache.kyuubi.server.metadata.api.Metadata
import org.apache.kyuubi.service.CompositeService
import org.apache.kyuubi.session.SessionType
import org.apache.kyuubi.util.{ClassUtils, ThreadUtils}

class MetadataManager extends CompositeService("MetadataManager") {
  private var _metadataStore: MetadataStore = _

  private val identifierRequestsRetryRefs =
    new ConcurrentHashMap[String, MetadataRequestsRetryRef]()

  private val identifierRequestsRetryingCounts =
    new ConcurrentHashMap[String, AtomicInteger]()

  private val requestsRetryTrigger =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("metadata-requests-retry-trigger")

  private var requestsRetryExecutor: ThreadPoolExecutor = _

  private var maxMetadataRequestsRetryRefs: Int = _

  private val metadataCleaner =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("metadata-cleaner")

  override def initialize(conf: KyuubiConf): Unit = {
    _metadataStore = MetadataManager.createMetadataStore(conf)
    val retryExecutorNumThreads =
      conf.get(KyuubiConf.METADATA_REQUEST_RETRY_THREADS)
    requestsRetryExecutor = ThreadUtils.newDaemonFixedThreadPool(
      retryExecutorNumThreads,
      "metadata-requests-retry-executor")
    maxMetadataRequestsRetryRefs = conf.get(KyuubiConf.METADATA_REQUEST_RETRY_QUEUE_SIZE)
    super.initialize(conf)
  }

  override def start(): Unit = {
    super.start()
    startMetadataRequestsRetryTrigger()
    startMetadataCleaner()
  }

  override def stop(): Unit = {
    ThreadUtils.shutdown(requestsRetryTrigger)
    ThreadUtils.shutdown(requestsRetryExecutor)
    ThreadUtils.shutdown(metadataCleaner)
    _metadataStore.close()
    super.stop()
  }

  def insertMetadata(metadata: Metadata, retryOnError: Boolean = true): Unit = {
    try {
      _metadataStore.insertMetadata(metadata)
    } catch {
      case e: Throwable if retryOnError =>
        error(s"Error inserting metadata for session ${metadata.identifier}", e)
        addMetadataRetryRequest(InsertMetadata(metadata))
    }
  }

  def getBatch(batchId: String): Batch = {
    Option(getBatchSessionMetadata(batchId)).map(buildBatch).orNull
  }

  def getBatchSessionMetadata(batchId: String): Metadata = {
    Option(_metadataStore.getMetadata(batchId, true)).filter(
      _.sessionType == SessionType.BATCH).orNull
  }

  def getBatches(
      batchType: String,
      batchUser: String,
      batchState: String,
      createTime: Long,
      endTime: Long,
      from: Int,
      size: Int): Seq[Batch] = {
    _metadataStore.getMetadataList(
      SessionType.BATCH,
      batchType,
      batchUser,
      batchState,
      null,
      createTime,
      endTime,
      from,
      size,
      true).map(buildBatch)
  }

  def getBatchesRecoveryMetadata(
      state: String,
      kyuubiInstance: String,
      from: Int,
      size: Int): Seq[Metadata] = {
    _metadataStore.getMetadataList(
      SessionType.BATCH,
      null,
      null,
      state,
      kyuubiInstance,
      0,
      0,
      from,
      size,
      false)
  }

  def updateMetadata(metadata: Metadata, retryOnError: Boolean = true): Unit = {
    try {
      _metadataStore.updateMetadata(metadata)
    } catch {
      case e: Throwable if retryOnError =>
        error(s"Error updating metadata for session ${metadata.identifier}", e)
        addMetadataRetryRequest(UpdateMetadata(metadata))
    }
  }

  def cleanupMetadataById(batchId: String): Unit = {
    _metadataStore.cleanupMetadataByIdentifier(batchId)
  }

  private def buildBatch(batchMetadata: Metadata): Batch = {
    val batchAppInfo = Map(
      APP_ID_KEY -> Option(batchMetadata.engineId),
      APP_NAME_KEY -> Option(batchMetadata.engineName),
      APP_STATE_KEY -> Option(batchMetadata.engineState),
      APP_URL_KEY -> Option(batchMetadata.engineUrl),
      APP_ERROR_KEY -> batchMetadata.engineError)
      .filter(_._2.isDefined)
      .map(info => (info._1, info._2.get))

    new Batch(
      batchMetadata.identifier,
      batchMetadata.username,
      batchMetadata.engineType,
      batchMetadata.requestName,
      batchAppInfo.asJava,
      batchMetadata.kyuubiInstance,
      batchMetadata.state,
      batchMetadata.createTime,
      batchMetadata.endTime)
  }

  private def startMetadataCleaner(): Unit = {
    val cleanerEnabled = conf.get(KyuubiConf.METADATA_CLEANER_ENABLED)
    val stateMaxAge = conf.get(METADATA_MAX_AGE)

    if (cleanerEnabled) {
      val interval = conf.get(KyuubiConf.METADATA_CLEANER_INTERVAL)
      val cleanerTask: Runnable = () => {
        try {
          _metadataStore.cleanupMetadataByAge(stateMaxAge)
        } catch {
          case e: Throwable => error("Error cleaning up the metadata by age", e)
        }
      }

      metadataCleaner.scheduleWithFixedDelay(
        cleanerTask,
        interval,
        interval,
        TimeUnit.MILLISECONDS)
    }
  }

  def addMetadataRetryRequest(request: MetadataRequest): Unit = {
    if (identifierRequestsRetryRefs.size() > maxMetadataRequestsRetryRefs) {
      throw new KyuubiException(
        "The number of metadata requests retry instances exceeds the limitation:" +
          maxMetadataRequestsRetryRefs)
    }
    val identifier = request.metadata.identifier
    val ref = identifierRequestsRetryRefs.computeIfAbsent(
      identifier,
      identifier => {
        val ref = new MetadataRequestsRetryRef
        debug(s"Created MetadataRequestsRetryRef for session $identifier.")
        ref
      })
    ref.addRetryingMetadataRequest(request)
    identifierRequestsRetryRefs.putIfAbsent(identifier, ref)
  }

  def getMetadataRequestsRetryRef(identifier: String): MetadataRequestsRetryRef = {
    identifierRequestsRetryRefs.get(identifier)
  }

  def deRegisterRequestsRetryRef(identifier: String): Unit = {
    identifierRequestsRetryRefs.remove(identifier)
    identifierRequestsRetryingCounts.remove(identifier)
  }

  private def startMetadataRequestsRetryTrigger(): Unit = {
    val interval = conf.get(KyuubiConf.METADATA_REQUEST_RETRY_INTERVAL)
    val triggerTask = new Runnable {
      override def run(): Unit = {
        identifierRequestsRetryRefs.forEach { (id, ref) =>
          if (!ref.hasRemainingRequests()) {
            identifierRequestsRetryRefs.remove(id)
            identifierRequestsRetryingCounts.remove(id)
          } else {
            val retryingCount =
              identifierRequestsRetryingCounts.computeIfAbsent(id, _ => new AtomicInteger(0))

            if (retryingCount.get() == 0) {
              val retryTask = new Runnable {
                override def run(): Unit = {
                  try {
                    info(s"Retrying metadata requests for $id")
                    var request = ref.metadataRequests.peek()
                    while (request != null) {
                      request match {
                        case insert: InsertMetadata =>
                          insertMetadata(insert.metadata, retryOnError = false)

                        case update: UpdateMetadata =>
                          updateMetadata(update.metadata, retryOnError = false)

                        case _ =>
                      }
                      ref.metadataRequests.remove(request)
                      request = ref.metadataRequests.peek()
                    }
                  } catch {
                    case e: Throwable =>
                      error(s"Error retrying metadata requests for $id", e)
                  } finally {
                    retryingCount.decrementAndGet()
                  }
                }
              }

              try {
                retryingCount.incrementAndGet()
                requestsRetryExecutor.submit(retryTask)
              } catch {
                case e: Throwable =>
                  error(s"Error submitting metadata retry requests for $id", e)
                  retryingCount.decrementAndGet()
              }
            }

          }
        }
      }
    }
    requestsRetryTrigger.scheduleWithFixedDelay(
      triggerTask,
      interval,
      interval,
      TimeUnit.MILLISECONDS)
  }
}

object MetadataManager extends Logging {
  def createMetadataStore(conf: KyuubiConf): MetadataStore = {
    val className = conf.get(KyuubiConf.METADATA_STORE_CLASS)
    if (className.isEmpty) {
      throw new KyuubiException(
        s"${KyuubiConf.METADATA_STORE_CLASS.key} cannot be empty.")
    }
    ClassUtils.createInstance(className, classOf[MetadataStore], conf)
  }
}
