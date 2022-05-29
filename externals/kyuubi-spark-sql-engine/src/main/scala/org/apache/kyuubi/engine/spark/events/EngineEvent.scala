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

package org.apache.kyuubi.engine.spark.events

import java.util.Date

import org.apache.spark.scheduler.SparkListenerEvent

import org.apache.kyuubi.Utils
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.engine.spark.SparkSQLEngine
import org.apache.kyuubi.events.KyuubiEvent
import org.apache.kyuubi.service.ServiceState

/**
 * @param applicationId application id a.k.a, the unique id for engine
 * @param applicationName the application name
 * @param owner the application user
 * @param shareLevel the share level for this engine
 * @param connectionUrl the jdbc connection string
 * @param master the master type, yarn, k8s, local etc.
 * @param sparkVersion short version of spark distribution
 * @param webUrl the tracking url of this engine
 * @param startTime start time
 * @param endTime end time
 * @param state the engine state
 * @param diagnostic caught exceptions if any
 * @param settings collection of all configurations of spark and kyuubi
 */
case class EngineEvent(
    applicationId: String,
    attemptId: Option[String],
    applicationName: String,
    owner: String,
    shareLevel: String,
    connectionUrl: String,
    master: String,
    sparkVersion: String,
    webUrl: String,
    startTime: Long,
    endTime: Long,
    state: Int,
    diagnostic: String,
    settings: Map[String, String]) extends KyuubiEvent with SparkListenerEvent {

  override lazy val partitions: Seq[(String, String)] =
    ("day", Utils.getDateFromTimestamp(startTime)) :: Nil

  override def toString: String = {
    // need to consider deploy mode and cluster to get core and mem
    val driverCores = settings.getOrElse("spark.driver.cores", 1)
    val driverMemory = settings.getOrElse("spark.driver.memory", "1g")
    val executorCore = settings.getOrElse("spark.executor.cores", 2)
    val executorMemory = settings.getOrElse("spark.executor.memory", "1g")
    val dae = settings.getOrElse("spark.dynamicAllocation.enabled", "false").toBoolean
    val maxExecutors =
      if (dae) {
        settings.getOrElse("spark.dynamicAllocation.maxExecutors", Int.MaxValue)
      } else {
        settings.getOrElse("spark.executor.instances", 2)
      }
    val tags = settings.getOrElse(
      "spark.yarn.tags",
      settings.getOrElse("spark.kubernetes.driver.label.kyuubi_unique_tag", ""))
    s"""
       |    Spark application name: $applicationName
       |          application ID:  $applicationId
       |          application tags: $tags
       |          application web UI: $webUrl
       |          master: $master
       |          version: $sparkVersion
       |          driver: [cpu: $driverCores, mem: $driverMemory]
       |          executor: [cpu: $executorCore, mem: $executorMemory, maxNum: $maxExecutors]
       |    Start time: ${new Date(startTime)}
       |    ${if (endTime != -1L) "End time: " + new Date(endTime) else ""}
       |    User: $owner (shared mode: $shareLevel)
       |    State: ${ServiceState(state)}
       |    ${if (diagnostic.nonEmpty) "Diagnostic: " + diagnostic else ""}""".stripMargin
  }
}

object EngineEvent {

  def apply(engine: SparkSQLEngine): EngineEvent = {
    val sc = engine.spark.sparkContext
    val webUrl = sc.getConf.getOption(
      "spark.org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter.param.PROXY_URI_BASES")
      .orElse(sc.uiWebUrl).getOrElse("")
    val connectionUrl =
      if (engine.getServiceState.equals(ServiceState.LATENT)) {
        null
      } else {
        engine.frontendServices.head.connectionUrl
      }
    new EngineEvent(
      sc.applicationId,
      sc.applicationAttemptId,
      sc.appName,
      sc.sparkUser,
      engine.getConf.get(ENGINE_SHARE_LEVEL),
      connectionUrl,
      sc.master,
      sc.version,
      webUrl,
      sc.startTime,
      endTime = -1L,
      state = engine.getServiceState.id,
      diagnostic = "",
      sc.getConf.getAll.toMap ++ engine.getConf.getAll)
  }
}
