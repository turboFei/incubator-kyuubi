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

package org.apache.kyuubi.engine

import scala.collection.JavaConverters._

import org.apache.hadoop.yarn.client.api.YarnClient

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.ApplicationOperation._
import org.apache.kyuubi.util.KyuubiHadoopUtils

class YarnApplicationOperation extends ApplicationOperation with Logging {

  @volatile private var yarnClient: YarnClient = _

  override def initialize(conf: KyuubiConf): Unit = {
    val yarnConf = KyuubiHadoopUtils.newYarnConfiguration(conf)
    // YarnClient is thread-safe
    val c = YarnClient.createYarnClient()
    c.init(yarnConf)
    c.start()
    yarnClient = c
    info(s"Successfully initialized yarn client: ${c.getServiceState}")
  }

  override def isSupported(clusterManager: Option[String]): Boolean = {
    yarnClient != null && clusterManager.nonEmpty && "yarn".equalsIgnoreCase(clusterManager.get)
  }

  override def killApplicationByTag(tag: String): KillResponse = {
    if (yarnClient != null) {
      try {
        val reports = yarnClient.getApplications(null, null, Set(tag).asJava)
        if (reports.isEmpty) {
          (false, NOT_FOUND)
        } else {
          try {
            val applicationId = reports.get(0).getApplicationId
            yarnClient.killApplication(applicationId)
            (true, s"Succeeded to terminate: $applicationId with $tag")
          } catch {
            case e: Exception =>
              (false, s"Failed to terminate application with $tag, due to ${e.getMessage}")
          }
        }
      } catch {
        case e: Exception =>
          (
            false,
            s"Failed to get while terminating application with tag $tag," +
              s" due to ${e.getMessage}")
      }
    } else {
      throw new IllegalStateException("Methods initialize and isSupported must be called ahead")
    }
  }

  override def getApplicationInfoByTag(tag: String): Map[String, String] = {
    if (yarnClient != null) {
      debug(s"Getting application info from Yarn cluster by $tag tag")
      val reports = yarnClient.getApplications(null, null, Set(tag).asJava)
      if (reports.isEmpty) {
        debug(s"Application with tag $tag not found")
        null
      } else {
        val report = reports.get(0)
        val res = Map(
          APP_ID_KEY -> report.getApplicationId.toString,
          APP_NAME_KEY -> report.getName,
          APP_STATE_KEY -> report.getYarnApplicationState.toString,
          APP_URL_KEY -> report.getTrackingUrl,
          APP_ERROR_KEY -> report.getDiagnostics)
        debug(s"Successfully got application info by $tag: " + res.mkString(", "))
        res
      }
    } else {
      throw new IllegalStateException("Methods initialize and isSupported must be called ahead")
    }
  }

  override def stop(): Unit = {
    if (yarnClient != null) {
      try {
        yarnClient.stop()
      } catch {
        case e: Exception => error(e.getMessage)
      }
    }
  }
}
