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

package org.apache.kyuubi.engine.spark

import scala.collection.mutable.ArrayBuffer

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.operation.SubmitApplicationProtocol
import org.apache.kyuubi.operation.log.OperationLog

class SparkSubmitProcessBuilder(
    override val proxyUser: String,
    override val conf: KyuubiConf,
    val submitCommand: SubmitApplicationProtocol,
    override val extraEngineLog: Option[OperationLog] = None)
  extends SparkProcessBuilder(proxyUser, conf, extraEngineLog) {
  import SparkProcessBuilder._

  private var appIdAndTrackingUrl: Option[(String, String)] = None

  override def mainClass: String = submitCommand.mainClass

  override def mainResource: Option[String] = Option(submitCommand.resource)

  override protected def commands: Array[String] = {
    val buffer = new ArrayBuffer[String]()
    buffer += executable
    buffer += CLASS
    buffer += mainClass

    val allConf = submitCommand.conf ++ procConf()
    allConf.foreach { case (k, v) =>
      buffer += CONF
      buffer += s"$k=$v"
    }

    buffer += PROXY_USER
    buffer += proxyUser

    mainResource.foreach { r => buffer += r }

    submitCommand.args.foreach { arg => buffer += arg }

    buffer.toArray
  }

  override protected def module: String = "kyuubi-spark-submit"

  val YARN_APP_TRACKING_REGEX =
    ".*tracking URL: (http[:/a-zA-Z0-9._-]*)(application_\\d+_\\d+).*".r("urlPrefix", "appId")
  val NORMAL_APP_TRACKING_REGEX = ".*Application Id: ([a-zA-Z0-9_-]*).*".r("appId")

  def getAppIdAndTrackingUrl(): Option[(String, String)] = appIdAndTrackingUrl

  override protected def captureLogLine(line: String): Unit = {
    if (appIdAndTrackingUrl.isEmpty) {
      line match {
        case YARN_APP_TRACKING_REGEX(urlPrefix, appId) =>
          appIdAndTrackingUrl = Some((appId, urlPrefix + appId))
        case NORMAL_APP_TRACKING_REGEX(appId) =>
          appIdAndTrackingUrl = Some((appId, ""))
        case _ =>
      }
    }
  }
}
