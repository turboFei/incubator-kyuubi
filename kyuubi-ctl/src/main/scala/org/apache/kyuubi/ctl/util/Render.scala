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
package org.apache.kyuubi.ctl.util

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import org.apache.kyuubi.client.api.v1.dto.{Batch, GetBatchesResponse}
import org.apache.kyuubi.ctl.util.DateTimeUtils._
import org.apache.kyuubi.ha.client.ServiceNodeInfo

private[ctl] object Render {

  def renderServiceNodesInfo(
      title: String,
      serviceNodeInfo: Seq[ServiceNodeInfo],
      verbose: Boolean): String = {
    val header = Seq("Namespace", "Host", "Port", "Version")
    val rows = serviceNodeInfo.sortBy(_.nodeName).map { sn =>
      Seq(sn.namespace, sn.host, sn.port.toString, sn.version.getOrElse(""))
    }
    Tabulator.format(title, header, rows, verbose)
  }

  def renderBatchListInfo(batchListInfo: GetBatchesResponse): String = {
    val title = s"Total number of batches: ${batchListInfo.getTotal}"
    val header =
      Seq(
        "Batch Id",
        "Type",
        "Name",
        "User",
        "State",
        "Cluster",
        "App Id",
        "App Url",
        "App State",
        "App Diagnostic",
        "Kyuubi Instance",
        "Create Time",
        "End Time")
    val rows = batchListInfo.getBatches.asScala.sortBy(_.getCreateTime).map { batch =>
      Seq(
        batch.getId,
        batch.getBatchType,
        batch.getName,
        batch.getUser,
        batch.getState,
        batch.getCluster,
        batch.getAppId,
        batch.getAppUrl,
        batch.getAppState,
        batch.getAppDiagnostic,
        batch.getKyuubiInstance,
        millisToDateString(batch.getCreateTime, "yyyy-MM-dd HH:mm:ss"),
        millisToDateString(batch.getEndTime, "yyyy-MM-dd HH:mm:ss"))
    }
    Tabulator.format(title, header, rows, true)
  }

  def renderBatchInfo(batch: Batch): String = {
    val batchInfo = ListBuffer[String]()

    batchInfo += "Batch Info"
    batchInfo += s"Batch Id: ${batch.getId}"
    batchInfo += s"Type: ${batch.getBatchType}"
    batchInfo += s"Name: ${batch.getName}"
    batchInfo += s"User: ${batch.getUser}"
    batchInfo += s"State: ${batch.getState}"
    Option(batch.getCluster).foreach { _ =>
      batchInfo += s"Cluster: ${batch.getCluster}"
    }
    Option(batch.getAppId).foreach { _ =>
      batchInfo += s"App Id: ${batch.getAppId}"
    }
    Option(batch.getAppUrl).foreach { _ =>
      batchInfo += s"App Url: ${batch.getAppUrl}"
    }
    Option(batch.getAppState).foreach { _ =>
      batchInfo += s"App State: ${batch.getAppState}"
    }
    Option(batch.getAppDiagnostic).filter(_.nonEmpty).foreach { _ =>
      batchInfo += s"App Diagnostic: ${batch.getAppDiagnostic}"
    }
    batchInfo += s"Kyuubi Instance: ${batch.getKyuubiInstance}"
    batchInfo += s"Create Time: ${millisToDateString(batch.getCreateTime, "yyyy-MM-dd HH:mm:ss")}"
    batchInfo += s"End Time: ${millisToDateString(batch.getEndTime, "yyyy-MM-dd HH:mm:ss")}"

    batchInfo.mkString("", "\n", "\t")
  }
}
