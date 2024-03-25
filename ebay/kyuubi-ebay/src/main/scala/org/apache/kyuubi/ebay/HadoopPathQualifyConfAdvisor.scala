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

import java.net.URI
import java.util.{HashMap => JHashMap, Map => JMap}
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.{KyuubiConf, KyuubiEbayConf}
import org.apache.kyuubi.config.KyuubiEbayConf.SESSION_CLUSTER
import org.apache.kyuubi.plugin.SessionConfAdvisor
import org.apache.kyuubi.util.KyuubiHadoopUtils

class HadoopPathQualifyConfAdvisor extends SessionConfAdvisor with Logging {
  import HadoopPathQualifyConfAdvisor._

  override def getConfOverlay(
      user: String,
      sessionConf: JMap[String, String]): JMap[String, String] = {
    val sessionCluster = sessionConf.asScala.get(SESSION_CLUSTER.key)
    val pathQualifyEnabled = sessionConf.asScala.get(KyuubiEbayConf.HADOOP_PATH_QUALIFY_ENABLED.key)
      .map(_.trim.equalsIgnoreCase("true"))
      .getOrElse(kyuubiConf.get(KyuubiEbayConf.HADOOP_PATH_QUALIFY_ENABLED))
    if (pathQualifyEnabled) {
      val qualifiedConfigs = new JHashMap[String, String]()
      pathQualifyConfigs.foreach { config =>
        Option(sessionConf.get(config)).map(_.split(",").map(_.trim)).filter(_.nonEmpty).foreach {
          paths =>
            val qualifiedPaths = paths.map(makeQualifyPath(_, sessionCluster))
            qualifiedConfigs.put(config, qualifiedPaths.mkString(","))
        }
      }
      qualifiedConfigs
    } else {
      Map.empty[String, String].asJava
    }
  }
}

object HadoopPathQualifyConfAdvisor extends Logging {
  private val clusterDefaultFs = new ConcurrentHashMap[Option[String], String]()
  private val DEFAULT_FS_KEY = "fs.defaultFS"
  private def kyuubiConf: KyuubiConf = TagBasedSessionConfAdvisor.getDefaultConf()
  private def pathQualifyConfigs: Seq[String] =
    kyuubiConf.get(KyuubiEbayConf.HADOOP_PATH_QUALIFY_CONFIGS)

  private def pathSchemeDefined(path: String): Boolean = {
    try {
      new URI(path).getScheme != null
    } catch {
      case _: Throwable => !path.startsWith("/")
    }
  }

  private def makeQualifyPath(path: String, sessionCluster: Option[String]): String = {
    if (pathSchemeDefined(path)) {
      path
    } else {
      s"${getDefaultFS(sessionCluster)}$path"
    }
  }

  private def getDefaultFS(sessionCluster: Option[String]): String = {
    clusterDefaultFs.computeIfAbsent(
      sessionCluster,
      _ => {
        val clusterKyuubiConf = KyuubiEbayConf.loadClusterConf(kyuubiConf, sessionCluster)
        val clusterHadoopConf = KyuubiHadoopUtils.newHadoopConf(
          clusterKyuubiConf,
          clusterOpt = sessionCluster)
        val defaultFS = clusterHadoopConf.get(DEFAULT_FS_KEY)
        info(s"Load default FS for cluster $sessionCluster: $defaultFS")
        defaultFS
      })
  }
}
