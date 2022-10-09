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

package org.apache.kyuubi.server

import java.io.{BufferedReader, File, InputStreamReader}
import java.nio.file.Files
import java.util.concurrent.{ThreadPoolExecutor, TimeUnit}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.concurrent.Promise
import scala.concurrent.duration.Duration

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}

import org.apache.kyuubi.client.api.v1.dto.OperationLog
import org.apache.kyuubi.config.{KyuubiConf, KyuubiEbayConf}
import org.apache.kyuubi.service.AbstractService
import org.apache.kyuubi.util.{KyuubiHadoopUtils, ThreadUtils}

class LogAggManager() extends AbstractService("LogAggManager") {
  private var executor: ThreadPoolExecutor = _
  private var localFs: FileSystem = _
  private var remoteFs: FileSystem = _
  private var remoteLogDir: Path = _
  private var hadoopConf: Configuration = _
  private var logFetchTimeout: Long = _

  override def initialize(conf: KyuubiConf): Unit = {
    super.initialize(conf)
    if (conf.get(KyuubiEbayConf.LOG_AGG_ENABLED)) {
      val logAggThreadsNum = conf.get(KyuubiEbayConf.LOG_AGG_THREADS_NUM)
      executor = ThreadUtils.newDaemonFixedThreadPool(logAggThreadsNum, getName)
      val (logAggCluster, logAggDir) =
        conf.get(KyuubiEbayConf.LOG_AGG_CLUSTER_DIR).split("\\s+") match {
          case Array(cluster, dir) => (Option(cluster), dir)
          case Array(dir) => (None, dir)
          case _ => throw new IllegalArgumentException("invalid cluster and dir specify")
        }
      hadoopConf = KyuubiHadoopUtils.newHadoopConf(conf, clusterOpt = logAggCluster)
      localFs = FileSystem.getLocal(hadoopConf)
      remoteFs = FileSystem.get(hadoopConf)
      remoteLogDir = remoteFs.makeQualified(new Path(logAggDir))
      logFetchTimeout = conf.get(KyuubiEbayConf.LOG_AGG_FETCH_TIMEOUT)
    }
  }

  def aggLog(localFile: File, identifier: String): Unit = {
    Option(executor).foreach { _ =>
      executor.submit(new Runnable {
        override def run(): Unit = {
          try {
            if (localFile.isFile) {
              val targetPath = new Path(remoteLogDir, identifier)
              if (!remoteFs.exists(targetPath)) {
                FileUtil.copy(
                  localFs,
                  new Path(localFile.getAbsolutePath),
                  remoteFs,
                  targetPath,
                  false,
                  true,
                  hadoopConf)
              } else {
                val dStream = remoteFs.append(targetPath)
                try {
                  val dataArr = Files.readAllBytes(localFile.toPath)
                  dStream.write(dataArr)
                } finally {
                  dStream.close()
                }
              }
            }
          } catch {
            case e: Throwable =>
              error(s"Error aggregate the log local file $localFile for $identifier", e)
          }
        }
      })
    }
  }

  def getAggregatedLog(identifier: String, from: Int, size: Int): Option[OperationLog] = {
    Option(executor) match {
      case Some(_) =>
        val promise = Promise[OperationLog]()
        val task = new Runnable {
          override def run(): Unit = {
            val logRows = ListBuffer[String]()
            try {
              val remoteLogFile = new Path(remoteLogDir, identifier)
              if (!remoteFs.exists(remoteLogFile)) {
                promise.trySuccess(null)
              } else {
                val br = new BufferedReader(new InputStreamReader(remoteFs.open(remoteLogFile)))
                try {
                  var line: String = br.readLine()
                  var lineOffset = 0
                  var logCount = 0
                  while (line != null && logCount < size) {
                    if (lineOffset >= from) {
                      logRows += line
                      logCount += 1
                    }
                    lineOffset += 1
                    line = br.readLine()
                  }
                  promise.trySuccess(new OperationLog(logRows.asJava, logCount))
                } finally {
                  br.close()
                }
              }
            } catch {
              case e: Throwable => promise.tryFailure(e)
            }
          }
        }
        executor.submit(task)
        Option(ThreadUtils.awaitResult(
          promise.future,
          Duration(logFetchTimeout, TimeUnit.MILLISECONDS)))
      case None => None
    }
  }

  override def start(): Unit = {
    super.start()
    LogAggManager._logAggManager = this
  }

  override def stop(): Unit = {
    super.stop()
    Option(executor).map(_.shutdown())
    LogAggManager._logAggManager = null
  }
}

object LogAggManager {
  private var _logAggManager: LogAggManager = _
  def get: Option[LogAggManager] = Option(_logAggManager)
}
