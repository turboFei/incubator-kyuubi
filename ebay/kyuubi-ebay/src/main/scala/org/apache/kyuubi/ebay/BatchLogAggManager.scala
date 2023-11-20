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

import java.io.{BufferedInputStream, BufferedOutputStream, BufferedReader, File, FileInputStream, FileOutputStream, InputStream, InputStreamReader, OutputStream}
import java.util.{Date, Locale}
import java.util.concurrent.{ScheduledExecutorService, ThreadPoolExecutor, TimeUnit}

import scala.collection.mutable.ListBuffer
import scala.concurrent.Promise
import scala.concurrent.duration.Duration

import org.apache.commons.io.IOUtils
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}

import org.apache.kyuubi.Utils
import org.apache.kyuubi.config.{KyuubiConf, KyuubiEbayConf}
import org.apache.kyuubi.service.AbstractService
import org.apache.kyuubi.util.{KyuubiHadoopUtils, ThreadUtils}

class BatchLogAggManager extends AbstractService("BatchLogAggManager") {
  private var executor: ThreadPoolExecutor = _
  private var cleaner: ScheduledExecutorService = _
  private var localFs: FileSystem = _
  private var remoteFs: FileSystem = _
  private var remoteLogDir: Path = _
  private var hadoopConf: Configuration = _
  private var logFetchTimeout: Long = _

  private lazy val dateFormatter = FastDateFormat.getInstance("yyyy-MM-dd", Locale.US)
  private val datePattern = """^(\d\d\d\d)-(\d\d)-(\d\d)$""".r

  override def initialize(conf: KyuubiConf): Unit = {
    super.initialize(conf)
    if (conf.get(KyuubiEbayConf.LOG_AGG_ENABLED)) {
      val logAggThreadsNum = conf.get(KyuubiEbayConf.LOG_AGG_THREADS_NUM)
      executor = ThreadUtils.newDaemonFixedThreadPool(logAggThreadsNum, getName)
      if (conf.get(KyuubiEbayConf.LOG_AGG_CLEANER_ENABLED)) {
        cleaner = ThreadUtils.newDaemonSingleThreadScheduledExecutor("log-agg-cleaner")
      }
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

  private def getLogPath(createTime: Long, identifier: String): String = {
    Seq(dateFormatter.format(new Date(createTime)), identifier).mkString(
      Path.SEPARATOR)
  }

  private def joinFiles(destFile: File, sources: Seq[File]): Unit = {
    var output: OutputStream = null
    try {
      output = new BufferedOutputStream(new FileOutputStream(destFile, true))
      sources.foreach { source =>
        var input: InputStream = null
        try {
          input = new BufferedInputStream(new FileInputStream(source))
          IOUtils.copy(input, output)
        } finally {
          Utils.tryLogNonFatalError(IOUtils.close(input))
        }
      }
    } finally {
      Utils.tryLogNonFatalError(IOUtils.close(output))
    }
  }

  def aggLog(localLogPaths: Seq[File], createTime: Long, identifier: String): Unit = {
    Option(executor).foreach { _ =>
      Utils.tryLogNonFatalError {
        executor.submit(new Runnable {
          override def run(): Unit = {
            var tempDir: File = null
            try {
              tempDir = Utils.createTempDir(prefix = "batch-log-agg").toFile
              val localAggFile = new File(tempDir, identifier)
              joinFiles(localAggFile, localLogPaths)
              val targetPath = new Path(remoteLogDir, getLogPath(createTime, identifier))
              if (!remoteFs.exists(targetPath.getParent)) {
                remoteFs.mkdirs(targetPath.getParent)
              }
              FileUtil.copy(
                localFs,
                new Path(localAggFile.getAbsolutePath),
                remoteFs,
                targetPath,
                true,
                true,
                hadoopConf)
            } catch {
              case e: Throwable =>
                error(s"Error aggregate the log local files for $identifier", e)
            } finally {
              Option(tempDir).foreach(Utils.deleteDirectoryRecursively(_))
            }
          }
        })
      }
    }
  }

  def getAggregatedLog(
      createTime: Long,
      identifier: String,
      from: Int,
      size: Int): Option[Seq[String]] = {
    Option(executor) match {
      case Some(_) =>
        val promise = Promise[Seq[String]]()
        val task = new Runnable {
          override def run(): Unit = {
            val logRows = ListBuffer[String]()
            try {
              var remoteLogFile = new Path(remoteLogDir, getLogPath(createTime, identifier))
              if (!remoteFs.exists(remoteLogFile)) {
                // fallback to legacy location
                remoteLogFile = new Path(remoteLogDir, identifier)
              }

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
                  promise.trySuccess(logRows)
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

  private def deleteLogPath(path: Path): Unit = {
    try {
      info(s"Cleaning the batch log path $path")
      remoteFs.delete(path, true)
    } catch {
      case e: Throwable => error(s"Error cleaning batch log path $path", e)
    }
  }

  def startLogCleaner(): Unit = {
    Option(cleaner).foreach { _ =>
      val cleanInterval = conf.get(KyuubiEbayConf.LOG_AGG_CLEANER_INTERVAL)
      val logMaxAge = conf.get(KyuubiEbayConf.LOG_AGG_CLEANER_MAX_LOG_AGE)
      ThreadUtils.scheduleTolerableRunnableWithFixedDelay(
        cleaner,
        () =>
          try {
            val currentTime = System.currentTimeMillis()
            val maxTime = currentTime - logMaxAge
            val maxDate = dateFormatter.format(new Date(maxTime))
            info(s"Cleaning the batch log dir created before $maxDate")
            val subPaths = remoteFs.listStatusIterator(remoteLogDir)
            while (subPaths.hasNext) {
              val subPathStatus = subPaths.next()
              val subPath = subPathStatus.getPath
              subPathStatus.getPath.getName match {
                case datePattern(yyyy, mm, dd) =>
                  val dateDir = s"$yyyy-$mm-$dd"
                  if (dateDir < maxDate) {
                    deleteLogPath(subPath)
                  }
                case _ =>
                  if (subPathStatus.isFile && subPathStatus.getModificationTime < maxTime) {
                    deleteLogPath(subPath)
                  }
              }
            }
          } catch {
            case e: Throwable =>
              error("Error cleaning aggregated log files", e)
          },
        0,
        cleanInterval,
        TimeUnit.MILLISECONDS)
    }
  }

  override def start(): Unit = {
    super.start()
    startLogCleaner()
    BatchLogAggManager._logAggManager = this
  }

  override def stop(): Unit = {
    Option(executor).map(_.shutdown)
    Option(cleaner).map(_.shutdown)
    BatchLogAggManager._logAggManager = null
    super.stop()
  }
}

object BatchLogAggManager {
  private var _logAggManager: BatchLogAggManager = _
  def get: Option[BatchLogAggManager] = Option(_logAggManager)
}
