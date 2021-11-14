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

import java.io.{File, IOException}
import java.lang.ProcessBuilder.Redirect
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

import scala.collection.JavaConverters._
import scala.util.matching.Regex

import com.google.common.collect.EvictingQueue
import org.apache.commons.lang3.StringUtils.containsIgnoreCase

import org.apache.kyuubi.{KyuubiSQLException, Logging}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.util.NamedThreadFactory

trait ProcBuilder {

  import ProcBuilder._

  protected def executable: String

  protected def mainResource: Option[String]

  protected def module: String

  protected def mainClass: String

  protected def proxyUser: String

  protected def commands: Array[String]

  protected def conf: KyuubiConf

  protected def env: Map[String, String] = conf.getEnvs

  protected val extraEngineLog: Option[OperationLog]

  protected val workingDir: Path

  final lazy val processBuilder: ProcessBuilder = {
    val pb = new ProcessBuilder(commands: _*)

    val envs = pb.environment()
    envs.putAll(env.asJava)
    pb.directory(workingDir.toFile)
    pb.redirectError(engineLog)
    pb.redirectOutput(engineLog)
    pb
  }

  @volatile private var error: Throwable = UNCAUGHT_ERROR

  private val engineLogMaxLines = conf.get(KyuubiConf.SESSION_ENGINE_STARTUP_MAX_LOG_LINES)
  private val lastRowsOfLog: EvictingQueue[String] = EvictingQueue.create(engineLogMaxLines)
  // Visible for test
  @volatile private[kyuubi] var logCaptureThreadReleased: Boolean = true
  private var logCaptureThread: Thread = _

  private[kyuubi] lazy val engineLog: File = ProcBuilder.synchronized {
    val engineLogTimeout = conf.get(KyuubiConf.ENGINE_LOG_TIMEOUT)
    val currentTime = System.currentTimeMillis()
    val processLogPath = workingDir
    val totalExistsFile = processLogPath.toFile.listFiles { (_, name) => name.startsWith(module) }
    val sorted = totalExistsFile.sortBy(_.getName.split("\\.").last.toInt)
    val nextIndex = if (sorted.isEmpty) {
      0
    } else {
      sorted.last.getName.split("\\.").last.toInt + 1
    }
    val file = sorted.find(_.lastModified() < currentTime - engineLogTimeout)
      .map { existsFile =>
        try {
          // Here we want to overwrite the exists log file
          existsFile.delete()
          existsFile.createNewFile()
          existsFile
        } catch {
          case e: Exception =>
            warn(s"failed to delete engine log file: ${existsFile.getAbsolutePath}", e)
            null
        }
      }
      .getOrElse {
        Files.createDirectories(processLogPath)
        val newLogFile = new File(processLogPath.toFile, s"$module.log.$nextIndex")
        newLogFile.createNewFile()
        newLogFile
      }
    file.setLastModified(currentTime)
    info(s"Logging to $file")
    file
  }

  final def start: Process = synchronized {

    val proc = processBuilder.start()
    val reader = Files.newBufferedReader(engineLog.toPath, StandardCharsets.UTF_8)

    val redirect: Runnable = { () =>
      try {
        val maxErrorSize = conf.get(KyuubiConf.ENGINE_ERROR_MAX_SIZE)
        while (true) {
          if (reader.ready()) {
            var line: String = reader.readLine
            if (containsIgnoreCase(line, "Exception:") &&
              !line.contains("at ") && !line.startsWith("Caused by:")) {
              val sb = new StringBuilder(line)
              error = KyuubiSQLException(sb.toString() + s"\n See more: $engineLog")
              line = reader.readLine()
              while (sb.length < maxErrorSize && line != null &&
                (containsIgnoreCase(line, "Exception:") ||
                  line.startsWith("\tat ") ||
                  line.startsWith("Caused by: "))) {
                sb.append("\n" + line)
                line = reader.readLine()
              }

              error = KyuubiSQLException(sb.toString() + s"\n See more: $engineLog")
            } else if (line != null) {
              lastRowsOfLog.add(line)
            }
            extraEngineLog.foreach(_.writeLine(line))
          } else {
            Thread.sleep(300)
          }
        }
      } catch {
        case _: IOException =>
        case _: InterruptedException =>
      } finally {
        logCaptureThreadReleased = true
        reader.close()
      }
    }

    logCaptureThreadReleased = false
    logCaptureThread = PROC_BUILD_LOGGER.newThread(redirect)
    logCaptureThread.start()
    proc
  }

  val YARN_APP_NAME_REGEX: Regex = "application_\\d+_\\d+".r

  def killApplication(line: String = lastRowsOfLog.toArray.mkString("\n")): String =
    YARN_APP_NAME_REGEX.findFirstIn(line) match {
      case Some(appId) =>
        env.get(KyuubiConf.KYUUBI_HOME) match {
          case Some(kyuubiHome) =>
            val pb = new ProcessBuilder("/bin/sh", s"$kyuubiHome/bin/stop-application.sh", appId)
            pb.environment()
              .putAll(env.asJava)
            pb.redirectError(Redirect.appendTo(engineLog))
            pb.redirectOutput(Redirect.appendTo(engineLog))
            val process = pb.start()
            process.waitFor() match {
              case id if id != 0 => s"Failed to kill Application $appId, please kill it manually. "
              case _ => s"Killed Application $appId successfully. "
            }
          case None =>
            s"KYUUBI_HOME is not set! Failed to kill Application $appId, please kill it manually."
        }
      case None => ""
    }

  def close(): Unit = {
    if (logCaptureThread != null) {
      logCaptureThread.interrupt()
    }
  }

  def getError: Throwable = synchronized {
    if (error == UNCAUGHT_ERROR) {
      Thread.sleep(1000)
    }
    error match {
      case UNCAUGHT_ERROR =>
        KyuubiSQLException(s"Failed to detect the root cause, please check $engineLog at server " +
          s"side if necessary. The last $engineLogMaxLines line(s) of log are:\n" +
          s"${lastRowsOfLog.toArray.mkString("\n")}")
      case other => other
    }
  }
}

object ProcBuilder extends Logging {
  private val PROC_BUILD_LOGGER = new NamedThreadFactory("process-logger-capture", daemon = true)

  private val UNCAUGHT_ERROR = new RuntimeException("Uncaught error")
}
