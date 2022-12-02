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

package org.apache.kyuubi.engine.spark.operation

import java.io.{BufferedReader, File, FilenameFilter, FileOutputStream, InputStreamReader, PrintWriter}
import java.lang.ProcessBuilder.Redirect
import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.api.python.KyuubiPythonGatewayServer
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

import org.apache.kyuubi.Logging
import org.apache.kyuubi.operation.ArrayFetchIterator
import org.apache.kyuubi.session.Session

class ExecutePython(
    session: Session,
    override val statement: String,
    worker: SessionPythonWorker) extends SparkOperation(session) {

  override protected def resultSchema: StructType = {
    if (result == null || result.schema.isEmpty) {
      new StructType().add("output", "string")
        .add("status", "string")
        .add("ename", "string")
        .add("evalue", "string")
        .add("traceback", "array<string>")
    } else {
      result.schema
    }
  }

  override protected def runInternal(): Unit = {
    val response = worker.runCode(statement)
    val output = response.map(_.content.getOutput()).getOrElse("")
    val status = response.map(_.content.status).getOrElse("UNKNOWN_STATUS")
    val ename = response.map(_.content.getEname()).getOrElse("")
    val evalue = response.map(_.content.getEvalue()).getOrElse("")
    val traceback = response.map(_.content.getTraceback()).getOrElse(Array.empty)
    iter =
      new ArrayFetchIterator[Row](Array(Row(output, status, ename, evalue, Row(traceback: _*))))
  }

}

case class SessionPythonWorker(
    errorReader: Thread,
    pythonWorkerMonitor: Thread,
    workerProcess: Process) {
  private val stdin: PrintWriter = new PrintWriter(workerProcess.getOutputStream)
  private val stdout: BufferedReader =
    new BufferedReader(new InputStreamReader(workerProcess.getInputStream), 1)

  def runCode(code: String): Option[PythonReponse] = {
    val input = ExecutePython.toJson(Map("code" -> code, "cmd" -> "run_code"))
    // scalastyle:off println
    stdin.println(input)
    // scalastyle:on
    stdin.flush()
    Option(stdout.readLine())
      .map(ExecutePython.fromJson[PythonReponse](_))
  }

  def close(): Unit = {
    val exitCmd = ExecutePython.toJson(Map("cmd" -> "exit_worker"))
    // scalastyle:off println
    stdin.println(exitCmd)
    // scalastyle:on
    stdin.flush()
    stdin.close()
    stdout.close()
    errorReader.interrupt()
    pythonWorkerMonitor.interrupt()
    workerProcess.destroy()
  }
}

object ExecutePython extends Logging {

  // TODO:(fchen) get from conf
  val pythonExec =
    sys.env.getOrElse("PYSPARK_PYTHON", sys.env.getOrElse("PYSPARK_DRIVER_PYTHON", "python3"))
  private val isPythonGatewayStart = new AtomicBoolean(false)
  val kyuubiPythonPath = Files.createTempDirectory("")
  def init(): Unit = {
    if (!isPythonGatewayStart.get()) {
      synchronized {
        if (!isPythonGatewayStart.get()) {
          KyuubiPythonGatewayServer.start()
          writeTempPyFile(kyuubiPythonPath, "execute_python.py")
          writeTempPyFile(kyuubiPythonPath, "kyuubi_util.py")
          isPythonGatewayStart.set(true)
        }
      }
    }
  }

  def createSessionPythonWorker(): SessionPythonWorker = {
    val builder = new ProcessBuilder(Seq(
      pythonExec,
      s"${ExecutePython.kyuubiPythonPath}/execute_python.py").asJava)
    val env = builder.environment()
    val pythonPath = sys.env.getOrElse("PYTHONPATH", "")
      .split(File.pathSeparator)
      .++(ExecutePython.kyuubiPythonPath.toString)
    env.put("PYTHONPATH", pythonPath.mkString(File.pathSeparator))
    env.put("SPARK_HOME", sys.env.getOrElse("SPARK_HOME", defaultSparkHome()))
    env.put("PYTHON_GATEWAY_CONNECTION_INFO", KyuubiPythonGatewayServer.CONNECTION_FILE_PATH)
    logger.info(
      s"""
         |launch python worker command: ${builder.command().asScala.mkString(" ")}
         |environment:
         |${builder.environment().asScala.map(kv => kv._1 + "=" + kv._2).mkString("\n")}
         |""".stripMargin)
    builder.redirectError(Redirect.PIPE)
    val process = builder.start()
    SessionPythonWorker(startStderrSteamReader(process), startWatcher(process), process)
  }

  // for test
  def defaultSparkHome(): String = {
    val homeDirFilter: FilenameFilter = (dir: File, name: String) =>
      dir.isDirectory && name.contains("spark-") && !name.contains("-engine")
    // get from kyuubi-server/../externals/kyuubi-download/target
    new File(getClass.getProtectionDomain.getCodeSource.getLocation.toURI).getPath
      .split("kyuubi-spark-sql-engine").flatMap { cwd =>
        val candidates = Paths.get(cwd, "kyuubi-download", "target")
          .toFile.listFiles(homeDirFilter)
        if (candidates == null) None else candidates.map(_.toPath).headOption
      }.find(Files.exists(_)).map(_.toAbsolutePath.toFile.getCanonicalPath)
      .getOrElse {
        throw new IllegalStateException("SPARK_HOME not found!")
      }
  }

  private def startStderrSteamReader(process: Process): Thread = {
    val stderrThread = new Thread("process stderr thread") {
      override def run() = {
        val lines = scala.io.Source.fromInputStream(process.getErrorStream).getLines()
        lines.foreach(logger.error)
      }
    }
    stderrThread.setDaemon(true)
    stderrThread.start()
    stderrThread
  }

  def startWatcher(process: Process): Thread = {
    val processWatcherThread = new Thread("process watcher thread") {
      override def run() = {
        val exitCode = process.waitFor()
        if (exitCode != 0) {
          logger.error(f"Process has died with $exitCode")
        }
      }
    }
    processWatcherThread.setDaemon(true)
    processWatcherThread.start()
    processWatcherThread
  }

  private def writeTempPyFile(pythonPath: Path, pyfile: String): File = {
    val source = getClass.getClassLoader.getResourceAsStream(s"python/$pyfile")

    val file = new File(pythonPath.toFile, pyfile)
    file.deleteOnExit()

    val sink = new FileOutputStream(file)
    val buf = new Array[Byte](1024)
    var n = source.read(buf)

    while (n > 0) {
      sink.write(buf, 0, n)
      n = source.read(buf)
    }
    source.close()
    sink.close()
    file
  }

  val mapper = new ObjectMapper().registerModule(DefaultScalaModule)
  def toJson[T](obj: T): String = {
    mapper.writeValueAsString(obj)
  }
  def fromJson[T](json: String, clz: Class[T]): T = {
    mapper.readValue(json, clz)
  }

  def fromJson[T](json: String)(implicit m: Manifest[T]): T = {
    mapper.readValue(json, m.runtimeClass).asInstanceOf[T]
  }

}

case class PythonReponse(
    msg_type: String,
    content: PythonResponseContent)

case class PythonResponseContent(
    data: Map[String, String],
    ename: String,
    evalue: String,
    traceback: Array[String],
    status: String) {
  def getOutput(): String = {
    Option(data)
      .map(_.getOrElse("text/plain", ""))
      .getOrElse("")
  }
  def getEname(): String = {
    Option(ename).getOrElse("")
  }
  def getEvalue(): String = {
    Option(evalue).getOrElse("")
  }
  def getTraceback(): Array[String] = {
    Option(traceback).getOrElse(Array.empty)
  }
}
