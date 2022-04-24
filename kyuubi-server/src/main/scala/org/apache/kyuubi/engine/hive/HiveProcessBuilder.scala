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

package org.apache.kyuubi.engine.hive

import java.io.File
import java.nio.file.{Files, Paths}
import java.util.LinkedHashSet

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.kyuubi._
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiReservedKeys.KYUUBI_SESSION_USER_KEY
import org.apache.kyuubi.engine.ProcBuilder
import org.apache.kyuubi.operation.log.OperationLog

class HiveProcessBuilder(
    override val proxyUser: String,
    override val conf: KyuubiConf,
    val extraEngineLog: Option[OperationLog] = None)
  extends ProcBuilder with Logging {

  private val hiveHome: String = getEngineHome("hive")

  override protected def module: String = "kyuubi-hive-sql-engine"

  override protected def mainClass: String = "org.apache.kyuubi.engine.hive.HiveSQLEngine"

  override protected def commands: Array[String] = {
    val buffer = new ArrayBuffer[String]()
    buffer += executable

    // TODO: How shall we deal with proxyUser,
    // user.name
    // kyuubi.session.user
    // or just leave it, because we can handle it at operation layer
    buffer += s"-D$KYUUBI_SESSION_USER_KEY=$proxyUser"

    // TODO: add Kyuubi.engineEnv.HIVE_ENGINE_MEMORY or kyuubi.engine.hive.memory to configure
    // -Xmx5g
    // java options
    for ((k, v) <- conf.getAll) {
      buffer += s"-D$k=$v"
    }

    buffer += "-cp"
    val classpathEntries = new LinkedHashSet[String]
    // hive engine runtime jar
    mainResource.foreach(classpathEntries.add)
    // classpath contains hive configurations, default to hive.home/conf
    classpathEntries.add(env.getOrElse("HIVE_CONF_DIR", s"$hiveHome${File.separator}conf"))
    // classpath contains hadoop configurations
    val hadoopConfDir = env.get("HADOOP_CONF_DIR")
    if (hadoopConfDir.isEmpty) {
      warn(s"HADOOP_CONF_DIR does not export.")
    } else {
      classpathEntries.add(hadoopConfDir.get)
    }
    env.get("YARN_CONF_DIR").foreach(classpathEntries.add)
    // jars from hive distribution
    classpathEntries.add(s"$hiveHome${File.separator}lib${File.separator}*")
    val hadoopCp = env.get("HIVE_HADOOP_CLASSPATH").orElse(env.get("HADOOP_CLASSPATH"))
    hadoopCp.foreach(path => classpathEntries.add(s"$path${File.separator}*"))
    if (hadoopCp.isEmpty) {
      warn(s"HIVE_HADOOP_CLASSPATH or HADOOP_CLASSPATH don't export.")
      mainResource.foreach { path =>
        val devHadoopJars = Paths.get(path).getParent
          .resolve(s"scala-$SCALA_COMPILE_VERSION")
          .resolve("jars")
        if (!Files.exists(devHadoopJars)) {
          throw new KyuubiException(s"The path $devHadoopJars does not exists. ")
        }
        classpathEntries.add(s"$devHadoopJars${File.separator}*")
      }

    }
    buffer += classpathEntries.asScala.mkString(File.pathSeparator)
    buffer += mainClass
    buffer.toArray
  }

  override def toString: String = commands.mkString("\n")

  override protected def shortName: String = "hive"
}
