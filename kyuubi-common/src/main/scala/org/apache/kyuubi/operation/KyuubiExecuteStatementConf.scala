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

package org.apache.kyuubi.operation

import java.util.{HashMap => JHashMap, Locale, Map => JMap}
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.{ConfigBuilder, ConfigEntry, ConfigProvider}
import org.apache.kyuubi.operation.KyuubiExecuteStatementConf.opConfEntries

case class KyuubiExecuteStatementConf(conf: JMap[String, String]) extends Logging {
  private val settings = new ConcurrentHashMap[String, String]()
  private lazy val reader: ConfigProvider = new ConfigProvider(settings)

  conf.asScala.foreach { case (key, value) =>
    if (opConfEntries.containsKey(key)) {
      require(value != null)
      settings.put(key, value)
    }
  }

  def get[T](config: ConfigEntry[T]): T = {
    config.readFrom(reader)
  }

  def getOption(key: String): Option[String] = Option(settings.get(key))
}

/**
 * Kyuubi extends some api based on hive protocol [[TExecuteStatementReq]],
 * this is the conf entries used for that.
 */
object KyuubiExecuteStatementConf {
  val opConfEntries: JMap[String, ConfigEntry[_]] =
    java.util.Collections.synchronizedMap(new JHashMap[String, ConfigEntry[_]]())

  private def register(entry: ConfigEntry[_]): Unit = opConfEntries.synchronized {
    require(!opConfEntries.containsKey(entry.key),
      s"Duplicate SQLConfigEntry. ${entry.key} has been registered")
    opConfEntries.put(entry.key, entry)
  }

  def buildConf(key: String): ConfigBuilder = {
    ConfigBuilder("kyuubi.execute.statement." + key).onCreate(register)
  }

  val DEFINED_OPERATION_ENABLED = buildConf("defined.operation.enabled")
    .doc("")
    .version("1.4.0")
    .booleanConf
    .createWithDefault(false)

  val DEFINED_OPERATION_TYPE = buildConf("defined.operation.type")
    .doc("")
    .version("1.4.0")
    .stringConf
    .transform(_.toUpperCase(Locale.ROOT))
    .checkValues(OperationType.kyuubiDefinedOperationTypes.map(_.toString).toSet)
    .createOptional
}
