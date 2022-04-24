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

package org.apache.kyuubi.engine.flink

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf

class FlinkProcessBuilderSuite extends KyuubiFunSuite {
  private def conf = KyuubiConf().set("kyuubi.on", "off")

  test("flink engine process builder") {
    val builder = new FlinkProcessBuilder("vinoyang", conf)
    val commands = builder.toString.split(' ')
    assert(commands.exists(_ endsWith "flink-sql-engine.sh"))
  }

  test("kill application") {
    val processBuilder = new FakeFlinkProcessBuilder(conf) {
      override protected def env: Map[String, String] = Map("FLINK_HOME" -> "")
    }
    val exit1 = processBuilder.killApplication(
      Right("""
              |[INFO] SQL update statement has been successfully submitted to the cluster:
              |Job ID: 6b1af540c0c0bb3fcfcad50ac037c862
              |""".stripMargin))
    assert(exit1.contains("6b1af540c0c0bb3fcfcad50ac037c862")
      && !exit1.contains("FLINK_HOME is not set!"))

    val exit2 = processBuilder.killApplication(Right("unknow"))
    assert(exit2.equals(""))
  }
}

class FakeFlinkProcessBuilder(config: KyuubiConf)
  extends FlinkProcessBuilder("fake", config) {
  override protected def commands: Array[String] = Array("ls")
}
