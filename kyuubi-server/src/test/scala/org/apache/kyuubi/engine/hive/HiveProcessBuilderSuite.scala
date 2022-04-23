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

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.tags.HiveTest

@HiveTest
class HiveProcessBuilderSuite extends KyuubiFunSuite {

  private def conf = KyuubiConf().set("kyuubi.on", "off")

  test("hive process builder") {
    val builder = new HiveProcessBuilder("kyuubi", conf)
    val commands = builder.toString.split('\n')
    assert(commands.head.endsWith("bin/java"), "wrong exec")
    assert(commands.contains("-Dkyuubi.session.user=kyuubi"))
    assert(commands.contains("-Dkyuubi.on=off"))
    assert(commands.exists(ss => ss.contains("kyuubi-hive-sql-engine")), "wrong classpath")
  }

}
