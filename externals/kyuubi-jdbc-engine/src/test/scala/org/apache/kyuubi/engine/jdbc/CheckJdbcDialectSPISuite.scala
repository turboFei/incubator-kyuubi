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

package org.apache.kyuubi.engine.jdbc

import java.nio.file.Paths

// scalastyle:off
import org.scalatest.funsuite.AnyFunSuite

import org.apache.kyuubi.util.AssertionUtils._
import org.apache.kyuubi.util.GoldenFileUtils._

class CheckJdbcDialectSPISuite extends AnyFunSuite {
  // scalastyle:on

  test("check JDBC dialect SPI service file sorted") {
    Seq(
      "org.apache.kyuubi.engine.jdbc.connection.JdbcConnectionProvider",
      "org.apache.kyuubi.engine.jdbc.dialect.JdbcDialect")
      .foreach { fileName =>
        val filePath = Paths.get(
          s"${getCurrentModuleHome(this)}/src/main/resources/META-INF/services/$fileName")
        assertFileContentSorted(filePath)
      }
  }
}
