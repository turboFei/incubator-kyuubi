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

import java.sql.SQLException

import org.apache.kyuubi.WithKyuubiServerOnYarn
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{ENGINE_INIT_TIMEOUT, ENGINE_SHARE_LEVEL}
import org.apache.kyuubi.tags.YarnTest

@YarnTest
class KyuubiSubmitApplicationSuite extends WithKyuubiServerOnYarn with HiveJDBCTestHelper {
  override protected def jdbcUrl: String = getJdbcUrl

  override protected val kyuubiServerConf: KyuubiConf = {
    // TODO KYUUBI #745
    KyuubiConf().set(ENGINE_INIT_TIMEOUT, 600000L)
      .set(ENGINE_SHARE_LEVEL, "CONNECTION")
  }

  override protected val connectionConf: Map[String, String] = Map(
    "kyuubi.operation.submit.spark.master" -> "yarn",
    "kyuubi.operation.submit.spark.executor.instances" -> "1",
    "kyuubi.operation.submit.conf.ignore.list" -> "spark.master",
    "kyuubi.engine.type" -> "SPARK_SUBMIT")

  test("HADP-43956 test submit spark application to yarn") {
    withJdbcStatement() { statement =>
      val submissionCmd = new SubmitApplicationProtocol
      submissionCmd.mainClass = "org.apache.spark.sql.hive.thriftserver.SparkSQLCLIDriver"
      submissionCmd.returnOnSubmitted = true

      val resultSet = statement.executeQuery(submissionCmd.toJson)

      assert(resultSet.next())
      assert(resultSet.getString("ApplicationId").contains("application_"))
      assert(resultSet.getString("URL").isEmpty)
      assert(!resultSet.next())
    }
  }

  test("HADP-43956 test SPARK_SUBMIT ignore conf from submission command") {
    withJdbcStatement() { statement =>
      val submissionCmd = new SubmitApplicationProtocol
      submissionCmd.mainClass = "org.apache.spark.sql.hive.thriftserver.SparkSQLCLIDriver"
      submissionCmd.conf = Map("spark.master" -> "local")
      submissionCmd.args = List("--verbose")
      submissionCmd.returnOnSubmitted = true

      val resultSet = statement.executeQuery(submissionCmd.toJson)
      assert(resultSet.next())
      assert(resultSet.getString(1).contains("application_"))
      assert(!resultSet.next())
    }
  }

  test("HADP-43956 test submit spark application failure") {
    withJdbcStatement() { statement =>
      val submissionCmd = new SubmitApplicationProtocol
      submissionCmd.mainClass = "invalidClass"
      submissionCmd.returnOnSubmitted = true

      intercept[SQLException] {
        statement.executeQuery(submissionCmd.toJson)
      }
    }
  }
}
