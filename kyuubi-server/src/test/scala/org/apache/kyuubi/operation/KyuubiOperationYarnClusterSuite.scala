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

import org.apache.hadoop.yarn.client.api.YarnClient

import org.apache.kyuubi.WithKyuubiServerOnYarn
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.tags.YarnTest
import org.apache.kyuubi.util.KyuubiHadoopUtils

@YarnTest
class KyuubiOperationYarnClusterSuite extends WithKyuubiServerOnYarn with SparkQueryTests {

  override protected def jdbcUrl: String = getJdbcUrl

  test("KYUUBI #527- Support test with mini yarn cluster") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("""SELECT "${spark.app.id}" as id""")
      assert(resultSet.next())
      assert(resultSet.getString("id").startsWith("application_"))
    }
  }

  test("session_user shall work on yarn") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("SELECT SESSION_USER() as su")
      assert(resultSet.next())
      assert(resultSet.getString("su") === user)
    }
  }

  test("move engine queue") {
    withSessionConf()(Map(
      SESSION_ENGINE_LAUNCH_MOVE_QUEUE_ENABLED.key -> "true",
      SESSION_ENGINE_LAUNCH_MOVE_QUEUE_INIT_QUEUE.key -> "default",
      "spark.yarn.queue" -> "two_cores_queue"))(Map.empty) {
      withJdbcStatement() { statement =>
        val resultSet = statement.executeQuery("set spark.app.id")
        assert(resultSet.next())
        val applicationId = KyuubiHadoopUtils.getApplicationIdFromString(resultSet.getString(2))
        val yarnClient = YarnClient.createYarnClient()
        yarnClient.init(KyuubiHadoopUtils.newHadoopConf(conf, clusterOpt = Option("yarn")))
        yarnClient.start()
        val queue = yarnClient.getApplicationReport(applicationId).getQueue
        assert(queue.equals("two_cores_queue"))
        yarnClient.stop()
      }
    }
  }

  test("move engine to invalid queue") {
    withSessionConf()(Map(
      SESSION_ENGINE_LAUNCH_MOVE_QUEUE_ENABLED.key -> "true",
      SESSION_ENGINE_LAUNCH_MOVE_QUEUE_INIT_QUEUE.key -> "default",
      "spark.yarn.queue" -> "invalid_queue"))(Map.empty) {
      val exception = intercept[SQLException] {
        withJdbcStatement() { _ =>
        }
      }
      assert(exception.getMessage.contains("The specified Queue: invalid_queue doesn't exist"))
    }
  }
}
