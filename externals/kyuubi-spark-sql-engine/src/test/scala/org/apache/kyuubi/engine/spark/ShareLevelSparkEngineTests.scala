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

package org.apache.kyuubi.engine.spark

import java.util.UUID

import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

import org.apache.kyuubi.engine.ShareLevel
import org.apache.kyuubi.engine.ShareLevel.ShareLevel
import org.apache.kyuubi.operation.HiveJDBCTestHelper
import org.apache.kyuubi.service.ServiceState

/**
 * This suite is to test some behavior with spark engine in different share level.
 * e.g. cleanup discovery service before stop.
 */
trait ShareLevelSparkEngineTests
  extends WithDiscoverySparkSQLEngine with HiveJDBCTestHelper {
  def shareLevel: ShareLevel

  override protected def jdbcUrl: String = getJdbcUrl
  override val namespace: String = {
    // for test, we always use uuid as namespace
    s"/kyuubi/${shareLevel.toString}/${UUID.randomUUID().toString}"
  }

  test("check discovery service is clean up with different share level") {
    withDiscoveryClient { discoveryClient =>
      assert(engine.getServiceState == ServiceState.STARTED)
      assert(discoveryClient.pathExists(namespace))
      withJdbcStatement() { _ => }
      shareLevel match {
        // Connection level, we will cleanup namespace since it's always a global unique value.
        case ShareLevel.CONNECTION =>
          assert(engine.getServiceState == ServiceState.STOPPED)
          assert(discoveryClient.pathNonExists(namespace))
        case _ =>
          assert(engine.getServiceState == ServiceState.STARTED)
          assert(discoveryClient.pathExists(namespace))
      }
    }
  }

  test("test spark engine max life-time") {
    withDiscoveryClient { discoveryClient =>
      assert(engine.getServiceState == ServiceState.STARTED)
      assert(discoveryClient.pathExists(namespace))
      withJdbcStatement() { _ => }

      eventually(Timeout(30.seconds)) {
        shareLevel match {
          case ShareLevel.CONNECTION =>
            assert(engine.getServiceState == ServiceState.STOPPED)
            assert(discoveryClient.pathNonExists(namespace))
          case _ =>
            assert(engine.getServiceState == ServiceState.STOPPED)
            assert(discoveryClient.pathExists(namespace))
        }
      }
    }
  }
}
