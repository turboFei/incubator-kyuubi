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

import java.util.UUID

import scala.collection.JavaConverters._
import scala.concurrent.duration._

import org.apache.hive.service.rpc.thrift.TProtocolVersion

import org.apache.kyuubi.WithKyuubiServerOnYarn
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.SESSION_BATCH_STATIC_SECRET_ID
import org.apache.kyuubi.engine.spark.SparkProcessBuilder
import org.apache.kyuubi.server.api.v1.BatchRequest
import org.apache.kyuubi.session.{KyuubiBatchSessionImpl, KyuubiSessionManager}

class KyuubiBatchYarnClusterSuite extends WithKyuubiServerOnYarn {
  private val staticSecretId = UUID.randomUUID()

  override protected val connectionConf: Map[String, String] = Map.empty

  override protected val kyuubiServerConf: KyuubiConf = {
    // TODO KYUUBI #745
    KyuubiConf().set(SESSION_BATCH_STATIC_SECRET_ID, staticSecretId.toString)
  }

  private def sessionManager(): KyuubiSessionManager =
    server.backendService.sessionManager.asInstanceOf[KyuubiSessionManager]

  test("static batch session secret id") {
    val protocolVersion = TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V1
    val batchSessionHandle = sessionManager.newBatchSessionHandle(protocolVersion)
    assert(batchSessionHandle.identifier.secretId === staticSecretId)
  }

  test("open batch session") {
    val sparkProcessBuilder = new SparkProcessBuilder("kyuubi", conf)

    val batchRequest = BatchRequest(
      "spark",
      sparkProcessBuilder.mainResource.get,
      "kyuubi",
      sparkProcessBuilder.mainClass,
      List.empty[String].asJava,
      List.empty[String].asJava,
      "spark-batch-submission",
      Map("spark.master" -> "yarn"),
      List.empty[String].asJava)

    val sessionHandle = sessionManager.openBatchSession(
      TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V1,
      batchRequest.proxyUser,
      "passwd",
      "localhost",
      batchRequest.conf,
      batchRequest)

    assert(sessionHandle.identifier.secretId === staticSecretId)
    val session = sessionManager.getSession(sessionHandle).asInstanceOf[KyuubiBatchSessionImpl]

    eventually(timeout(3.minutes), interval(500.milliseconds)) {
      val applicationIdAndUrl = session.batchJobSubmissionOp.appIdAndUrl
      assert(applicationIdAndUrl.isDefined)
      assert(applicationIdAndUrl.exists(_._1.startsWith("application_")))
      assert(applicationIdAndUrl.exists(_._2.nonEmpty))
    }
    sessionManager.closeSession(sessionHandle)
  }
}
