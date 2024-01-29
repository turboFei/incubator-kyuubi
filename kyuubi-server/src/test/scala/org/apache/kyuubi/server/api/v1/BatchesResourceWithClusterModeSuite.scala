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

package org.apache.kyuubi.server.api.v1

import javax.ws.rs.client.Entity
import javax.ws.rs.core.MediaType

import scala.collection.JavaConverters._

import org.apache.kyuubi.{KyuubiFunSuite, RestFrontendTestHelper}
import org.apache.kyuubi.client.api.v1.dto.{Batch, BatchRequest}
import org.apache.kyuubi.config.{KyuubiConf, KyuubiEbayConf}
import org.apache.kyuubi.config.KyuubiConf.{ENGINE_CHECK_INTERVAL, ENGINE_SPARK_MAX_LIFETIME}
import org.apache.kyuubi.engine.spark.SparkProcessBuilder
import org.apache.kyuubi.server.metadata.api.MetadataFilter
import org.apache.kyuubi.server.metadata.jdbc.JDBCMetadataStore

class BatchesResourceWithClusterModeSuite extends KyuubiFunSuite with RestFrontendTestHelper {
  override protected lazy val conf: KyuubiConf = {
    KyuubiConf().set(KyuubiEbayConf.SESSION_CLUSTER_MODE_ENABLED, true)
  }

  private lazy val jdbcMetadataStore = new JDBCMetadataStore(conf)

  override def afterAll(): Unit = {
    super.afterAll()
    jdbcMetadataStore.getMetadataList(MetadataFilter(), 0, Int.MaxValue).foreach {
      batch =>
        jdbcMetadataStore.cleanupMetadataByIdentifier(batch.identifier)
    }
    jdbcMetadataStore.close()
  }

  test("open batch session") {
    val sparkProcessBuilder = new SparkProcessBuilder("kyuubi", true, conf)
    var requestObj = new BatchRequest(
      "spark",
      sparkProcessBuilder.mainResource.get,
      sparkProcessBuilder.mainClass,
      "spark-batch-submission",
      Map(
        "spark.master" -> "local",
        s"spark.${ENGINE_SPARK_MAX_LIFETIME.key}" -> "5000",
        s"spark.${ENGINE_CHECK_INTERVAL.key}" -> "1000").asJava,
      Seq.empty[String].asJava)

    var response = webTarget.path("api/v1/batches")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .post(Entity.entity(requestObj, MediaType.APPLICATION_JSON_TYPE))
    assert(500 == response.getStatus)

    requestObj.setConf(
      (requestObj.getConf.asScala ++ Map(KyuubiEbayConf.SESSION_CLUSTER.key -> "test")).asJava)

    response = webTarget.path("api/v1/batches")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .post(Entity.entity(requestObj, MediaType.APPLICATION_JSON_TYPE))
    assert(200 == response.getStatus)
    val batch = response.readEntity(classOf[Batch])
    assert(batch.getKyuubiInstance === fe.connectionUrl)

    // invalid cluster
    requestObj.setConf((requestObj.getConf.asScala ++ Map(
      KyuubiEbayConf.SESSION_CLUSTER.key -> "invalidCluster")).asJava)

    response = webTarget.path("api/v1/batches")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .post(Entity.entity(requestObj, MediaType.APPLICATION_JSON_TYPE))
    assert(500 == response.getStatus)
  }
}
