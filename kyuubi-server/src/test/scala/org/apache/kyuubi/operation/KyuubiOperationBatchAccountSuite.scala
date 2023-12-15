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

import javax.ws.rs.client.Entity
import javax.ws.rs.core.MediaType

import scala.collection.JavaConverters._

import org.apache.kyuubi.RestFrontendTestHelper
import org.apache.kyuubi.client.api.v1.dto.{SessionHandle, SessionOpenRequest}
import org.apache.kyuubi.config.{KyuubiConf, KyuubiEbayConf}
import org.apache.kyuubi.config.KyuubiConf.FrontendProtocols
import org.apache.kyuubi.config.KyuubiConf.FrontendProtocols.FrontendProtocol
import org.apache.kyuubi.jdbc.hive.KyuubiStatement
import org.apache.kyuubi.service.authentication.{AnonymousBatchAccountAuthenticationProviderImpl, KyuubiAuthenticationFactory}

class KyuubiOperationBatchAccountSuite extends RestFrontendTestHelper with HiveJDBCTestHelper {
  override protected def jdbcUrl: String = getJdbcUrl
  override protected def getJdbcUrl: String =
    s"jdbc:hive2://${server.frontendServices.last.connectionUrl}/;"

  override protected val frontendProtocols: Seq[FrontendProtocol] =
    FrontendProtocols.REST :: FrontendProtocols.THRIFT_BINARY :: Nil

  override protected lazy val conf: KyuubiConf = {
    KyuubiConf().set(KyuubiConf.ENGINE_SHARE_LEVEL, "connection")
      .set(
        KyuubiEbayConf.AUTHENTICATION_BATCH_ACCOUNT_CLASS,
        classOf[AnonymousBatchAccountAuthenticationProviderImpl].getCanonicalName)
  }

  test("test proxy batch account") {
    withSessionConf(Map(
      KyuubiAuthenticationFactory.KYUUBI_PROXY_BATCH_ACCOUNT -> "b_stf"))(Map.empty)(Map.empty) {
      withJdbcStatement() { statement =>
        val kyuubiStatement = statement.asInstanceOf[KyuubiStatement]
        val rs = kyuubiStatement.executeScala(
          "org.apache.hadoop.security.UserGroupInformation.getCurrentUser().getShortUserName()")
        assert(rs.next())
        assert(rs.getString(1).contains("b_stf"))
      }
    }
  }

  test("fallback to verify batch account if proxy user is specified") {
    withSessionConf(Map(
      KyuubiAuthenticationFactory.HS2_PROXY_USER -> "b_stf"))(Map.empty)(Map.empty) {
      withJdbcStatement() { statement =>
        val kyuubiStatement = statement.asInstanceOf[KyuubiStatement]
        val rs = kyuubiStatement.executeScala(
          "org.apache.hadoop.security.UserGroupInformation.getCurrentUser().getShortUserName()")
        assert(rs.next())
        assert(rs.getString(1).contains("b_stf"))
      }
    }
  }

  test("rest api proxy batch account") {
    val requestObj = new SessionOpenRequest(Map(
      KyuubiAuthenticationFactory.KYUUBI_PROXY_BATCH_ACCOUNT -> "batch").asJava)

    val response = webTarget.path("api/v1/sessions")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .post(Entity.entity(requestObj, MediaType.APPLICATION_JSON_TYPE))

    val sessionHandle = response.readEntity(classOf[SessionHandle]).getIdentifier
    assert(server.backendService.sessionManager.getSession(
      org.apache.kyuubi.session.SessionHandle(sessionHandle)).user == "batch")
  }
}
