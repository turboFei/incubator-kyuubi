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

import org.apache.kyuubi.WithKyuubiServer
import org.apache.kyuubi.config.{KyuubiConf, KyuubiEbayConf}
import org.apache.kyuubi.jdbc.hive.KyuubiStatement
import org.apache.kyuubi.service.authentication.{AnonymousBatchAccountAuthenticationProviderImpl, KyuubiAuthenticationFactory}

class KyuubiOperationBatchAccountSuite extends WithKyuubiServer with HiveJDBCTestHelper {
  override protected def jdbcUrl: String = getJdbcUrl

  override protected val conf: KyuubiConf = {
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
}
