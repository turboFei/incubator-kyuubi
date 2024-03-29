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

import java.nio.charset.StandardCharsets
import java.util.Base64
import javax.ws.rs.core.GenericType

import org.apache.kyuubi.{KyuubiFunSuite, RestFrontendTestHelper, Utils}
import org.apache.kyuubi.server.http.util.HttpAuthUtils.AUTHORIZATION_HEADER

class EbayResourceSuite extends KyuubiFunSuite with RestFrontendTestHelper {
  private val encodeAuthorization: String = {
    new String(
      Base64.getEncoder.encode(
        s"${Utils.currentUser}:".getBytes()),
      StandardCharsets.UTF_8)
  }

  test("HADP-50404: Support to get groups with REST ebay resource") {
    val response = webTarget.path("api/v1/ebay/groups")
      .queryParam("user", Utils.currentUser)
      .request()
      .header(AUTHORIZATION_HEADER, s"BASIC $encodeAuthorization")
      .get()
    assert(200 == response.getStatus)
    val groups = response.readEntity(new GenericType[Seq[String]]() {})
    assert(groups.nonEmpty)
  }

  test("users") {
    val response = webTarget.path("api/v1/ebay/users")
      .request()
      .header(AUTHORIZATION_HEADER, s"BASIC $encodeAuthorization")
      .get()
    assert(200 == response.getStatus)
  }

  test("engines") {
    val response = webTarget.path("api/v1/ebay/engines")
      .request()
      .header(AUTHORIZATION_HEADER, s"BASIC $encodeAuthorization")
      .delete()
    assert(200 == response.getStatus)
  }
}
