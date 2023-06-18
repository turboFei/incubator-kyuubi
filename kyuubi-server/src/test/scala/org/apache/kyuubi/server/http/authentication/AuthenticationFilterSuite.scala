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

package org.apache.kyuubi.server.http.authentication

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.service.authentication.AuthTypes

class AuthenticationFilterSuite extends KyuubiFunSuite {
  test("add auth handler and destroy") {
    val filter = new AuthenticationFilter(KyuubiConf())
    filter.addAuthHandler(new BasicAuthenticationHandler(null))
    assert(filter.authSchemeHandlers.isEmpty)
    filter.addAuthHandler(new BasicAuthenticationHandler(AuthTypes.LDAP))
    assert(filter.authSchemeHandlers.size == 1)
    filter.addAuthHandler(new BasicAuthenticationHandler(AuthTypes.LDAP))
    assert(filter.authSchemeHandlers.size == 1)
    filter.addAuthHandler(new KerberosAuthenticationHandler())
    assert(filter.authSchemeHandlers.size == 1)
    filter.destroy()
    assert(filter.authSchemeHandlers.isEmpty)
  }
}
