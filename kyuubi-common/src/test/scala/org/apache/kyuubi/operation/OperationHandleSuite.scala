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

import org.apache.hive.service.rpc.thrift.TProtocolVersion
import org.apache.hive.service.rpc.thrift.TProtocolVersion._

import org.apache.kyuubi.KyuubiFunSuite

class OperationHandleSuite extends KyuubiFunSuite {

  test("OperationHandle") {
    val h1 = OperationHandle(HIVE_CLI_SERVICE_PROTOCOL_V10)
    TProtocolVersion.values().foreach { protocol =>
      assert(h1 === OperationHandle(h1.identifier, protocol))
    }

    val t1 = h1.toTOperationHandle
    TProtocolVersion.values().foreach { protocol =>
      assert(h1 === OperationHandle(t1, protocol))
    }
    val h2 = OperationHandle(t1)
    assert(h1 === h2)
    assert(!t1.isHasResultSet)
    h1.setHasResultSet(true)
    assert(h1.toTOperationHandle.isHasResultSet)
    assert(h1 !== null)
    assert(h1 !== new Integer(1))
    val h4 = OperationHandle(UUID.randomUUID(), HIVE_CLI_SERVICE_PROTOCOL_V10)
    assert(h4 !== h1)
  }
}
