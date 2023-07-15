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

import org.apache.hive.service.rpc.thrift.{TOperationState, TProtocolVersion}
import org.apache.hive.service.rpc.thrift.TOperationState._

import org.apache.kyuubi.{KyuubiFunSuite, KyuubiSQLException}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.operation.OperationState._
import org.apache.kyuubi.session.NoopSessionManager

class OperationStateSuite extends KyuubiFunSuite {
  test("toTOperationState") {
    val to = OperationState.toTOperationState _
    assert(to(INITIALIZED) === INITIALIZED_STATE)
    assert(to(PENDING) === PENDING_STATE)
    assert(to(RUNNING) === RUNNING_STATE)
    assert(to(COMPILED) === RUNNING_STATE) // mapping complied to running state
    assert(to(FINISHED) === FINISHED_STATE)
    assert(to(CANCELED) === CANCELED_STATE)
    assert(to(CLOSED) === CLOSED_STATE)
    assert(to(ERROR) === ERROR_STATE)
    assert(to(TIMEOUT) === TIMEDOUT_STATE)
    assert(to(UNKNOWN) === TOperationState.UKNOWN_STATE)
  }

  test("validate transition") {
    (OperationState.values -- Set(PENDING, RUNNING, COMPILED, TIMEOUT, CANCELED, CLOSED))
      .foreach { state =>
        intercept[KyuubiSQLException](OperationState.validateTransition(INITIALIZED, state))
      }

    (OperationState.values -- Set(RUNNING, COMPILED, FINISHED, TIMEOUT, CANCELED, CLOSED, ERROR))
      .foreach { state =>
        intercept[KyuubiSQLException](OperationState.validateTransition(PENDING, state))
      }

    (OperationState.values -- Set(RUNNING, COMPILED, FINISHED, TIMEOUT, CANCELED, CLOSED, ERROR))
      .foreach { state =>
        intercept[KyuubiSQLException](OperationState.validateTransition(RUNNING, state))
      }

    (OperationState.values -- Set(RUNNING, COMPILED, FINISHED, TIMEOUT, CANCELED, CLOSED, ERROR))
      .foreach { state =>
        intercept[KyuubiSQLException](OperationState.validateTransition(COMPILED, state))
      }

    (OperationState.values -- Set(FINISHED, TIMEOUT, CANCELED, CLOSED, ERROR)).foreach { state =>
      intercept[KyuubiSQLException](OperationState.validateTransition(FINISHED, state))
    }

    (OperationState.values -- Set(CLOSED)).foreach { state =>
      Seq(FINISHED, CANCELED, TIMEOUT, ERROR).foreach { state1 =>
        intercept[KyuubiSQLException](OperationState.validateTransition(state1, state))
      }
    }
  }

  test("is terminal") {
    Seq(FINISHED, TIMEOUT, CANCELED, CLOSED, ERROR).foreach { state =>
      assert(OperationState.isTerminal(state))
    }

    (OperationState.values -- Seq(FINISHED, TIMEOUT, CANCELED, CLOSED, ERROR)).foreach { state =>
      assert(!OperationState.isTerminal(state))
    }
  }

  test("kyuubi-5036 operation close should set completeTime") {
    val sessionManager = new NoopSessionManager
    sessionManager.initialize(KyuubiConf())
    val sHandle = sessionManager.openSession(
      TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V11,
      "kyuubi",
      "passwd",
      "localhost",
      Map.empty)
    val session = sessionManager.getSession(sHandle)

    val operation = new NoopOperation(session)
    assert(operation.getStatus.completed == 0)

    operation.close()
    val afterClose1 = operation.getStatus
    assert(afterClose1.state == OperationState.CLOSED)
    assert(afterClose1.completed != 0)
    Thread.sleep(1000)
    val afterClose2 = operation.getStatus
    assert(afterClose1.completed == afterClose2.completed)
  }
}
