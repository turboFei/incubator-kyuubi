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

import org.apache.hive.service.rpc.thrift.TGetInfoType

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.ebay.carmel.gateway.session.{CarmelSessionImpl, CarmelSessionStatus}
import org.apache.kyuubi.session.Session

abstract class InterceptedOperation(session: Session) extends KyuubiOperation(session) {
  override protected def runInternal(): Unit = {
    session match {
      case carmelSession: CarmelSessionImpl =>
        if (carmelSession.getBackendSessionStatus == CarmelSessionStatus.INACTIVE) {
          throw KyuubiSQLException(
            "The session has been closed due to the backend session has been dropped")
        }
      case _ =>
        try {
          client.getInfo(TGetInfoType.CLI_DBMS_NAME)
        } catch {
          case _: Throwable => throw KyuubiSQLException("The session to engine was interrupted")
        }
    }
    setState(OperationState.FINISHED)
    setHasResultSet(true)
  }
}
