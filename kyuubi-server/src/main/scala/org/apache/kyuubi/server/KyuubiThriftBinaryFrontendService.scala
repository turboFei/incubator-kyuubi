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

package org.apache.kyuubi.server

import org.apache.hive.service.rpc.thrift.{TExecuteStatementReq, TExecuteStatementResp}

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.ha.client.{KyuubiServiceDiscovery, ServiceDiscovery}
import org.apache.kyuubi.operation.{KyuubiExecuteStatementConf, OperationType}
import org.apache.kyuubi.operation.OperationType.LAUNCH_ENGINE
import org.apache.kyuubi.service.{Serverable, Service, ThriftBinaryFrontendService}
import org.apache.kyuubi.session.{KyuubiSessionImpl, SessionHandle}

class KyuubiThriftBinaryFrontendService(
    override val serverable: Serverable)
  extends ThriftBinaryFrontendService("KyuubiThriftBinaryFrontendService") {
  import KyuubiExecuteStatementConf._
  import ThriftBinaryFrontendService._

  override lazy val discoveryService: Option[Service] = {
    if (ServiceDiscovery.supportServiceDiscovery(conf)) {
      Some(new KyuubiServiceDiscovery(this))
    } else {
      None
    }
  }

  override def ExecuteKyuubiDefinedStatement(req: TExecuteStatementReq): TExecuteStatementResp = {
    val resp = new TExecuteStatementResp
    try {
      val execStmtConf = new KyuubiExecuteStatementConf(req.getConfOverlay)
      val definedOpType = execStmtConf.get(DEFINED_OPERATION_TYPE).map(OperationType.withName)

      definedOpType match {
        case Some(LAUNCH_ENGINE) =>
          val sessionHandle = SessionHandle(req.getSessionHandle)
          val session = be.sessionManager.getSession(sessionHandle)
            .asInstanceOf[KyuubiSessionImpl]
          val launchEngineOpHandle = session.launchEngineOp.getHandle
          resp.setOperationHandle(launchEngineOpHandle.toTOperationHandle)
          resp.setStatus(OK_STATUS)

        case _ =>
          throw KyuubiSQLException("No Kyuubi defined operation type specified")
      }
    } catch {
      case e: Exception =>
        error("Error executing statement: ", e)
        resp.setStatus(KyuubiSQLException.toTStatus(e))
    }
    resp
  }

  override def connectionUrl: String = {
    checkInitialized()
    s"${serverAddr.getCanonicalHostName}:$portNum"
  }

  override protected def oomHook: Runnable = {
    () => serverable.stop()
  }
}
