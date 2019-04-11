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

package yaooqinn.kyuubi.session

import org.apache.hadoop.security.UserGroupInformation
import org.apache.hive.service.cli.thrift.TProtocolVersion
import org.apache.spark.{KyuubiSparkUtil, SparkConf}
import scala.collection.mutable.{HashSet => MHSet}

import yaooqinn.kyuubi.{KyuubiSQLException, Logging}
import yaooqinn.kyuubi.auth.KyuubiAuthFactory
import yaooqinn.kyuubi.operation.{IKyuubiOperation, OperationHandle, OperationManager}
import yaooqinn.kyuubi.session.security.TokenCollector
import yaooqinn.kyuubi.utils.KyuubiHadoopUtil

abstract class AbstractKyuubiSession(
    protocol: TProtocolVersion,
    username: String,
    password: String,
    conf: SparkConf,
    ipAddress: String,
    withImpersonation: Boolean,
    sessionManager: SessionManager,
    operationManager: OperationManager) extends IKyuubiSession with Logging {

  protected val sessionHandle: SessionHandle = new SessionHandle(protocol)
  protected val opHandleSet = new MHSet[OperationHandle]
  @volatile protected var lastAccessTime: Long = System.currentTimeMillis()
  protected var lastIdleTime = 0L

  protected val sessionUGI: UserGroupInformation = {
    val currentUser = UserGroupInformation.getLoginUser
    if (withImpersonation) {
      if (UserGroupInformation.isSecurityEnabled) {
        if (conf.contains(KyuubiSparkUtil.PRINCIPAL) && conf.contains(KyuubiSparkUtil.KEYTAB)) {
          // If principal and keytab are configured, do re-login in case of token expiry.
          // Do not check keytab file existing as spark-submit has it done
          currentUser.reloginFromKeytab()
        }
        val user = UserGroupInformation.createProxyUser(username, currentUser)
        KyuubiHadoopUtil.doAs(user)(TokenCollector.obtainTokenIfRequired(conf))
        user
      } else {
        UserGroupInformation.createRemoteUser(username)
      }
    } else {
      currentUser
    }
  }

  protected def acquire(userAccess: Boolean): Unit = {
    if (userAccess) {
      lastAccessTime = System.currentTimeMillis
    }
  }

  protected def release(userAccess: Boolean): Unit = {
    if (userAccess) {
      lastAccessTime = System.currentTimeMillis
    }
    if (opHandleSet.isEmpty) {
      lastIdleTime = System.currentTimeMillis
    } else {
      lastIdleTime = 0
    }
  }

  @throws[KyuubiSQLException]
  protected def executeStatementInternal(statement: String): OperationHandle = {
    acquire(true)
    val operation =
      operationManager.newExecuteStatementOperation(this, statement)
    val opHandle = operation.getHandle
    try {
      operation.run()
      opHandleSet.add(opHandle)
      opHandle
    } catch {
      case e: KyuubiSQLException =>
        operationManager.closeOperation(opHandle)
        throw e
    } finally {
      release(true)
    }
  }

  override def ugi: UserGroupInformation = this.sessionUGI

  /**
    * execute operation handler
    *
    * @param statement sql statement
    * @return
    */
  @throws[KyuubiSQLException]
  override def executeStatement(statement: String): OperationHandle = {
    executeStatementInternal(statement)
  }

  /**
    * execute operation handler
    *
    * @param statement sql statement
    * @return
    */
  @throws[KyuubiSQLException]
  override def executeStatementAsync(statement: String): OperationHandle = {
    executeStatementInternal(statement)
  }


  override def cancelOperation(opHandle: OperationHandle): Unit = {
    acquire(true)
    try {
      operationManager.cancelOperation(opHandle)
    } finally {
      release(true)
    }
  }

  override def closeOperation(opHandle: OperationHandle): Unit = {
    acquire(true)
    try {
      operationManager.closeOperation(opHandle)
      opHandleSet.remove(opHandle)
    } finally {
      release(true)
    }
  }

  @throws[KyuubiSQLException]
  override def getDelegationToken(
                          authFactory: KyuubiAuthFactory,
                          owner: String,
                          renewer: String): String = {
    authFactory.getDelegationToken(owner, renewer)
  }

  @throws[KyuubiSQLException]
  override def cancelDelegationToken(authFactory: KyuubiAuthFactory, tokenStr: String): Unit = {
    authFactory.cancelDelegationToken(tokenStr)
  }

  @throws[KyuubiSQLException]
  override def renewDelegationToken(authFactory: KyuubiAuthFactory, tokenStr: String): Unit = {
    authFactory.renewDelegationToken(tokenStr)
  }

  override def closeExpiredOperations: Unit = {
    if (opHandleSet.nonEmpty) {
      closeTimedOutOperations(operationManager.removeExpiredOperations(opHandleSet.toSeq))
    }
  }

  private def closeTimedOutOperations(operations: Seq[IKyuubiOperation]): Unit = {
    acquire(false)
    try {
      operations.foreach { op =>
        opHandleSet.remove(op.getHandle)
        try {
          op.close()
        } catch {
          case e: Exception =>
            warn("Exception is thrown closing timed-out operation " + op.getHandle, e)
        }
      }
    } finally {
      release(false)
    }
  }

  override def getNoOperationTime: Long = {
    if (lastIdleTime > 0) {
      System.currentTimeMillis - lastIdleTime
    } else {
      0
    }
  }

  override def getProtocolVersion: TProtocolVersion = sessionHandle.getProtocolVersion

  override def getSessionHandle: SessionHandle = sessionHandle

  override def getPassword: String = password

  override def getIpAddress: String = ipAddress

  override def getLastAccessTime: Long = lastAccessTime

  override def getUserName: String = sessionUGI.getShortUserName

  override def getSessionMgr: SessionManager = sessionManager
}
