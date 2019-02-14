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

import org.apache.commons.io.FileUtils
import org.apache.hive.service.cli.thrift.TProtocolVersion
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

import yaooqinn.kyuubi.{KyuubiSQLException, Logging}
import yaooqinn.kyuubi.cli._
import yaooqinn.kyuubi.operation.{KyuubiOperation, OperationHandle, OperationManager}
import yaooqinn.kyuubi.spark.SparkSessionWithUGI

/**
 * An Execution Session with [[SparkSession]] instance inside, which shares [[SparkContext]]
 * with other sessions create by the same user.
 *
 * One user, one [[SparkContext]]
 * One user, multi [[KyuubiSession]]s
 *
 * One [[KyuubiSession]], one [[SparkSession]]
 * One [[SparkContext]], multi [[SparkSession]]s
 *
 */
private[kyuubi] class KyuubiSession(
    val protocol: TProtocolVersion,
    val username: String,
    val password: String,
    val conf: SparkConf,
    val ipAddress: String,
    val withImpersonation: Boolean,
    val sessionManager: SessionManager,
    val operationManager: OperationManager) extends Session with Logging {

  private val sparkSessionWithUGI =
    new SparkSessionWithUGI(sessionUGI, conf, sessionManager.getCacheMgr)

  private def acquire(userAccess: Boolean): Unit = {
    if (userAccess) {
      lastAccessTime = System.currentTimeMillis
    }
  }

  private def release(userAccess: Boolean): Unit = {
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
  private def executeStatementInternal(statement: String): OperationHandle = {
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

  private def cleanupSessionLogDir(): Unit = {
    if (_isOperationLogEnabled) {
      try {
        FileUtils.forceDelete(sessionLogDir)
      } catch {
        case e: Exception =>
          error("Failed to cleanup session log dir: " + sessionLogDir, e)
      }
    }
  }

  def sparkSession: SparkSession = this.sparkSessionWithUGI.sparkSession

  @throws[KyuubiSQLException]
  def open(sessionConf: Map[String, String]): Unit = {
    sparkSessionWithUGI.init(sessionConf)
    lastAccessTime = System.currentTimeMillis
    lastIdleTime = lastAccessTime
  }

  def getInfo(getInfoType: GetInfoType): GetInfoValue = {
    acquire(true)
    try {
      getInfoType match {
        case GetInfoType.SERVER_NAME => new GetInfoValue("Kyuubi Server")
        case GetInfoType.DBMS_NAME => new GetInfoValue("Spark SQL")
        case GetInfoType.DBMS_VERSION =>
          new GetInfoValue(this.sparkSessionWithUGI.sparkSession.version)
        case _ =>
          throw new KyuubiSQLException("Unrecognized GetInfoType value " + getInfoType.toString)
      }
    } finally {
      release(true)
    }
  }

  /**
   * execute operation handler
   *
   * @param statement sql statement
   * @return
   */
  @throws[KyuubiSQLException]
  def executeStatement(statement: String): OperationHandle = {
    executeStatementInternal(statement)
  }

  /**
   * execute operation handler
   *
   * @param statement sql statement
   * @return
   */
  @throws[KyuubiSQLException]
  def executeStatementAsync(statement: String): OperationHandle = {
    executeStatementInternal(statement)
  }

  private def closeTimedOutOperations(operations: Seq[KyuubiOperation]): Unit = {
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
}
