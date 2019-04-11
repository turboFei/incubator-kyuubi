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
import org.apache.spark.sql.types.StructType

import yaooqinn.kyuubi.KyuubiSQLException
import yaooqinn.kyuubi.auth.KyuubiAuthFactory
import yaooqinn.kyuubi.cli.{FetchOrientation, FetchType, GetInfoType, GetInfoValue}
import yaooqinn.kyuubi.operation.OperationHandle
import yaooqinn.kyuubi.schema.RowSet

trait IKyuubiSession {

  def ugi: UserGroupInformation

  @throws[KyuubiSQLException]
  def open(sessionConf: Map[String, String]): Unit

  def getInfo(getInfoType: GetInfoType): GetInfoValue

  @throws[KyuubiSQLException]
  def executeStatement(statement: String): OperationHandle

  @throws[KyuubiSQLException]
  def executeStatementAsync(statement: String): OperationHandle

  @throws[KyuubiSQLException]
  def close(): Unit

  def cancelOperation(opHandle: OperationHandle): Unit

  def closeOperation(opHandle: OperationHandle): Unit

  def getResultSetMetadata(opHandle: OperationHandle): StructType

  @throws[KyuubiSQLException]
  def fetchResults(
      opHandle: OperationHandle,
      orientation: FetchOrientation,
      maxRows: Long,
      fetchType: FetchType): RowSet

  @throws[KyuubiSQLException]
  def getDelegationToken(authFactory: KyuubiAuthFactory, owner: String, renewer: String): String

  @throws[KyuubiSQLException]
  def cancelDelegationToken(authFactory: KyuubiAuthFactory, tokenStr: String): Unit

  @throws[KyuubiSQLException]
  def renewDelegationToken(authFactory: KyuubiAuthFactory, tokenStr: String): Unit

  def closeExpiredOperations: Unit

  def getNoOperationTime: Long

  def getProtocolVersion: TProtocolVersion

  def getSessionHandle: SessionHandle

  def getPassword: String

  def getIpAddress: String

  def getLastAccessTime: Long

  def getUserName: String

  def getSessionMgr: SessionManager
}
