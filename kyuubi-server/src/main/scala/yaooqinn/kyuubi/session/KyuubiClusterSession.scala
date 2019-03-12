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

import java.sql.DriverManager
import java.util.Properties

import org.apache.hive.jdbc.HiveConnection
import org.apache.hive.service.cli.thrift.TProtocolVersion
import org.apache.spark.KyuubiConf._
import org.apache.spark.SparkConf

import yaooqinn.kyuubi
import yaooqinn.kyuubi.KyuubiSQLException
import yaooqinn.kyuubi.cli._
import yaooqinn.kyuubi.operation.{OperationHandle, OperationManager}
import yaooqinn.kyuubi.yarn.{KyuubiAppMaster, KyuubiAppMasterWithUGI}

/**
 * An Execution Session with [[KyuubiAppMaster]] instance inside, which shared
 * with other sessions create by the same user.
 *
 * One user, one [[KyuubiAppMaster]]
 * One user, multi [[KyuubiClusterSession]]s
 */
private[kyuubi] class KyuubiClusterSession(
    protocol: TProtocolVersion,
    username: String,
    password: String,
    conf: SparkConf,
    ipAddress: String,
    withImpersonation: Boolean,
    sessionManager: SessionManager,
    operationManager: OperationManager)
  extends Session(protocol,
    username,
    password,
    conf,
    ipAddress,
    withImpersonation,
    sessionManager,
    operationManager) {

  private val kyuubiAppMasterWithUGI = new KyuubiAppMasterWithUGI(sessionUGI, conf)
  private var _jdbcUrl: String = _
  private var _connection: HiveConnection = _

  def getConf: SparkConf = conf

  def getHiveConnection(): HiveConnection = {
    if (_connection == null || _connection.isClosed) {
      try {
        _connection = DriverManager.getConnection(_jdbcUrl).asInstanceOf[HiveConnection]
      } catch {
        case e: Exception =>
          throw new KyuubiSQLException(s"Error occured when creating HiveConnection for $username.")
      }
    }
    _connection
  }

  @throws[KyuubiSQLException]
  def open(sessionConf: Map[String, String]): Unit = {
    kyuubiAppMasterWithUGI.init()
    _jdbcUrl = makeJdbcUrl(sessionConf)
    _connection = getHiveConnection()
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
          new GetInfoValue(getHiveConnection.getMetaData.getDatabaseProductVersion)
        case _ =>
          throw new KyuubiSQLException("Unrecognized GetInfoType value " + getInfoType.toString)
      }
    } finally {
      release(true)
    }
  }

  def stopShared(): Unit = {
    // The KyuubiAppMaster will unregister itself
  }

  def makeJdbcUrl(sessionConf: Map[String, String]): String = {
    val url = new StringBuilder
    url.append("jdbc:hive2://")
      .append(conf.get(HA_ZOOKEEPER_QUORUM.key))
      .append("/;")
      .append("serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=")
      .append(kyuubiAppMasterWithUGI.kyuubiAmInstanceNameSpace)
      .append(";principal=")
      .append(conf.get("spark.yarn.principal"))

    if (sessionConf.size > 0) {
      url.append("#")
      for ((k, v) <- sessionConf) {
        url.append(k).append("=").append(v).append(";")
      }
    }
    url.mkString
  }

}
