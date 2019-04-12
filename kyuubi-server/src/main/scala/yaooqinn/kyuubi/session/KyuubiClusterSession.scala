/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package yaooqinn.kyuubi.session

import java.util.concurrent.locks.ReentrantLock

import org.apache.hive.service.cli.thrift._
import org.apache.spark.{KyuubiConf, SparkConf}
import org.apache.spark.sql.types.StructType
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.{TSocket, TTransport}
import scala.collection.JavaConverters._

import yaooqinn.kyuubi.KyuubiSQLException
import yaooqinn.kyuubi.auth.PlainSaslHelper
import yaooqinn.kyuubi.cli.{FetchOrientation, FetchType, GetInfoType, GetInfoValue}
import yaooqinn.kyuubi.operation.{OperationHandle, OperationManager}
import yaooqinn.kyuubi.schema.RowSet
import yaooqinn.kyuubi.server.KyuubiServer
import yaooqinn.kyuubi.utils.KyuubiHadoopUtil
import yaooqinn.kyuubi.yarn.KyuubiAppMasterWithUGI

private[kyuubi] class KyuubiClusterSession(
    protocol: TProtocolVersion,
    username: String,
    password: String,
    conf: SparkConf,
    ipAddress: String,
    withImpersonation: Boolean,
    sessionManager: SessionManager,
    operationManager: OperationManager)
  extends AbstractKyuubiSession(protocol,
    username, password, conf, ipAddress, withImpersonation, sessionManager, operationManager) {

  import KyuubiClusterSession._

  private val kyuubiAppMasterWithUGI: KyuubiAppMasterWithUGI =
    new KyuubiAppMasterWithUGI(sessionUGI, conf)
  private var client: TCLIService.Iface = _
  private var transport: TTransport = _
  private val transportLock: ReentrantLock = new ReentrantLock(true)
  private var thriftSessHandle: TSessionHandle = _
  private val maxThriftAttempts =
    conf.getInt(KyuubiConf.SESSION_CLUSTER_CONNECTION_MAX_ATTEMPTS.key, 3)
  private var thriftFailCount = 0

  def getConf: SparkConf = conf

  @throws[KyuubiSQLException]
  override def open(sessionConf: Map[String, String]): Unit = {
    kyuubiAppMasterWithUGI.init(sessionConf)
    KyuubiHadoopUtil.doAs(sessionUGI) {
      while (!getThriftConnection(sessionConf)) {
        Thread.sleep(1000)
      }
    }
    lastAccessTime = System.currentTimeMillis
    lastIdleTime = lastAccessTime
  }

  /**
   * Just for unit test with an local kyuubiServer.
   */
  @throws[KyuubiSQLException]
  private def mockOpen(server: KyuubiServer): Unit = {
    client = server.feService
    openThriftConnection(Map.empty)
    lastAccessTime = System.currentTimeMillis
    lastIdleTime = lastAccessTime
  }

  private def getThriftConnection(sessionConf: Map[String, String]): Boolean = {
    try {
      val instance = kyuubiAppMasterWithUGI.kyuubiAmInstance.get
      val splitInstance = instance.split(":")
      val host = splitInstance(0)
      val port = splitInstance(1).toInt

      transport = new TSocket(host, port)
      transport = PlainSaslHelper.getPlainTransport(username, "", transport)
      if (!transport.isOpen) {
        info(s"Will try to open client transport for $username")
        transport.open()
      }
      client = new TCLIService.Client(new TBinaryProtocol(transport))
      openThriftConnection(sessionConf)
    } catch {
      case e: Exception =>
        thriftFailCount += 1
        if (thriftFailCount < maxThriftAttempts) {
          error(s"Error occured when get thriftConnection for $username $thriftFailCount times" +
            " and retry.")
          closeThriftConnection()
          false
        } else {
          throw new KyuubiSQLException(s"Get Thrift connection for [$username] failed" +
            s" $maxThriftAttempts times.", e)
        }
    }
  }

  private def openThriftConnection(sessionConf: Map[String, String]): Boolean = {
    val openReq = new TOpenSessionReq()
    openReq.setConfiguration(sessionConf.asJava)
    openReq.setUsername(username)
    openReq.setPassword(password)

    try {
      val openResp = client.OpenSession(openReq)
      verifySuccess(openResp.getStatus)
      thriftSessHandle = openResp.getSessionHandle
      true
    } catch {
      case e: Exception =>
        throw new KyuubiSQLException(e.toString, "08S01", e)
    }
  }

  private def closeThriftConnection(): Unit = {
    try {
      if (client != null) {
        if (thriftSessHandle != null) {
          val closeReq = new TCloseSessionReq(thriftSessHandle)
          client.CloseSession(closeReq)
        }
      }
    } catch {
      case e: Exception =>
        error("Error occured when close the ThriftConnection", e)
    } finally {
      if (transport != null) {
        transport.close()
      }
    }
  }

  override def close(): Unit = {
    acquire(true)
    closeThriftConnection()
    release(true)
  }

  override def getInfo(getInfoType: GetInfoType): GetInfoValue = {
    throw new KyuubiSQLException("Method not Implemented!")
  }

  override def fetchResults(
      opHandle: OperationHandle,
      orientation: FetchOrientation,
      maxRows: Long,
      fetchType: FetchType): RowSet = {
    throw new KyuubiSQLException("Method not Implemented!")
  }

  override def getResultSetMetadata(opHandle: OperationHandle): StructType = {
    throw new KyuubiSQLException("Method not Implemented!")
  }

  def getInfoResp(tGetInfoReq: TGetInfoReq): TGetInfoResp = {
    acquire(true)
    try {
      val req = new TGetInfoReq(thriftSessHandle, tGetInfoReq.getInfoType)
      transportLock.lock()
      try {
        val resp = client.GetInfo(req)
        verifySuccess(resp.getStatus)
        resp
      } finally {
        transportLock.unlock()
      }
    } catch {
      case e: Exception =>
        warn("Error getting info: ", e)
        val resp = new TGetInfoResp()
        resp.setStatus(KyuubiSQLException.toTStatus(e))
        resp
    } finally {
      release(true)
    }
  }

  def thriftClient: TCLIService.Iface = client

  def thriftHandle: TSessionHandle = thriftSessHandle

  def thriftLock: ReentrantLock = transportLock
}

object KyuubiClusterSession {
  def verifySuccess (status: TStatus, withInfo: Boolean = false): Unit = {
    if (!(status.getStatusCode() == TStatusCode.SUCCESS_STATUS ||
      (withInfo && status.getStatusCode() == TStatusCode.SUCCESS_WITH_INFO_STATUS))) {
      throw new KyuubiSQLException(status.getErrorMessage, status.getSqlState)
    }
  }
}