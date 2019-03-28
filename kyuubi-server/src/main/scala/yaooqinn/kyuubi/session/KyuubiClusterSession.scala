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

import java.util.concurrent.locks.ReentrantLock

import org.apache.hive.service.cli.thrift._
import org.apache.hive.service.cli.thrift.TCLIService
import org.apache.hive.service.cli.thrift.TCloseSessionReq
import org.apache.hive.service.cli.thrift.TGetInfoReq
import org.apache.hive.service.cli.thrift.TOpenSessionReq
import org.apache.hive.service.cli.thrift.TProtocolVersion
import org.apache.hive.service.cli.thrift.TSessionHandle
import org.apache.spark.SparkConf
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.{TSocket, TTransport}
import scala.collection.JavaConverters._

import yaooqinn.kyuubi.KyuubiSQLException
import yaooqinn.kyuubi.auth.PlainSaslHelper
import yaooqinn.kyuubi.cli._
import yaooqinn.kyuubi.operation.OperationManager
import yaooqinn.kyuubi.server.KyuubiServer
import yaooqinn.kyuubi.utils.KyuubiHadoopUtil
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

  import KyuubiClusterSession._

  private val kyuubiAppMasterWithUGI: KyuubiAppMasterWithUGI =
    new KyuubiAppMasterWithUGI(sessionUGI, conf)
  private var client: TCLIService.Iface = _
  private var transport: TTransport = _
  private val transportLock: ReentrantLock = new ReentrantLock(true)
  private var thriftSessHandle: TSessionHandle = _

  def getConf: SparkConf = conf

  @throws[KyuubiSQLException]
  def open(sessionConf: Map[String, String]): Unit = {
    kyuubiAppMasterWithUGI.init(sessionConf)
    KyuubiHadoopUtil.doAs(sessionUGI) {
      getThriftConnection(sessionConf)
    }
    lastAccessTime = System.currentTimeMillis
    lastIdleTime = lastAccessTime
  }

  /**
   * Just for unit test with an local kyuubiServer.
   */
  @throws[KyuubiSQLException]
  def mockOpen(server: KyuubiServer): Unit = {
    client = server.feService
    openThriftConnection(Map.empty)
    lastAccessTime = System.currentTimeMillis
    lastIdleTime = lastAccessTime
  }

  def getInfo(getInfoType: GetInfoType): GetInfoValue = {
    throw new KyuubiSQLException("Method Not Implemented!")
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
        throw new KyuubiSQLException(e.toString, "08S01", e)
    } finally {
      release(true)
    }
  }

  def stopShared(): Unit = {
    // TODO: Though the KyuubiAppMaster will unregister itself
  }

  private def getThriftConnection(sessionConf: Map[String, String]): Unit = {
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
        error(s"Error occured when get thriftConnection for $username.")
        throw new KyuubiSQLException(s"Get Thrift connection for [$username] failed", e)
    }
  }

  private def openThriftConnection(sessionConf: Map[String, String]): Unit = {
    val openReq = new TOpenSessionReq()
    openReq.setConfiguration(sessionConf.asJava)
    openReq.setUsername(username)
    openReq.setPassword(password)

    try {
      val openResp = client.OpenSession(openReq)
      verifySuccess(openResp.getStatus)
      thriftSessHandle = openResp.getSessionHandle
    } catch {
      case e: Exception =>
        throw new KyuubiSQLException(e.toString, "08S01", e)
    }
  }

  override def close(): Unit = {
    acquire(true)

    val closeReq = new TCloseSessionReq(thriftSessHandle)
    try {
      client.CloseSession(closeReq)
    } catch {
      case e: Exception =>
        throw new KyuubiSQLException("Error occured when close the ThriftConnection", e)
    } finally {
      if (transport != null) {
        transport.close()
      }
    }

    release(true)
    super.close()
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
