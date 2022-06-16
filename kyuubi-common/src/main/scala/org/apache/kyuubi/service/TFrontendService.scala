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

package org.apache.kyuubi.service

import java.net.{InetAddress, ServerSocket}
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.JavaConverters._
import scala.language.implicitConversions

import org.apache.hadoop.conf.Configuration
import org.apache.hive.service.rpc.thrift._
import org.apache.thrift.protocol.TProtocol
import org.apache.thrift.server.{ServerContext, TServerEventHandler}
import org.apache.thrift.transport.TTransport

import org.apache.kyuubi.{KyuubiSQLException, Logging, Utils}
import org.apache.kyuubi.Utils.stringifyException
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.FRONTEND_CONNECTION_URL_USE_HOSTNAME
import org.apache.kyuubi.operation.{FetchOrientation, OperationHandle}
import org.apache.kyuubi.service.authentication.KyuubiAuthenticationFactory
import org.apache.kyuubi.session.SessionHandle
import org.apache.kyuubi.util.{KyuubiHadoopUtils, NamedThreadFactory}

/**
 * Apache Thrift based hive-service-rpc base class
 *   1. http
 *   2. binary
 */
abstract class TFrontendService(name: String)
  extends AbstractFrontendService(name) with TCLIService.Iface with Runnable with Logging {
  import TFrontendService._
  private val started = new AtomicBoolean(false)
  private lazy val serverThread = new NamedThreadFactory(getName, false).newThread(this)
  private var _hadoopConf: Configuration = _

  override def initialize(conf: KyuubiConf): Unit = {
    _hadoopConf = KyuubiHadoopUtils.newHadoopConf(conf)
    super.initialize(conf)
  }

  /**
   * According to session conf to get related hadoop conf.
   * For engine side, there is only one hadoop conf, but for kyuubi server side, there might be
   * multiple hadoop conf when session cluster mode is enabled.
   */
  protected def hadoopConf(sessionConf: Map[String, String]): Configuration = _hadoopConf

  protected def serverHost: Option[String]
  protected def portNum: Int
  protected lazy val serverAddr: InetAddress =
    serverHost.map(InetAddress.getByName).getOrElse(Utils.findLocalInetAddress)
  protected lazy val serverSocket = new ServerSocket(portNum, -1, serverAddr)
  protected lazy val actualPort: Int = serverSocket.getLocalPort
  protected lazy val authFactory: KyuubiAuthenticationFactory =
    new KyuubiAuthenticationFactory(conf, isServer())

  /**
   * Start the service itself(FE) and its composited (Discovery service, DS) in the order of:
   *   Start FE ->
   *     if (success) -> Continue starting DS
   *       if (success) -> finish
   *       else -> Stop DS -> Raise Error -> Stop FE -> Raise Error
   *     else
   *       Raise Error -> Stop FE -> Raise Error
   *    This makes sure that the FE has started and ready to serve before exposing through DS.
   */
  override def start(): Unit = synchronized {
    try {
      if (started.compareAndSet(false, true)) {
        serverThread.start()
      }
      super.start()
    } catch {
      case e: Throwable =>
        stopInternal()
        throw e
    }
  }

  protected def stopServer(): Unit

  /**
   * Inner stop progress that will not stop all services composited with this.
   */
  private def stopInternal(): Unit = {
    if (started.compareAndSet(true, false)) {
      serverThread.interrupt()
      stopServer()
      info(getName + " has stopped")
    }
  }

  /**
   * Stop the service itself(FE) and its composited (Discovery service, DS) in the order of:
   *   Stop DS -> Stop FE
   * This makes sure of
   *   1. The service stop serving before terminating during stopping
   *   2. For engines with group share level, the DS stopping is invoked by a pool in FE,
   *   so we need to stop DS first in case of interrupting.
   */
  override def stop(): Unit = synchronized {
    super.stop()
    stopInternal()
  }

  override def connectionUrl: String = {
    checkInitialized()
    val host = serverHost match {
      case Some(h) => h // respect user's setting ahead
      case None if conf.get(FRONTEND_CONNECTION_URL_USE_HOSTNAME) =>
        serverAddr.getHostName
      case None =>
        serverAddr.getHostAddress
    }

    host + ":" + actualPort
  }

  protected def getProxyUser(
      sessionConf: java.util.Map[String, String],
      ipAddress: String,
      realUser: String): String = {
    val proxyUser = sessionConf.get(KyuubiAuthenticationFactory.HS2_PROXY_USER)
    val batchAccount = sessionConf.get(KyuubiAuthenticationFactory.KYUUBI_PROXY_BATCH_ACCOUNT)
    if (proxyUser == null && batchAccount == null) {
      realUser
    } else if (proxyUser != null) {
      KyuubiAuthenticationFactory.verifyProxyAccess(
        realUser,
        proxyUser,
        ipAddress,
        hadoopConf(sessionConf.asScala.toMap))
      proxyUser
    } else {
      KyuubiAuthenticationFactory.verifyBatchAccountAccess(
        realUser,
        batchAccount,
        conf)
      batchAccount
    }
  }

  protected def getUserName(req: TOpenSessionReq): String = {
    val realUser: String =
      ServiceUtils.getShortName(authFactory.getRemoteUser.getOrElse(req.getUsername))
    if (req.getConfiguration == null || !isServer) {
      realUser
    } else {
      getProxyUser(req.getConfiguration, authFactory.getIpAddress.orNull, realUser)
    }
  }
  protected def getIpAddress: String = {
    authFactory.getIpAddress.orNull
  }

  protected def getMinVersion(versions: TProtocolVersion*): TProtocolVersion = {
    versions.minBy(_.getValue)
  }

  @throws[KyuubiSQLException]
  protected def getSessionHandle(req: TOpenSessionReq, res: TOpenSessionResp): SessionHandle = {
    val protocol = getMinVersion(SERVER_VERSION, req.getClient_protocol)
    res.setServerProtocolVersion(protocol)
    val userName = getUserName(req)
    val ipAddress = getIpAddress
    val configuration =
      Option(req.getConfiguration).map(_.asScala.toMap).getOrElse(Map.empty[String, String])
    val sessionHandle = be.openSession(
      protocol,
      userName,
      req.getPassword,
      ipAddress,
      configuration)
    sessionHandle
  }

  override def OpenSession(req: TOpenSessionReq): TOpenSessionResp = {
    debug(req.toString)
    info("Client protocol version: " + req.getClient_protocol)
    val resp = new TOpenSessionResp
    try {
      val sessionHandle = getSessionHandle(req, resp)
      resp.setSessionHandle(sessionHandle.toTSessionHandle)
      resp.setConfiguration(new java.util.HashMap[String, String]())
      resp.setStatus(OK_STATUS)
      Option(CURRENT_SERVER_CONTEXT.get()).foreach(_.setSessionHandle(sessionHandle))
    } catch {
      case e: Exception =>
        error("Error opening session: ", e)
        resp.setStatus(KyuubiSQLException.toTStatus(e, verbose = true))
    }
    resp
  }

  override def CloseSession(req: TCloseSessionReq): TCloseSessionResp = {
    debug(req.toString)
    val handle = SessionHandle(req.getSessionHandle)
    info(s"Received request of closing $handle")
    val resp = new TCloseSessionResp
    try {
      be.closeSession(handle)
      resp.setStatus(OK_STATUS)
    } catch {
      case e: Exception =>
        error("Error closing session: ", e)
        resp.setStatus(KyuubiSQLException.toTStatus(e))
    } finally {
      Option(CURRENT_SERVER_CONTEXT.get()).foreach(_.setSessionHandle(null))
    }
    info(s"Finished closing $handle")
    resp
  }

  override def GetInfo(req: TGetInfoReq): TGetInfoResp = {
    debug(req.toString)
    val resp = new TGetInfoResp
    try {
      val infoValue = be.getInfo(SessionHandle(req.getSessionHandle), req.getInfoType)
      resp.setInfoValue(infoValue)
      resp.setStatus(OK_STATUS)
    } catch {
      case e: Exception =>
        error("Error getting type info: ", e)
        resp.setInfoValue(TGetInfoValue.lenValue(0))
        resp.setStatus(KyuubiSQLException.toTStatus(e))
    }
    resp
  }

  override def ExecuteStatement(req: TExecuteStatementReq): TExecuteStatementResp = {
    debug(req.toString)
    val resp = new TExecuteStatementResp
    try {
      val sessionHandle = SessionHandle(req.getSessionHandle)
      val statement = req.getStatement
      val runAsync = req.isRunAsync
      val confOverlay = Option(req.getConfOverlay).getOrElse(Map.empty.asJava)
      val queryTimeout = req.getQueryTimeout
      val operationHandle = be.executeStatement(
        sessionHandle,
        statement,
        confOverlay.asScala.toMap,
        runAsync,
        queryTimeout)
      resp.setOperationHandle(operationHandle.toTOperationHandle)
      resp.setStatus(OK_STATUS)
    } catch {
      case e: Exception =>
        error("Error executing statement: ", e)
        resp.setStatus(KyuubiSQLException.toTStatus(e))
    }
    resp
  }

  override def GetTypeInfo(req: TGetTypeInfoReq): TGetTypeInfoResp = {
    debug(req.toString)
    val resp = new TGetTypeInfoResp
    try {
      val operationHandle = be.getTypeInfo(SessionHandle(req.getSessionHandle))
      resp.setOperationHandle(operationHandle.toTOperationHandle)
      resp.setStatus(OK_STATUS)
    } catch {
      case e: Exception =>
        error("Error getting type info: ", e)
        resp.setStatus(KyuubiSQLException.toTStatus(e))
    }
    resp
  }

  override def GetCatalogs(req: TGetCatalogsReq): TGetCatalogsResp = {
    debug(req.toString)
    val resp = new TGetCatalogsResp
    try {
      val opHandle = be.getCatalogs(SessionHandle(req.getSessionHandle))
      resp.setOperationHandle(opHandle.toTOperationHandle)
      resp.setStatus(OK_STATUS)
    } catch {
      case e: Exception =>
        error("Error getting catalogs: ", e)
        resp.setStatus(KyuubiSQLException.toTStatus(e))
    }
    resp
  }

  override def GetSchemas(req: TGetSchemasReq): TGetSchemasResp = {
    debug(req.toString)
    val resp = new TGetSchemasResp
    try {
      val opHandle = be.getSchemas(
        SessionHandle(req.getSessionHandle),
        req.getCatalogName,
        req.getSchemaName)
      resp.setOperationHandle(opHandle.toTOperationHandle)
      resp.setStatus(OK_STATUS)
    } catch {
      case e: Exception =>
        error("Error getting schemas: ", e)
        resp.setStatus(KyuubiSQLException.toTStatus(e))
    }
    resp
  }

  override def GetTables(req: TGetTablesReq): TGetTablesResp = {
    debug(req.toString)
    val resp = new TGetTablesResp
    try {
      val sessionHandle = SessionHandle(req.getSessionHandle)
      val catalog = req.getCatalogName
      val schema = req.getSchemaName
      val table = req.getTableName
      val tableTypes = req.getTableTypes
      val opHandle = be.getTables(sessionHandle, catalog, schema, table, tableTypes)
      resp.setOperationHandle(opHandle.toTOperationHandle)
      resp.setStatus(OK_STATUS)
    } catch {
      case e: Exception =>
        error("Error getting tables: ", e)
        resp.setStatus(KyuubiSQLException.toTStatus(e))
    }
    resp
  }

  override def GetTableTypes(req: TGetTableTypesReq): TGetTableTypesResp = {
    debug(req.toString)
    val resp = new TGetTableTypesResp
    try {
      val opHandle = be.getTableTypes(SessionHandle(req.getSessionHandle))
      resp.setOperationHandle(opHandle.toTOperationHandle)
      resp.setStatus(OK_STATUS)
    } catch {
      case e: Exception =>
        error("Error getting table types: ", e)
        resp.setStatus(KyuubiSQLException.toTStatus(e))
    }
    resp
  }

  override def GetColumns(req: TGetColumnsReq): TGetColumnsResp = {
    debug(req.toString)
    val resp = new TGetColumnsResp
    try {
      val sessionHandle = SessionHandle(req.getSessionHandle)
      val catalog = req.getCatalogName
      val schema = req.getSchemaName
      val table = req.getTableName
      val col = req.getColumnName
      val opHandle = be.getColumns(sessionHandle, catalog, schema, table, col)
      resp.setOperationHandle(opHandle.toTOperationHandle)
      resp.setStatus(OK_STATUS)
    } catch {
      case e: Exception =>
        error("Error getting columns: ", e)
        resp.setStatus(KyuubiSQLException.toTStatus(e))
    }
    resp
  }

  override def GetFunctions(req: TGetFunctionsReq): TGetFunctionsResp = {
    debug(req.toString)
    val resp = new TGetFunctionsResp
    try {
      val sessionHandle = SessionHandle(req.getSessionHandle)
      val catalog = req.getCatalogName
      val schema = req.getSchemaName
      val func = req.getFunctionName
      val opHandle = be.getFunctions(sessionHandle, catalog, schema, func)
      resp.setOperationHandle(opHandle.toTOperationHandle)
      resp.setStatus(OK_STATUS)
    } catch {
      case e: Exception =>
        error("Error getting functions: ", e)
        resp.setStatus(KyuubiSQLException.toTStatus(e))
    }
    resp
  }

  override def GetPrimaryKeys(req: TGetPrimaryKeysReq): TGetPrimaryKeysResp = {
    debug(req.toString)
    val resp = new TGetPrimaryKeysResp
    try {
      val sessionHandle = SessionHandle(req.getSessionHandle)
      val catalog = req.getCatalogName
      val schema = req.getSchemaName
      val table = req.getTableName
      val opHandle = be.getPrimaryKeys(sessionHandle, catalog, schema, table)
      resp.setOperationHandle(opHandle.toTOperationHandle)
      resp.setStatus(OK_STATUS)
    } catch {
      case e: Exception =>
        error("Error getting primary keys: ", e)
        resp.setStatus(KyuubiSQLException.toTStatus(e))
    }
    resp
  }

  override def GetCrossReference(req: TGetCrossReferenceReq): TGetCrossReferenceResp = {
    debug(req.toString)
    val resp = new TGetCrossReferenceResp
    try {
      val sessionHandle = SessionHandle(req.getSessionHandle)
      val primaryCatalog = req.getParentCatalogName
      val primarySchema = req.getParentSchemaName
      val primaryTable = req.getParentTableName
      val foreignCatalog = req.getForeignCatalogName
      val foreignSchema = req.getForeignSchemaName
      val foreignTable = req.getForeignTableName
      val opHandle = be.getCrossReference(
        sessionHandle,
        primaryCatalog,
        primarySchema,
        primaryTable,
        foreignCatalog,
        foreignSchema,
        foreignTable)
      resp.setOperationHandle(opHandle.toTOperationHandle)
      resp.setStatus(OK_STATUS)
    } catch {
      case e: Exception =>
        error("Error getting primary keys: ", e)
        resp.setStatus(KyuubiSQLException.toTStatus(e))
    }
    resp
  }

  override def GetOperationStatus(req: TGetOperationStatusReq): TGetOperationStatusResp = {
    debug(req.toString)
    val resp = new TGetOperationStatusResp
    try {
      val operationHandle = OperationHandle(req.getOperationHandle)
      val operationStatus = be.getOperationStatus(operationHandle)
      resp.setOperationState(operationStatus.state)
      resp.setOperationStarted(operationStatus.start)
      resp.setOperationCompleted(operationStatus.completed)
      resp.setHasResultSet(operationStatus.hasResultSet)
      operationStatus.exception.foreach { e =>
        resp.setSqlState(e.getSQLState)
        resp.setErrorCode(e.getErrorCode)
        resp.setErrorMessage(stringifyException(e))
      }
      resp.setNumModifiedRows(operationStatus.numModifiedRows)
      operationStatus.operationProgressUpdate.foreach { p =>
        resp.setProgressUpdateResponse(p)
      }
      resp.setStatus(OK_STATUS)
    } catch {
      case e: Exception =>
        error("Error getting operation status: ", e)
        resp.setStatus(KyuubiSQLException.toTStatus(e))
    }
    resp
  }

  override def CancelOperation(req: TCancelOperationReq): TCancelOperationResp = {
    debug(req.toString)
    val resp = new TCancelOperationResp
    try {
      be.cancelOperation(OperationHandle(req.getOperationHandle))
      resp.setStatus(OK_STATUS)
    } catch {
      case e: Exception =>
        error("Error cancelling operation: ", e)
        resp.setStatus(KyuubiSQLException.toTStatus(e))
    }
    resp
  }

  override def CloseOperation(req: TCloseOperationReq): TCloseOperationResp = {
    debug(req.toString)
    val resp = new TCloseOperationResp
    try {
      be.closeOperation(OperationHandle(req.getOperationHandle))
      resp.setStatus(OK_STATUS)
    } catch {
      case e: Exception =>
        error("Error closing operation: ", e)
        resp.setStatus(KyuubiSQLException.toTStatus(e))
    }
    resp
  }

  override def GetResultSetMetadata(req: TGetResultSetMetadataReq): TGetResultSetMetadataResp = {
    debug(req.toString)
    val resp = new TGetResultSetMetadataResp
    try {
      val schema = be.getResultSetMetadata(OperationHandle(req.getOperationHandle))
      resp.setSchema(schema)
      resp.setStatus(OK_STATUS)
    } catch {
      case e: Exception =>
        error("Error getting result set metadata: ", e)
        resp.setStatus(KyuubiSQLException.toTStatus(e))
    }
    resp
  }

  override def FetchResults(req: TFetchResultsReq): TFetchResultsResp = {
    debug(req.toString)
    val resp = new TFetchResultsResp
    try {
      val operationHandle = OperationHandle(req.getOperationHandle)
      val orientation = FetchOrientation.getFetchOrientation(req.getOrientation)
      // 1 means fetching log
      val fetchLog = req.getFetchType == 1
      val maxRows = req.getMaxRows.toInt
      val rowSet = be.fetchResults(operationHandle, orientation, maxRows, fetchLog)
      resp.setResults(rowSet)
      resp.setHasMoreRows(false)
      resp.setStatus(OK_STATUS)
    } catch {
      case e: Exception =>
        error("Error fetching results: ", e)
        resp.setStatus(KyuubiSQLException.toTStatus(e))
    }
    resp
  }

  protected def notSupportTokenErrorStatus = {
    val errorStatus = new TStatus(TStatusCode.ERROR_STATUS)
    errorStatus.setErrorMessage("Delegation token is not supported")
    errorStatus
  }

  override def GetDelegationToken(req: TGetDelegationTokenReq): TGetDelegationTokenResp = {
    debug(req.toString)
    val resp = new TGetDelegationTokenResp()
    resp.setStatus(notSupportTokenErrorStatus)
    resp
  }

  override def CancelDelegationToken(req: TCancelDelegationTokenReq): TCancelDelegationTokenResp = {
    debug(req.toString)
    val resp = new TCancelDelegationTokenResp
    resp.setStatus(notSupportTokenErrorStatus)
    resp
  }

  override def RenewDelegationToken(req: TRenewDelegationTokenReq): TRenewDelegationTokenResp = {
    debug(req.toString)
    val resp = new TRenewDelegationTokenResp
    resp.setStatus(OK_STATUS)
    resp
  }

  override def DownloadData(req: TDownloadDataReq): TDownloadDataResp = {
    debug(req.toString)
    val resp = new TDownloadDataResp()
    resp.setStatus(KyuubiSQLException.featureNotSupported().toTStatus)
    resp
  }

  override def TransferData(req: TTransferDataReq): TTransferDataResp = {
    debug(req.toString)
    val resp = new TTransferDataResp()
    resp.setStatus(KyuubiSQLException.featureNotSupported().toTStatus)
    resp
  }

  protected def isServer(): Boolean = false

  class FeTServerEventHandler extends TServerEventHandler {
    implicit def toFeServiceServerContext(context: ServerContext): FeServiceServerContext = {
      context.asInstanceOf[FeServiceServerContext]
    }

    override def deleteContext(context: ServerContext, in: TProtocol, out: TProtocol): Unit = {
      val handle = context.getSessionHandle
      if (handle != null) {
        info(s"Session [$handle] disconnected without closing properly, close it now")
        try {
          be.closeSession(handle)
        } catch {
          case e: KyuubiSQLException =>
            error("Failed closing session", e)
        }
      }
    }

    override def processContext(context: ServerContext, in: TTransport, out: TTransport): Unit = {
      CURRENT_SERVER_CONTEXT.set(context)
    }

    override def preServe(): Unit = {}

    override def createContext(in: TProtocol, out: TProtocol): ServerContext = {
      new FeServiceServerContext()
    }
  }

}

private[kyuubi] object TFrontendService {
  final val OK_STATUS = new TStatus(TStatusCode.SUCCESS_STATUS)

  final val CURRENT_SERVER_CONTEXT = new ThreadLocal[FeServiceServerContext]()

  final val SERVER_VERSION = TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10

  class FeServiceServerContext extends ServerContext {
    private var sessionHandle: SessionHandle = _

    def setSessionHandle(sessionHandle: SessionHandle): Unit = {
      this.sessionHandle = sessionHandle
    }

    def getSessionHandle: SessionHandle = sessionHandle
  }
}
