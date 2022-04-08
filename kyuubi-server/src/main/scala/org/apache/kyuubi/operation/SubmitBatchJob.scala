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

import java.util.{ArrayList => JArrayList}

import scala.collection.JavaConverters._

import org.apache.hive.service.rpc.thrift.{TColumn, TColumnDesc, TPrimitiveTypeEntry, TRow, TRowSet, TStringColumn, TTableSchema, TTypeDesc, TTypeEntry, TTypeId}
import org.apache.kyuubi.operation.FetchOrientation.{FETCH_FIRST, FETCH_NEXT, FETCH_PRIOR, FetchOrientation}
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.server.api.v1.BatchRequest
import org.apache.kyuubi.session.KyuubiBatchSessionImpl

import java.nio.ByteBuffer

class SubmitBatchJob(
    session: KyuubiBatchSessionImpl,
    resource: String,
    proxyUser: String,
    batchRequest: BatchRequest,
    override val shouldRunAsync: Boolean)
  extends KyuubiOperation(OperationType.UNKNOWN_OPERATION, session) {

  override def statement: String = "SUBMIT_BATCH_JOB"

  @volatile
  private var appIdAndTrackingUrl: Option[(String, String)] = None

  private var resultFetched: Boolean = _

  private lazy val _operationLog: OperationLog = OperationLog.createOperationLog(session, getHandle)

  override def getOperationLog: Option[OperationLog] = Option(_operationLog)

  override protected def beforeRun(): Unit = {
    OperationLog.setCurrentOperationLog(_operationLog)
    setHasResultSet(true)
    setState(OperationState.PENDING)
  }

  override protected def afterRun(): Unit = {
    OperationLog.removeCurrentOperationLog()
  }

  private def doSubmitBatchJob(): Unit = {
    val builder = batchRequest.batchType
  }

  override def getResultSetSchema: TTableSchema = {
    val tAppIdColumnDesc = new TColumnDesc()
    tAppIdColumnDesc.setColumnName("ApplicationId")
    val appIdDesc = new TTypeDesc
    appIdDesc.addToTypes(TTypeEntry.primitiveEntry(new TPrimitiveTypeEntry(TTypeId.STRING_TYPE)))
    tAppIdColumnDesc.setTypeDesc(appIdDesc)
    tAppIdColumnDesc.setPosition(0)

    val tUrlColumnDesc = new TColumnDesc()
    tUrlColumnDesc.setColumnName("URL")
    val urlDesc = new TTypeDesc
    urlDesc.addToTypes(TTypeEntry.primitiveEntry(new TPrimitiveTypeEntry(TTypeId.STRING_TYPE)))
    tUrlColumnDesc.setTypeDesc(urlDesc)
    tUrlColumnDesc.setPosition(1)

    val schema = new TTableSchema()
    schema.addToColumns(tAppIdColumnDesc)
    schema.addToColumns(tUrlColumnDesc)
    schema
  }

  override def getNextRowSet(order: FetchOrientation, rowSetSize: Int): TRowSet = {
    validateDefaultFetchOrientation(order)
    assertState(OperationState.FINISHED)
    setHasResultSet(true)
    order match {
      case FETCH_NEXT => fetchNext()
      case FETCH_PRIOR => resultSet
      case FETCH_FIRST => resultSet
    }
  }

  lazy val resultSet: TRowSet = {
    val tRow = new TRowSet(0, new JArrayList[TRow](1))
    val (appId, url) = appIdAndTrackingUrl.toSeq.unzip

    val tAppIdColumn = TColumn.stringVal(new TStringColumn(
      appId.asJava,
      ByteBuffer.allocate(0)))

    val tUrlColumn = TColumn.stringVal(new TStringColumn(
      url.asJava,
      ByteBuffer.allocate(0)))

    tRow.addToColumns(tAppIdColumn)
    tRow.addToColumns(tUrlColumn)
    tRow
  }

  private def fetchNext(): TRowSet = {
    if (!resultFetched) {
      resultFetched = true
      resultSet
    } else {
      new TRowSet(0, new JArrayList[TRow](0))
    }
  }
}
