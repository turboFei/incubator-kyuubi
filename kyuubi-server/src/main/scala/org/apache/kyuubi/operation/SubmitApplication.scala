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

import java.nio.ByteBuffer
import java.util.{ArrayList => JArrayList}
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import com.fasterxml.jackson.annotation.{JsonAutoDetect, JsonInclude, JsonPropertyOrder}
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility
import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hive.service.rpc.thrift.{TColumn, TColumnDesc, TPrimitiveTypeEntry, TRow, TRowSet, TStringColumn, TTableSchema, TTypeDesc, TTypeEntry, TTypeId}

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.engine.EngineType
import org.apache.kyuubi.engine.EngineType.EngineType
import org.apache.kyuubi.engine.spark.SparkSubmitProcessBuilder
import org.apache.kyuubi.operation.FetchOrientation.{FETCH_FIRST, FETCH_NEXT, FETCH_PRIOR, FetchOrientation}
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.session.KyuubiSessionImpl

class SubmitApplication(
    session: KyuubiSessionImpl,
    engineType: EngineType,
    json: String,
    override val shouldRunAsync: Boolean)
  extends KyuubiOperation(OperationType.UNKNOWN_OPERATION, session) {

  override def statement: String = "SUBMIT_APPLICATION"

  private val submitCmd = SubmitApplicationProtocol.fromJson(json)
  if (submitCmd.mainClass == null) {
    throw KyuubiSQLException(s"The mainClass field of $json must be specified")
  }

  val normalizedSubmitConf =
    session.sessionManager.operationManager.validateSubmitConfig(submitCmd.conf)
  val sessionSubmitConf = session.sessionConf.getAll.flatMap { case (k, v) =>
    if (k.startsWith("kyuubi.operation.submit.spark.")) {
      Some(k.stripPrefix("kyuubi.operation.submit.") -> v)
    } else {
      None
    }
  }
  submitCmd.conf = sessionSubmitConf ++ normalizedSubmitConf
  info(s"Normalize the submit command from $json -> ${submitCmd.toJson}")

  @volatile
  private var appIdAndTrackingUrl: Option[(String, String)] = None

  private var resultFetched: Boolean = _

  private lazy val _operationLog: OperationLog =
    if (shouldRunAsync) {
      OperationLog.createOperationLog(session, getHandle)
    } else {
      // when launch engine synchronously, operation log is not needed
      null
    }
  override def getOperationLog: Option[OperationLog] = Option(_operationLog)

  override protected def beforeRun(): Unit = {
    OperationLog.setCurrentOperationLog(_operationLog)
    setHasResultSet(true)
    setState(OperationState.PENDING)
  }

  override protected def afterRun(): Unit = {
    OperationLog.removeCurrentOperationLog()
  }

  override protected def runInternal(): Unit = {
    val asyncOperation: Runnable = () => {
      setState(OperationState.RUNNING)
      try {
        doSubmitApplication()
        setState(OperationState.FINISHED)
      } catch onError()
    }
    try {
      val opHandle = session.sessionManager.submitBackgroundOperation(asyncOperation)
      setBackgroundHandle(opHandle)
    } catch onError("submitting submit application operation in background, request rejected")

    if (!shouldRunAsync) getBackgroundHandle.get()
  }

  private def doSubmitApplication(): Unit = {
    val builder = engineType match {
      case EngineType.SPARK_SUBMIT =>
        new SparkSubmitProcessBuilder(session.user, session.sessionConf, submitCmd, getOperationLog)
    }

    try {
      info(s"Submitting application: $builder")
      val process = builder.start
      val started = System.currentTimeMillis()
      var exitValue: Option[Int] = None
      while (appIdAndTrackingUrl.isEmpty) {
        if (exitValue.isEmpty && process.waitFor(1, TimeUnit.SECONDS)) {
          exitValue = Some(process.exitValue())
          if (exitValue.get != 0) {
            val error = builder.getError
            throw error
          }
        }

        appIdAndTrackingUrl = builder.getAppIdAndTrackingUrl()
        if (started + operationTimeout <= System.currentTimeMillis() &&
          appIdAndTrackingUrl.isEmpty) {
          process.destroyForcibly()
          throw KyuubiSQLException(
            s"Timeout($operationTimeout ms) for $engineType with $builder.",
            builder.getError)
        }
      }

      if (submitCmd.returnOnSubmitted) {
        process.destroyForcibly()
      } else {
        while (process.isAlive) {
          try {
            Thread.sleep(1000)
          } catch {
            case NonFatal(e) => error("Error waiting submission process completion", e)
          }
        }
        if (process.exitValue() != 0) {
          throw KyuubiSQLException(s"Process exit with value ${process.exitValue()}")
        }
      }
    } finally {
      // we must close the process builder whether session open is success or failure since
      // we have a log capture thread in process builder.
      builder.close()
    }
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

@JsonInclude(Include.NON_ABSENT)
@JsonAutoDetect(getterVisibility = Visibility.ANY, setterVisibility = Visibility.ANY)
@JsonPropertyOrder(alphabetic = true)
private[kyuubi] class SubmitApplicationProtocol {
  var conf: Map[String, String] = Map.empty
  var mainClass: String = null
  var resource: String = null
  var args: List[String] = List.empty
  var returnOnSubmitted: Boolean = true

  def toJson: String = {
    SubmitApplicationProtocol.mapper.writeValueAsString(this)
  }
}

private[kyuubi] object SubmitApplicationProtocol {
  private val mapper = new ObjectMapper()
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    .enable(SerializationFeature.INDENT_OUTPUT)
    .registerModule(DefaultScalaModule)

  def fromJson(json: String): SubmitApplicationProtocol = {
    mapper.readValue(json, classOf[SubmitApplicationProtocol])
  }
}
