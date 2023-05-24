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

import scala.collection.JavaConverters._

import org.apache.hive.service.rpc.thrift.{TI64Value, TRowSet}

import org.apache.kyuubi.Utils
import org.apache.kyuubi.operation.FetchOrientation.FetchOrientation
import org.apache.kyuubi.session.Session

class DownloadDataOperation(
    session: Session,
    tableName: String,
    query: String,
    format: String,
    options: Map[String, String])
  extends ExecuteStatement(
    session,
    DownloadDataOperation.statement(tableName, query, format, options),
    Map.empty,
    true,
    0L) {

  @volatile private var _downloadDataSize = 0L

  override protected def executeStatement(): Unit = {
    try {
      _remoteOpHandle = client.downloadData(tableName, query, format, options)
      setHasResultSet(_remoteOpHandle.isHasResultSet)
    } catch onError()
  }

  override def getNextRowSet(order: FetchOrientation, rowSetSize: Int): TRowSet = {
    val tRowSet = super.getNextRowSet(order, rowSetSize)
    Utils.tryLogNonFatalError {
      if (tRowSet.getColumnsSize > 0) {
        tRowSet.getColumns.asScala.lastOption.foreach { col =>
          if (col.getI64Val.isSetValues) {
            col.getI64Val.getValues.asScala.foreach { value =>
              Option(value).foreach(_downloadDataSize += _)
            }
          }
        }
      } else {
        tRowSet.getRows.asScala.foreach { row =>
          row.getColVals.asScala.lastOption.foreach { col =>
            if (col.getI64Val.isSetValue) {
              _downloadDataSize +=
                col.getI64Val.getFieldValue(TI64Value._Fields.VALUE).asInstanceOf[Long]
            }
          }
        }
      }
    }
    tRowSet
  }

  override def metrics: Map[String, String] = super.metrics ++ Map(
    "downloadDataSize" -> _downloadDataSize.toString)
}

object DownloadDataOperation {
  private def statement(
      tableName: String,
      query: String,
      format: String,
      options: Map[String, String]): String = {
    s"Downloading data with arguments [$tableName, $query, $format, $options]"
  }
}
