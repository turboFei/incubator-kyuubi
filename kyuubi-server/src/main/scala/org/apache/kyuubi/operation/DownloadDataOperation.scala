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

  override protected def executeStatement(): Unit = {
    try {
      _remoteOpHandle = client.downloadData(tableName, query, format, options)
      setHasResultSet(_remoteOpHandle.isHasResultSet)
    } catch onError()
  }
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
