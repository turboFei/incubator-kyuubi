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

package org.apache.spark.sql.catalyst.data

import java.util.Locale

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.execution.command.{DDLUtils, DescribeCommandBase}
import org.apache.spark.sql.execution.datasources.{DataSource, FileFormat, LogicalRelation}

import org.apache.kyuubi.{KyuubiSQLException, Logging}
import org.apache.kyuubi.config.KyuubiEbayConf
import org.apache.kyuubi.engine.spark.SparkSQLEngine

/**
 * A KYUUBI DESCRIBE PATH command, as parsed from SQL
 */
case class KyuubiDescribePathCommand(identifier: String, isExtended: Boolean)
  extends DescribeCommandBase with Logging {
  private val datasourceProviderList =
    SparkSQLEngine.kyuubiConf.get(KyuubiEbayConf.KYUUBI_DESCRIBE_PATH_DATA_SOURCES)

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val result = new ArrayBuffer[Row]()

    val (datasourceProvider, path) =
      sparkSession.sessionState.sqlParser.parseMultipartIdentifier(identifier) match {
        case Seq(_provider, _path) => Some(_provider) -> _path
        case Seq(_path) => None -> _path
        case _ => throw KyuubiSQLException("Invalid identifier as it has more than 2 name parts.")
      }

    val (inferredRelation, relationProvider) =
      inferPathLogicalRelation(sparkSession, datasourceProvider, path)
    describeSchema(inferredRelation.schema, result, false)
    if (isExtended) {
      describeFormattedPathInfo(result, relationProvider)
    }
    result
  }

  private def describeFormattedPathInfo(buffer: ArrayBuffer[Row], provider: String): Unit = {
    append(buffer, "", "", "")
    append(buffer, "# Detailed Path Information", "", "")
    append(buffer, "Datasource Provider", provider, "")
  }

  private def inferPathLogicalRelation(
      sparkSession: SparkSession,
      datasourceProvider: Option[String],
      path: String): (LogicalRelation, String) = {
    datasourceProvider.map(provider => {
      getLogicalRelation(sparkSession, provider, path) -> provider
    }).getOrElse {
      var relation: LogicalRelation = null
      var relationProvider: String = null

      for (provider <- datasourceProviderList if relation == null) {
        try {
          relation = getLogicalRelation(sparkSession, provider, path)
          relationProvider = provider
        } catch {
          case e: Throwable =>
            logWarning(s"Error inferring the datasource relation for `$path` with $provider", e)
        }
      }
      if (relation == null) {
        throw KyuubiSQLException(
          s"Failed to infer the datasource relation for `$path` with datasource" +
            s" providers[${datasourceProviderList.mkString("(", ",", ")")}].")
      }
      relation -> relationProvider
    }
  }

  /** Refer org.apache.spark.sql.execution.datasources.ResolveSQLOnFile. */
  private def getLogicalRelation(
      sparkSession: SparkSession,
      provider: String,
      path: String): LogicalRelation = {
    try {
      val dataSource = DataSource(
        sparkSession,
        paths = path :: Nil,
        className = provider)
      val isFileFormat = classOf[FileFormat].isAssignableFrom(dataSource.providingClass)
      if (!isFileFormat ||
        dataSource.className.toLowerCase(Locale.ROOT) == DDLUtils.HIVE_PROVIDER) {
        throw new AnalysisException("Unsupported data source type for direct query on files: " +
          s"${dataSource.className}")
      }
      val relation = LogicalRelation(dataSource.resolveRelation())
      info(s"Succeed to get logical relation with datasource[$provider] for $path")
      relation
    } catch {
      case e: Throwable =>
        throw KyuubiSQLException(s"Failed to get the logical relation for $provider.`$path`.", e)
    }
  }
}
