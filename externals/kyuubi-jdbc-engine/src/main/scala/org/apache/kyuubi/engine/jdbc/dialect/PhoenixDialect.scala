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
package org.apache.kyuubi.engine.jdbc.dialect
import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.commons.lang3.StringUtils

import org.apache.kyuubi.engine.jdbc.phoenix.PhoenixSchemaHelper
import org.apache.kyuubi.engine.jdbc.schema.{DefaultJdbcTRowSetGenerator, JdbcTRowSetGenerator, SchemaHelper}
import org.apache.kyuubi.operation.meta.ResultSetSchemaConstant._
import org.apache.kyuubi.session.Session

class PhoenixDialect extends JdbcDialect {

  override def getTablesQuery(
      catalog: String,
      schema: String,
      tableName: String,
      tableTypes: util.List[String]): String = {
    val tTypes =
      if (tableTypes == null || tableTypes.isEmpty) {
        Set("s", "u")
      } else {
        tableTypes.asScala.toSet
      }
    val query = new StringBuilder(
      s"""
         |SELECT TENANT_ID, TABLE_SCHEM, TABLE_NAME, COLUMN_NAME, COLUMN_FAMILY,
         |TABLE_SEQ_NUM, TABLE_TYPE, PK_NAME,
         |COLUMN_COUNT, SALT_BUCKETS, DATA_TABLE_NAME, INDEX_STATE
         |IMMUTABLE_ROWS, VIEW_STATEMENT
         |FROM SYSTEM.CATALOG
         |""".stripMargin)

    val filters = ArrayBuffer[String]()

    if (StringUtils.isNotBlank(tableName)) {
      filters += s"$TABLE_NAME LIKE '$tableName'"
    }

    if (tTypes.nonEmpty) {
      filters += s"(${tTypes.map { tableType => s"$TABLE_TYPE = '$tableType'" }
          .mkString(" OR ")})"
    }

    if (filters.nonEmpty) {
      query.append(" WHERE ")
      query.append(filters.mkString(" AND "))
    }

    query.toString()
  }

  override def getColumnsQuery(
      session: Session,
      catalogName: String,
      schemaName: String,
      tableName: String,
      columnName: String): String = {
    val query = new StringBuilder(
      """
        |SELECT TENANT_ID, TABLE_SCHEM, TABLE_NAME, COLUMN_NAME, COLUMN_FAMILY,
        |TABLE_SEQ_NUM, TABLE_TYPE, PK_NAME,
        |COLUMN_COUNT, SALT_BUCKETS, DATA_TABLE_NAME, INDEX_STATE
        |IMMUTABLE_ROWS, VIEW_STATEMENT
        |FROM SYSTEM.CATALOG
        |""".stripMargin)

    val filters = ArrayBuffer[String]()

    if (StringUtils.isNotEmpty(tableName)) {
      filters += s"$TABLE_NAME LIKE '$tableName'"
    }
    if (StringUtils.isNotEmpty(columnName)) {
      filters += s"$COLUMN_NAME LIKE '$columnName'"
    }

    if (filters.nonEmpty) {
      query.append(" WHERE ")
      query.append(filters.mkString(" AND "))
    }

    query.toString()
  }

  override def getTRowSetGenerator(): JdbcTRowSetGenerator = new DefaultJdbcTRowSetGenerator

  override def getSchemaHelper(): SchemaHelper = {
    new PhoenixSchemaHelper
  }

  override def name(): String = {
    "phoenix"
  }
}
