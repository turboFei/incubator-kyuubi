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

package org.apache.kyuubi.engine.spark.operation

import org.apache.spark.sql.types._

import org.apache.kyuubi.engine.spark.shim.SparkCatalogShim
import org.apache.kyuubi.operation.IterableFetchIterator
import org.apache.kyuubi.operation.meta.ResultSetSchemaConstant._
import org.apache.kyuubi.session.Session

class GetColumns(
    session: Session,
    catalogName: String,
    schemaName: String,
    tableName: String,
    columnName: String)
  extends SparkOperation(session) {

  override def statement: String = {
    super.statement +
      s" [catalog: $catalogName," +
      s" schemaPattern: $schemaName," +
      s" tablePattern: $tableName," +
      s" columnPattern: $columnName]"
  }

  override protected def resultSchema: StructType = {
    new StructType()
      .add(TABLE_CAT, "string", nullable = true, "Catalog name. NULL if not applicable")
      .add(TABLE_SCHEM, "string", nullable = true, "Schema name")
      .add(TABLE_NAME, "string", nullable = true, "Table name")
      .add(COLUMN_NAME, "string", nullable = true, "Column name")
      .add(DATA_TYPE, "int", nullable = true, "SQL type from java.sql.Types")
      .add(
        TYPE_NAME,
        "string",
        nullable = true,
        "Data source dependent type name, for a UDT" +
          " the type name is fully qualified")
      .add(
        COLUMN_SIZE,
        "int",
        nullable = true,
        "Column size. For char or date types this is" +
          " the maximum number of characters, for numeric or decimal types this is precision.")
      .add(BUFFER_LENGTH, "tinyint", nullable = true, "Unused")
      .add(DECIMAL_DIGITS, "int", nullable = true, "he number of fractional digits")
      .add(NUM_PREC_RADIX, "int", nullable = true, "Radix (typically either 10 or 2)")
      .add(NULLABLE, "int", nullable = true, "Is NULL allowed")
      .add(REMARKS, "string", nullable = true, "Comment describing column (may be null)")
      .add(COLUMN_DEF, "string", nullable = true, "Default value (may be null)")
      .add(SQL_DATA_TYPE, "int", nullable = true, "Unused")
      .add(SQL_DATETIME_SUB, "int", nullable = true, "Unused")
      .add(
        CHAR_OCTET_LENGTH,
        "int",
        nullable = true,
        "For char types the maximum number of bytes in the column")
      .add(ORDINAL_POSITION, "int", nullable = true, "Index of column in table (starting at 1)")
      .add(
        IS_NULLABLE,
        "string",
        nullable = true,
        "'NO' means column definitely does not allow NULL values; 'YES' means the column might" +
          " allow NULL values. An empty string means nobody knows.")
      .add(
        SCOPE_CATALOG,
        "string",
        nullable = true,
        "Catalog of table that is the scope of a reference attribute "
          + "(null if DATA_TYPE isn't REF)")
      .add(
        SCOPE_SCHEMA,
        "string",
        nullable = true,
        "Schema of table that is the scope of a reference attribute "
          + "(null if the DATA_TYPE isn't REF)")
      .add(
        SCOPE_TABLE,
        "string",
        nullable = true,
        "Table name that this the scope of a reference attribute "
          + "(null if the DATA_TYPE isn't REF)")
      .add(
        SOURCE_DATA_TYPE,
        "smallint",
        nullable = true,
        "Source type of a distinct type or user-generated Ref type, "
          + "SQL type from java.sql.Types (null if DATA_TYPE isn't DISTINCT or user-generated REF)")
      .add(
        IS_AUTO_INCREMENT,
        "string",
        nullable = true,
        "Indicates whether this column is auto incremented.")
  }

  override protected def runInternal(): Unit = {
    try {
      val schemaPattern = toJavaRegex(schemaName)
      val tablePattern = toJavaRegex(tableName)
      val columnPattern = toJavaRegex(columnName)
      iter = new IterableFetchIterator(SparkCatalogShim()
        .getColumns(spark, catalogName, schemaPattern, tablePattern, columnPattern).toList)
    } catch {
      onError()
    }
  }
}
