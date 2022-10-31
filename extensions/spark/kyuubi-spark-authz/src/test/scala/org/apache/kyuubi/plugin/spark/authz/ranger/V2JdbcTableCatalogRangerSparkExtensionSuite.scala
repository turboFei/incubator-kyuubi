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
package org.apache.kyuubi.plugin.spark.authz.ranger

import java.sql.DriverManager

import scala.util.Try

// scalastyle:off
import org.apache.kyuubi.plugin.spark.authz.AccessControlException

/**
 * Tests for RangerSparkExtensionSuite
 * on JdbcTableCatalog with DataSource V2 API.
 */
class V2JdbcTableCatalogRangerSparkExtensionSuite extends RangerSparkExtensionSuite {
  override protected val catalogImpl: String = "in-memory"

  val catalogV2 = "testcat"
  val jdbcCatalogV2 = "jdbc2"
  val namespace1 = "ns1"
  val namespace2 = "ns2"
  val table1 = "table1"
  val table2 = "table2"
  val outputTable1 = "outputTable1"
  val cacheTable1 = "cacheTable1"

  val dbUrl = s"jdbc:derby:memory:$catalogV2"
  val jdbcUrl: String = s"$dbUrl;create=true"

  override def beforeAll(): Unit = {
    if (isSparkV31OrGreater) {
      spark.conf.set(
        s"spark.sql.catalog.$catalogV2",
        "org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog")
      spark.conf.set(s"spark.sql.catalog.$catalogV2.url", jdbcUrl)
      spark.conf.set(
        s"spark.sql.catalog.$catalogV2.driver",
        "org.apache.derby.jdbc.AutoloadedDriver")

      super.beforeAll()

      doAs("admin", sql(s"CREATE DATABASE IF NOT EXISTS $catalogV2.$namespace1"))
      doAs(
        "admin",
        sql(s"CREATE TABLE IF NOT EXISTS $catalogV2.$namespace1.$table1" +
          " (id int, name string, city string)"))
      doAs(
        "admin",
        sql(s"CREATE TABLE IF NOT EXISTS $catalogV2.$namespace1.$outputTable1" +
          " (id int, name string, city string)"))
    }
  }

  override def afterAll(): Unit = {
    super.afterAll()
    spark.sessionState.catalog.reset()
    spark.sessionState.conf.clear()

    // cleanup db
    Try {
      DriverManager.getConnection(s"$dbUrl;shutdown=true")
    }
  }

  test("[KYUUBI #3424] CREATE DATABASE") {
    assume(isSparkV31OrGreater)

    // create database
    val e1 = intercept[AccessControlException](
      doAs("someone", sql(s"CREATE DATABASE IF NOT EXISTS $catalogV2.$namespace2").explain()))
    assert(e1.getMessage.contains(s"does not have [create] privilege" +
      s" on [$namespace2]"))
  }

  test("[KYUUBI #3424] DROP DATABASE") {
    assume(isSparkV31OrGreater)

    // create database
    val e1 = intercept[AccessControlException](
      doAs("someone", sql(s"DROP DATABASE IF EXISTS $catalogV2.$namespace2").explain()))
    assert(e1.getMessage.contains(s"does not have [drop] privilege" +
      s" on [$namespace2]"))
  }

  test("[KYUUBI #3424] SELECT TABLE") {
    assume(isSparkV31OrGreater)

    // select
    val e1 = intercept[AccessControlException](
      doAs("someone", sql(s"select city, id from $catalogV2.$namespace1.$table1").explain()))
    assert(e1.getMessage.contains(s"does not have [select] privilege" +
      s" on [$namespace1/$table1/id]"))
  }

  test("[KYUUBI #3424] CREATE TABLE") {
    assume(isSparkV31OrGreater)

    // CreateTable
    val e2 = intercept[AccessControlException](
      doAs("someone", sql(s"CREATE TABLE IF NOT EXISTS $catalogV2.$namespace1.$table2")))
    assert(e2.getMessage.contains(s"does not have [create] privilege" +
      s" on [$namespace1/$table2]"))

    // CreateTableAsSelect
    val e21 = intercept[AccessControlException](
      doAs(
        "someone",
        sql(s"CREATE TABLE IF NOT EXISTS $catalogV2.$namespace1.$table2" +
          s" AS select * from $catalogV2.$namespace1.$table1")))
    assert(e21.getMessage.contains(s"does not have [select] privilege" +
      s" on [$namespace1/$table1/id]"))
  }

  test("[KYUUBI #3424] DROP TABLE") {
    assume(isSparkV31OrGreater)

    // DropTable
    val e3 = intercept[AccessControlException](
      doAs("someone", sql(s"DROP TABLE $catalogV2.$namespace1.$table1")))
    assert(e3.getMessage.contains(s"does not have [drop] privilege" +
      s" on [$namespace1/$table1]"))
  }

  test("[KYUUBI #3424] INSERT TABLE") {
    assume(isSparkV31OrGreater)

    // AppendData: Insert Using a VALUES Clause
    val e4 = intercept[AccessControlException](
      doAs(
        "someone",
        sql(s"INSERT INTO $catalogV2.$namespace1.$outputTable1 (id, name, city)" +
          s" VALUES (1, 'bowenliang123', 'Guangzhou')")))
    assert(e4.getMessage.contains(s"does not have [update] privilege" +
      s" on [$namespace1/$outputTable1]"))

    // AppendData: Insert Using a TABLE Statement
    val e42 = intercept[AccessControlException](
      doAs(
        "someone",
        sql(s"INSERT INTO $catalogV2.$namespace1.$outputTable1 (id, name, city)" +
          s" TABLE $catalogV2.$namespace1.$table1")))
    assert(e42.getMessage.contains(s"does not have [select] privilege" +
      s" on [$namespace1/$table1/id]"))

    // AppendData: Insert Using a SELECT Statement
    val e43 = intercept[AccessControlException](
      doAs(
        "someone",
        sql(s"INSERT INTO $catalogV2.$namespace1.$outputTable1 (id, name, city)" +
          s" SELECT * from $catalogV2.$namespace1.$table1")))
    assert(e43.getMessage.contains(s"does not have [select] privilege" +
      s" on [$namespace1/$table1/id]"))

    // OverwriteByExpression: Insert Overwrite
    val e44 = intercept[AccessControlException](
      doAs(
        "someone",
        sql(s"INSERT OVERWRITE $catalogV2.$namespace1.$outputTable1 (id, name, city)" +
          s" VALUES (1, 'bowenliang123', 'Guangzhou')")))
    assert(e44.getMessage.contains(s"does not have [update] privilege" +
      s" on [$namespace1/$outputTable1]"))
  }

  test("[KYUUBI #3424] MERGE INTO") {
    assume(isSparkV31OrGreater)

    val mergeIntoSql =
      s"""
         |MERGE INTO $catalogV2.$namespace1.$outputTable1 AS target
         |USING $catalogV2.$namespace1.$table1  AS source
         |ON target.id = source.id
         |WHEN MATCHED AND (target.name='delete') THEN DELETE
         |WHEN MATCHED AND (target.name='update') THEN UPDATE SET target.city = source.city
      """.stripMargin

    // MergeIntoTable:  Using a MERGE INTO Statement
    val e1 = intercept[AccessControlException](
      doAs(
        "someone",
        sql(mergeIntoSql)))
    assert(e1.getMessage.contains(s"does not have [select] privilege" +
      s" on [$namespace1/$table1/id]"))

    try {
      SparkRangerAdminPlugin.getRangerConf.setBoolean(
        s"ranger.plugin.${SparkRangerAdminPlugin.getServiceType}.authorize.in.single.call",
        true)
      val e2 = intercept[AccessControlException](
        doAs(
          "someone",
          sql(mergeIntoSql)))
      assert(e2.getMessage.contains(s"does not have" +
        s" [select] privilege" +
        s" on [$namespace1/$table1/id,$namespace1/table1/name,$namespace1/$table1/city]," +
        s" [update] privilege on [$namespace1/$outputTable1]"))
    } finally {
      SparkRangerAdminPlugin.getRangerConf.setBoolean(
        s"ranger.plugin.${SparkRangerAdminPlugin.getServiceType}.authorize.in.single.call",
        false)
    }
  }

  test("[KYUUBI #3424] UPDATE TABLE") {
    assume(isSparkV31OrGreater)

    // UpdateTable
    val e5 = intercept[AccessControlException](
      doAs(
        "someone",
        sql(s"UPDATE $catalogV2.$namespace1.$table1 SET city='Hangzhou' " +
          " WHERE id=1")))
    assert(e5.getMessage.contains(s"does not have [update] privilege" +
      s" on [$namespace1/$table1]"))
  }

  test("[KYUUBI #3424] DELETE FROM TABLE") {
    assume(isSparkV31OrGreater)

    // DeleteFromTable
    val e6 = intercept[AccessControlException](
      doAs("someone", sql(s"DELETE FROM $catalogV2.$namespace1.$table1 WHERE id=1")))
    assert(e6.getMessage.contains(s"does not have [update] privilege" +
      s" on [$namespace1/$table1]"))
  }

  test("[KYUUBI #3424] CACHE TABLE") {
    assume(isSparkV31OrGreater)

    // CacheTable
    val e7 = intercept[AccessControlException](
      doAs(
        "someone",
        sql(s"CACHE TABLE $cacheTable1" +
          s" AS select * from $catalogV2.$namespace1.$table1")))
    if (isSparkV32OrGreater) {
      assert(e7.getMessage.contains(s"does not have [select] privilege" +
        s" on [$namespace1/$table1/id]"))
    } else {
      assert(e7.getMessage.contains(s"does not have [select] privilege" +
        s" on [$catalogV2.$namespace1/$table1]"))
    }
  }

  test("[KYUUBI #3424] TRUNCATE TABLE") {
    assume(isSparkV32OrGreater)

    val e1 = intercept[AccessControlException](
      doAs(
        "someone",
        sql(s"TRUNCATE TABLE $catalogV2.$namespace1.$table1")))
    assert(e1.getMessage.contains(s"does not have [update] privilege" +
      s" on [$namespace1/$table1]"))
  }

  test("[KYUUBI #3424] MSCK REPAIR TABLE") {
    assume(isSparkV32OrGreater)

    val e1 = intercept[AccessControlException](
      doAs(
        "someone",
        sql(s"MSCK REPAIR TABLE $catalogV2.$namespace1.$table1")))
    assert(e1.getMessage.contains(s"does not have [alter] privilege" +
      s" on [$namespace1/$table1]"))
  }

  test("[KYUUBI #3424] ALTER TABLE") {
    assume(isSparkV31OrGreater)

    // AddColumns
    val e61 = intercept[AccessControlException](
      doAs(
        "someone",
        sql(s"ALTER TABLE $catalogV2.$namespace1.$table1 ADD COLUMNS (age int) ").explain()))
    assert(e61.getMessage.contains(s"does not have [alter] privilege" +
      s" on [$namespace1/$table1]"))

    // DropColumns
    val e62 = intercept[AccessControlException](
      doAs(
        "someone",
        sql(s"ALTER TABLE $catalogV2.$namespace1.$table1 DROP COLUMNS city ").explain()))
    assert(e62.getMessage.contains(s"does not have [alter] privilege" +
      s" on [$namespace1/$table1]"))

    // RenameColumn
    val e63 = intercept[AccessControlException](
      doAs(
        "someone",
        sql(s"ALTER TABLE $catalogV2.$namespace1.$table1 RENAME COLUMN city TO city2 ").explain()))
    assert(e63.getMessage.contains(s"does not have [alter] privilege" +
      s" on [$namespace1/$table1]"))

    // AlterColumn
    val e64 = intercept[AccessControlException](
      doAs(
        "someone",
        sql(s"ALTER TABLE $catalogV2.$namespace1.$table1 " +
          s"ALTER COLUMN city COMMENT 'city' ")))
    assert(e64.getMessage.contains(s"does not have [alter] privilege" +
      s" on [$namespace1/$table1]"))
  }

  test("[KYUUBI #3424] COMMENT ON") {
    assume(isSparkV31OrGreater)

    // CommentOnNamespace
    val e1 = intercept[AccessControlException](
      doAs(
        "someone",
        sql(s"COMMENT ON DATABASE $catalogV2.$namespace1 IS 'xYz' ").explain()))
    assert(e1.getMessage.contains(s"does not have [alter] privilege" +
      s" on [$namespace1]"))

    // CommentOnNamespace
    val e2 = intercept[AccessControlException](
      doAs(
        "someone",
        sql(s"COMMENT ON NAMESPACE $catalogV2.$namespace1 IS 'xYz' ").explain()))
    assert(e2.getMessage.contains(s"does not have [alter] privilege" +
      s" on [$namespace1]"))

    // CommentOnTable
    val e3 = intercept[AccessControlException](
      doAs(
        "someone",
        sql(s"COMMENT ON TABLE $catalogV2.$namespace1.$table1 IS 'xYz' ").explain()))
    assert(e3.getMessage.contains(s"does not have [alter] privilege" +
      s" on [$namespace1/$table1]"))

  }
}
