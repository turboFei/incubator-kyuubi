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

import org.apache.spark.SparkConf
import org.scalatest.Outcome

import org.apache.kyuubi.Utils
import org.apache.kyuubi.plugin.spark.authz.AccessControlException
import org.apache.kyuubi.plugin.spark.authz.RangerTestNamespace._
import org.apache.kyuubi.plugin.spark.authz.RangerTestUsers._
import org.apache.kyuubi.plugin.spark.authz.util.AuthZUtils._
import org.apache.kyuubi.tags.HudiTest
import org.apache.kyuubi.util.AssertionUtils.interceptContains

/**
 * Tests for RangerSparkExtensionSuite on Hudi SQL.
 * Run this test should enbale `hudi` profile.
 */
@HudiTest
class HudiCatalogRangerSparkExtensionSuite extends RangerSparkExtensionSuite {
  override protected val catalogImpl: String = "in-memory"
  // TODO: Apache Hudi not support Spark 3.5 and Scala 2.13 yet,
  //  should change after Apache Hudi support Spark 3.5 and Scala 2.13.
  private def isSupportedVersion = !isSparkV35OrGreater && !isScalaV213

  override protected val sqlExtensions: String =
    if (isSupportedVersion) {
      "org.apache.spark.sql.hudi.HoodieSparkSessionExtension"
    } else {
      ""
    }

  override protected val extraSparkConf: SparkConf =
    new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

  val namespace1 = hudiNamespace
  val table1 = "table1_hoodie"
  val table2 = "table2_hoodie"
  val outputTable1 = "outputTable_hoodie"
  val index1 = "table_hoodie_index1"

  override def withFixture(test: NoArgTest): Outcome = {
    assume(isSupportedVersion)
    test()
  }

  override def beforeAll(): Unit = {
    if (isSupportedVersion) {
      if (isSparkV32OrGreater) {
        spark.conf.set(
          s"spark.sql.catalog.$sparkCatalog",
          "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
        spark.conf.set(s"spark.sql.catalog.$sparkCatalog.type", "hadoop")
        spark.conf.set(
          s"spark.sql.catalog.$sparkCatalog.warehouse",
          Utils.createTempDir("hudi-hadoop").toString)
      }
      super.beforeAll()
    }
  }

  override def afterAll(): Unit = {
    if (isSupportedVersion) {
      super.afterAll()
      spark.sessionState.catalog.reset()
      spark.sessionState.conf.clear()
    }
  }

  test("AlterTableCommand") {
    withCleanTmpResources(Seq((s"$namespace1.$table1", "table"), (namespace1, "database"))) {
      doAs(admin, sql(s"CREATE DATABASE IF NOT EXISTS $namespace1"))
      doAs(
        admin,
        sql(
          s"""
             |CREATE TABLE IF NOT EXISTS $namespace1.$table1(id int, name string, city string)
             |USING hudi
             |OPTIONS (
             | type = 'cow',
             | primaryKey = 'id',
             | 'hoodie.datasource.hive_sync.enable' = 'false'
             |)
             |PARTITIONED BY(city)
             |""".stripMargin))

      // AlterHoodieTableAddColumnsCommand
      interceptContains[AccessControlException](
        doAs(someone, sql(s"ALTER TABLE $namespace1.$table1 ADD COLUMNS(age int)")))(
        s"does not have [alter] privilege on [$namespace1/$table1/age]")

      // AlterHoodieTableChangeColumnCommand
      interceptContains[AccessControlException](
        doAs(someone, sql(s"ALTER TABLE $namespace1.$table1 CHANGE COLUMN id id bigint")))(
        s"does not have [alter] privilege" +
          s" on [$namespace1/$table1/id]")

      // AlterHoodieTableDropPartitionCommand
      interceptContains[AccessControlException](
        doAs(someone, sql(s"ALTER TABLE $namespace1.$table1 DROP PARTITION (city='test')")))(
        s"does not have [alter] privilege" +
          s" on [$namespace1/$table1/city]")

      // AlterHoodieTableRenameCommand
      interceptContains[AccessControlException](
        doAs(someone, sql(s"ALTER TABLE $namespace1.$table1 RENAME TO $namespace1.$table2")))(
        s"does not have [alter] privilege" +
          s" on [$namespace1/$table1]")

      // AlterTableCommand && Spark31AlterTableCommand
      try {
        sql("set hoodie.schema.on.read.enable=true")
        interceptContains[AccessControlException](
          doAs(someone, sql(s"ALTER TABLE $namespace1.$table1 ADD COLUMNS(age int)")))(
          s"does not have [alter] privilege on [$namespace1/$table1]")
      } finally {
        sql("set hoodie.schema.on.read.enable=false")
      }
    }
  }

  test("CreateHoodieTableCommand") {
    withCleanTmpResources(Seq((namespace1, "database"))) {
      doAs(admin, sql(s"CREATE DATABASE IF NOT EXISTS $namespace1"))
      interceptContains[AccessControlException](
        doAs(
          someone,
          sql(
            s"""
               |CREATE TABLE IF NOT EXISTS $namespace1.$table1(id int, name string, city string)
               |USING HUDI
               |OPTIONS (
               | type = 'cow',
               | primaryKey = 'id',
               | 'hoodie.datasource.hive_sync.enable' = 'false'
               |)
               |PARTITIONED BY(city)
               |""".stripMargin)))(s"does not have [create] privilege on [$namespace1/$table1]")
    }
  }

  test("CreateHoodieTableAsSelectCommand") {
    withCleanTmpResources(Seq((s"$namespace1.$table1", "table"), (namespace1, "database"))) {
      doAs(admin, sql(s"CREATE DATABASE IF NOT EXISTS $namespace1"))
      doAs(
        admin,
        sql(
          s"""
             |CREATE TABLE IF NOT EXISTS $namespace1.$table1(id int, name string, city string)
             |USING HUDI
             |OPTIONS (
             | type = 'cow',
             | primaryKey = 'id',
             | 'hoodie.datasource.hive_sync.enable' = 'false'
             |)
             |PARTITIONED BY(city)
             |""".stripMargin))
      interceptContains[AccessControlException](
        doAs(
          someone,
          sql(
            s"""
               |CREATE TABLE IF NOT EXISTS $namespace1.$table2
               |USING HUDI
               |AS
               |SELECT id FROM $namespace1.$table1
               |""".stripMargin)))(s"does not have [select] privilege on [$namespace1/$table1/id]")
    }
  }

  test("CreateHoodieTableLikeCommand") {
    withCleanTmpResources(Seq(
      (s"$namespace1.$table1", "table"),
      (s"$namespace1.$table2", "table"),
      (namespace1, "database"))) {
      doAs(admin, sql(s"CREATE DATABASE IF NOT EXISTS $namespace1"))
      doAs(
        admin,
        sql(
          s"""
             |CREATE TABLE IF NOT EXISTS $namespace1.$table1(id int, name string, city string)
             |USING HUDI
             |OPTIONS (
             | type = 'cow',
             | primaryKey = 'id',
             | 'hoodie.datasource.hive_sync.enable' = 'false'
             |)
             |PARTITIONED BY(city)
             |""".stripMargin))

      val createTableSql =
        s"""
           |CREATE TABLE IF NOT EXISTS $namespace1.$table2
           |LIKE  $namespace1.$table1
           |USING HUDI
           |""".stripMargin
      interceptContains[AccessControlException] {
        doAs(
          someone,
          sql(
            createTableSql))
      }(s"does not have [select] privilege on [$namespace1/$table1]")
      doAs(admin, sql(createTableSql))
    }
  }

  test("DropHoodieTableCommand") {
    withCleanTmpResources(Seq((namespace1, "database"))) {
      doAs(admin, sql(s"CREATE DATABASE IF NOT EXISTS $namespace1"))
      doAs(
        admin,
        sql(
          s"""
             |CREATE TABLE IF NOT EXISTS $namespace1.$table1(id int, name string, city string)
             |USING HUDI
             |OPTIONS (
             | type = 'cow',
             | primaryKey = 'id',
             | 'hoodie.datasource.hive_sync.enable' = 'false'
             |)
             |PARTITIONED BY(city)
             |""".stripMargin))

      val dropTableSql = s"DROP TABLE IF EXISTS $namespace1.$table1"
      interceptContains[AccessControlException] {
        doAs(someone, sql(dropTableSql))
      }(s"does not have [drop] privilege on [$namespace1/$table1]")
      doAs(admin, sql(dropTableSql))
    }
  }

  test("RepairHoodieTableCommand") {
    withCleanTmpResources(Seq((s"$namespace1.$table1", "table"), (namespace1, "database"))) {
      doAs(admin, sql(s"CREATE DATABASE IF NOT EXISTS $namespace1"))
      doAs(
        admin,
        sql(
          s"""
             |CREATE TABLE IF NOT EXISTS $namespace1.$table1(id int, name string, city string)
             |USING HUDI
             |OPTIONS (
             | type = 'cow',
             | primaryKey = 'id',
             | 'hoodie.datasource.hive_sync.enable' = 'false'
             |)
             |PARTITIONED BY(city)
             |""".stripMargin))

      val repairTableSql = s"MSCK REPAIR TABLE $namespace1.$table1"
      interceptContains[AccessControlException] {
        doAs(someone, sql(repairTableSql))
      }(s"does not have [alter] privilege on [$namespace1/$table1]")
      doAs(admin, sql(repairTableSql))
    }
  }

  test("TruncateHoodieTableCommand") {
    withCleanTmpResources(Seq((s"$namespace1.$table1", "table"), (namespace1, "database"))) {
      doAs(admin, sql(s"CREATE DATABASE IF NOT EXISTS $namespace1"))
      doAs(
        admin,
        sql(
          s"""
             |CREATE TABLE IF NOT EXISTS $namespace1.$table1(id int, name string, city string)
             |USING HUDI
             |OPTIONS (
             | type = 'cow',
             | primaryKey = 'id',
             | 'hoodie.datasource.hive_sync.enable' = 'false'
             |)
             |PARTITIONED BY(city)
             |""".stripMargin))

      val truncateTableSql = s"TRUNCATE TABLE $namespace1.$table1"
      interceptContains[AccessControlException] {
        doAs(someone, sql(truncateTableSql))
      }(s"does not have [update] privilege on [$namespace1/$table1]")
      doAs(admin, sql(truncateTableSql))
    }
  }

  test("CompactionHoodieTableCommand / CompactionShowHoodieTableCommand") {
    withCleanTmpResources(Seq((s"$namespace1.$table1", "table"), (namespace1, "database"))) {
      doAs(admin, sql(s"CREATE DATABASE IF NOT EXISTS $namespace1"))
      doAs(
        admin,
        sql(
          s"""
             |CREATE TABLE IF NOT EXISTS $namespace1.$table1(id int, name string, city string)
             |USING HUDI
             |OPTIONS (
             | type = 'mor',
             | primaryKey = 'id',
             | 'hoodie.datasource.hive_sync.enable' = 'false'
             |)
             |PARTITIONED BY(city)
             |""".stripMargin))

      val compactionTable = s"RUN COMPACTION ON $namespace1.$table1"
      interceptContains[AccessControlException] {
        doAs(someone, sql(compactionTable))
      }(s"does not have [select] privilege on [$namespace1/$table1]")
      doAs(admin, sql(compactionTable))

      val showCompactionTable = s"SHOW COMPACTION ON  $namespace1.$table1"
      interceptContains[AccessControlException] {
        doAs(someone, sql(showCompactionTable))
      }(s"does not have [select] privilege on [$namespace1/$table1]")
      doAs(admin, sql(showCompactionTable))
    }
  }

  test("InsertIntoHoodieTableCommand") {
    withSingleCallEnabled {
      withCleanTmpResources(Seq(
        (s"$namespace1.$table1", "table"),
        (s"$namespace1.$table2", "table"),
        (namespace1, "database"))) {
        doAs(admin, sql(s"CREATE DATABASE IF NOT EXISTS $namespace1"))
        doAs(
          admin,
          sql(
            s"""
               |CREATE TABLE IF NOT EXISTS $namespace1.$table1(id int, name string, city string)
               |USING HUDI
               |OPTIONS (
               | type = 'cow',
               | primaryKey = 'id',
               | 'hoodie.datasource.hive_sync.enable' = 'false'
               |)
               |PARTITIONED BY(city)
               |""".stripMargin))

        doAs(
          admin,
          sql(
            s"""
               |CREATE TABLE IF NOT EXISTS $namespace1.$table2(id int, name string, city string)
               |USING $format
               |""".stripMargin))

        val insertIntoHoodieTableSql =
          s"""
             |INSERT INTO $namespace1.$table1
             |PARTITION(city = 'hangzhou')
             |SELECT id, name
             |FROM $namespace1.$table2
             |WHERE city = 'hangzhou'
             |""".stripMargin
        interceptContains[AccessControlException] {
          doAs(someone, sql(insertIntoHoodieTableSql))
        }(s"does not have [select] privilege on " +
          s"[$namespace1/$table2/id,$namespace1/$table2/name,hudi_ns/$table2/city], " +
          s"[update] privilege on [$namespace1/$table1]")
      }
    }
  }

  test("ShowHoodieTablePartitionsCommand") {
    withSingleCallEnabled {
      withCleanTmpResources(Seq(
        (s"$namespace1.$table1", "table"),
        (s"$namespace1.$table2", "table"),
        (namespace1, "database"))) {
        doAs(admin, sql(s"CREATE DATABASE IF NOT EXISTS $namespace1"))
        doAs(
          admin,
          sql(
            s"""
               |CREATE TABLE IF NOT EXISTS $namespace1.$table1(id int, name string, city string)
               |USING HUDI
               |OPTIONS (
               | type = 'cow',
               | primaryKey = 'id',
               | 'hoodie.datasource.hive_sync.enable' = 'false'
               |)
               |PARTITIONED BY(city)
               |""".stripMargin))

        val showPartitionsSql = s"SHOW PARTITIONS $namespace1.$table1"
        interceptContains[AccessControlException] {
          doAs(someone, sql(showPartitionsSql))
        }(s"does not have [select] privilege on [$namespace1/$table1]")
        doAs(admin, sql(showPartitionsSql))

        val showPartitionSpecSql =
          s"SHOW PARTITIONS $namespace1.$table1 PARTITION (city = 'hangzhou')"
        interceptContains[AccessControlException] {
          doAs(someone, sql(showPartitionSpecSql))
        }(s"does not have [select] privilege on [$namespace1/$table1/city]")
        doAs(admin, sql(showPartitionSpecSql))
      }
    }
  }

  test("DeleteHoodieTableCommand/UpdateHoodieTableCommand/MergeIntoHoodieTableCommand") {
    withSingleCallEnabled {
      withCleanTmpResources(Seq(
        (s"$namespace1.$table1", "table"),
        (s"$namespace1.$table2", "table"),
        (namespace1, "database"))) {
        doAs(admin, sql(s"CREATE DATABASE IF NOT EXISTS $namespace1"))
        doAs(
          admin,
          sql(
            s"""
               |CREATE TABLE IF NOT EXISTS $namespace1.$table1(id int, name string, city string)
               |USING HUDI
               |OPTIONS (
               | type = 'cow',
               | primaryKey = 'id',
               | 'hoodie.datasource.hive_sync.enable' = 'false'
               |)
               |PARTITIONED BY(city)
               |""".stripMargin))

        doAs(
          admin,
          sql(
            s"""
               |CREATE TABLE IF NOT EXISTS $namespace1.$table2(id int, name string, city string)
               |USING HUDI
               |OPTIONS (
               | type = 'cow',
               | primaryKey = 'id',
               | 'hoodie.datasource.hive_sync.enable' = 'false'
               |)
               |PARTITIONED BY(city)
               |""".stripMargin))

        val deleteFrom = s"DELETE FROM $namespace1.$table1 WHERE id = 10"
        interceptContains[AccessControlException] {
          doAs(someone, sql(deleteFrom))
        }(s"does not have [update] privilege on [$namespace1/$table1]")
        doAs(admin, sql(deleteFrom))

        val updateSql = s"UPDATE $namespace1.$table1 SET name = 'test' WHERE id > 10"
        interceptContains[AccessControlException] {
          doAs(someone, sql(updateSql))
        }(s"does not have [update] privilege on [$namespace1/$table1]")
        doAs(admin, sql(updateSql))

        val mergeIntoSQL =
          s"""
             |MERGE INTO $namespace1.$table1 target
             |USING $namespace1.$table2 source
             |ON target.id = source.id
             |WHEN MATCHED
             |AND target.name == 'test'
             | THEN UPDATE SET id = source.id, name = source.name, city = source.city
             |""".stripMargin
        interceptContains[AccessControlException] {
          doAs(someone, sql(mergeIntoSQL))
        }(s"does not have [select] privilege on " +
          s"[$namespace1/$table2/id,$namespace1/$table2/name,$namespace1/$table2/city]")
        doAs(admin, sql(mergeIntoSQL))
      }
    }
  }

  test("CallProcedureHoodieCommand") {
    withSingleCallEnabled {
      withCleanTmpResources(Seq(
        (s"$namespace1.$table1", "table"),
        (s"$namespace1.$table2", "table"),
        (namespace1, "database"))) {
        doAs(admin, sql(s"CREATE DATABASE IF NOT EXISTS $namespace1"))
        doAs(
          admin,
          sql(
            s"""
               |CREATE TABLE IF NOT EXISTS $namespace1.$table1(id int, name string, city string)
               |USING HUDI
               |OPTIONS (
               | type = 'cow',
               | primaryKey = 'id',
               | 'hoodie.datasource.hive_sync.enable' = 'false'
               |)
               |PARTITIONED BY(city)
               |""".stripMargin))
        doAs(
          admin,
          sql(
            s"""
               |CREATE TABLE IF NOT EXISTS $namespace1.$table2(id int, name string, city string)
               |USING HUDI
               |OPTIONS (
               | type = 'cow',
               | primaryKey = 'id',
               | 'hoodie.datasource.hive_sync.enable' = 'false'
               |)
               |PARTITIONED BY(city)
               |""".stripMargin))

        val copy_to_table =
          s"CALL copy_to_table(table => '$namespace1.$table1', new_table => '$namespace1.$table2')"
        interceptContains[AccessControlException] {
          doAs(someone, sql(copy_to_table))
        }(s"does not have [select] privilege on [$namespace1/$table1]")
        doAs(admin, sql(copy_to_table))

        val show_table_properties = s"CALL show_table_properties(table => '$namespace1.$table1')"
        interceptContains[AccessControlException] {
          doAs(someone, sql(show_table_properties))
        }(s"does not have [select] privilege on [$namespace1/$table1]")
        doAs(admin, sql(show_table_properties))
      }
    }
  }

  test("IndexBasedCommand") {
    assume(
      !isSparkV33OrGreater,
      "Hudi index creation not supported on Spark 3.3 or greater currently")
    withCleanTmpResources(Seq((s"$namespace1.$table1", "table"), (namespace1, "database"))) {
      doAs(admin, sql(s"CREATE DATABASE IF NOT EXISTS $namespace1"))
      doAs(
        admin,
        sql(
          s"""
             |CREATE TABLE IF NOT EXISTS $namespace1.$table1(id int, name string, city string)
             |USING HUDI
             |OPTIONS (
             | type = 'cow',
             | primaryKey = 'id',
             | 'hoodie.datasource.hive_sync.enable' = 'false'
             |)
             |PARTITIONED BY(city)
             |""".stripMargin))

      // CreateIndexCommand
      val createIndex = s"CREATE INDEX $index1 ON $namespace1.$table1 USING LUCENE (id)"
      interceptContains[AccessControlException](
        doAs(
          someone,
          sql(createIndex)))(s"does not have [index] privilege on [$namespace1/$table1]")
      doAs(admin, sql(createIndex))

      // RefreshIndexCommand
      val refreshIndex = s"REFRESH INDEX $index1 ON $namespace1.$table1"
      interceptContains[AccessControlException](
        doAs(
          someone,
          sql(refreshIndex)))(s"does not have [alter] privilege on [$namespace1/$table1]")
      doAs(admin, sql(refreshIndex))

      // ShowIndexesCommand
      val showIndex = s"SHOW INDEXES FROM TABLE $namespace1.$table1"
      interceptContains[AccessControlException](
        doAs(
          someone,
          sql(showIndex)))(s"does not have [select] privilege on [$namespace1/$table1]")
      doAs(admin, sql(showIndex))

      // DropIndexCommand
      val dropIndex = s"DROP INDEX $index1 ON $namespace1.$table1"
      interceptContains[AccessControlException](
        doAs(
          someone,
          sql(dropIndex)))(s"does not have [drop] privilege on [$namespace1/$table1]")
      doAs(admin, sql(dropIndex))
    }
  }
}
