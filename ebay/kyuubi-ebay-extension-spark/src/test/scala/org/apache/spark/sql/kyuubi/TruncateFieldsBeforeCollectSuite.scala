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
package org.apache.spark.sql.kyuubi

import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, QueryTest, SparkSession}
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.kyuubi.FieldsTruncationHandler.{getTypeLevelTruncationEnabledKey, TRUNCATION_KEY_PREFIX}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{BinaryType, StringType}

import org.apache.kyuubi.Utils

class TruncateFieldsBeforeCollectSuite extends QueryTest with SharedSparkSession {

  override def sparkConf(): SparkConf = {
    val metastorePath = Utils.createTempDir()
    val warehousePath = Utils.createTempDir()
    warehousePath.toFile.delete()
    metastorePath.toFile.delete()
    super.sparkConf
      .set(
        StaticSQLConf.SPARK_SESSION_EXTENSIONS.key,
        "org.apache.spark.sql.kyuubi.KyuubiEbaySparkSQLExtension")
      .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
      .set("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict")
      .set("spark.hadoop.hive.metastore.client.capability.check", "false")
      .set(
        ConfVars.METASTORECONNECTURLKEY.varname,
        s"jdbc:derby:;databaseName=$metastorePath;create=true")
      .set(StaticSQLConf.WAREHOUSE_PATH, warehousePath.toString)
      .set("spark.ui.enabled", "false")
  }

  def withViewContext(viewNames: String*)(f: () => Unit): Unit = {
    try {
      f.apply()
    } catch {
      case e: Throwable => e.printStackTrace()
    } finally {
      viewNames.foreach { name =>
        spark.sql(s"DROP VIEW IF EXISTS $name")
      }
    }
  }

  def runWithView(
      tempViewName: String,
      createTableFn: (SparkSession) => Dataset[_],
      fn: (SparkSession) => Unit): Unit = {
    val ds = createTableFn(spark)
    ds.createOrReplaceTempView(tempViewName)
    withViewContext(tempViewName)(() => fn(spark))
  }

  // positive cases
  test("test truncation enabled and disable") {
    val createTable = (spark: SparkSession) => {
      import spark.implicits._
      Seq(Person(1, "Andy", 32), Person(2, "Michael", 28), Person(3, "Justin", 19))
        .toDS()
    }

    runWithView(
      "people",
      createTable,
      (spark) => {

        // with truncation enabled
        withSQLConf(
          KyuubiEbaySQLConf.FIELDS_TRUNCATION_ENABLED.key -> "true",
          // truncate the name size to 2
          KyuubiEbaySQLConf.FIELDS_TRUNCATION_STRING_TYPE_MAX_LENGTH.key -> "2") {

          // project with order
          var ds = spark.sql("select * from people order by age desc")
          ds.explain(true)

          // the name should be truncated
          var nameTruncated = ds.limit(2).collect()
          assert(2 == nameTruncated.length)

          var andy = nameTruncated.find(row => row.getAs[Long]("id").equals(1L)).get
          assert("An".equals(andy.getAs[String]("name")))
          var michael = nameTruncated.find(row => row.getAs[Long]("id").equals(2L)).get
          assert("Mi".equals(michael.getAs[String]("name")))

          // the name should be truncated
          nameTruncated = ds.take(2)
          assert(2 == nameTruncated.length)

          andy = nameTruncated.find(row => row.getAs[Long]("id").equals(1L)).get
          assert("An".equals(andy.getAs[String]("name")))
          michael = nameTruncated.find(row => row.getAs[Long]("id").equals(2L)).get
          assert("Mi".equals(michael.getAs[String]("name")))

          // the name should be truncated
          nameTruncated = ds.collect()
          assert(3 == nameTruncated.length)

          andy = nameTruncated.find(row => row.getAs[Long]("id").equals(1L)).get
          assert("An".equals(andy.getAs[String]("name")))
          michael = nameTruncated.find(row => row.getAs[Long]("id").equals(2L)).get
          assert("Mi".equals(michael.getAs[String]("name")))
          var justin = nameTruncated.find(row => row.getAs[Long]("id").equals(3L)).get
          assert("Ju".equals(justin.getAs[String]("name")))

          // project only
          ds = spark.sql("select * from people")
          nameTruncated = ds.collect()
          assert(3 == nameTruncated.length)

          andy = nameTruncated.find(row => row.getAs[Long]("id").equals(1L)).get
          assert("An".equals(andy.getAs[String]("name")))
          michael = nameTruncated.find(row => row.getAs[Long]("id").equals(2L)).get
          assert("Mi".equals(michael.getAs[String]("name")))
          justin = nameTruncated.find(row => row.getAs[Long]("id").equals(3L)).get
          assert("Ju".equals(justin.getAs[String]("name")))
        }

        // disable truncation
        withSQLConf(
          // truncation disabled
          KyuubiEbaySQLConf.FIELDS_TRUNCATION_ENABLED.key -> "false",
          // truncate the name size to 2 but won't work
          KyuubiEbaySQLConf.FIELDS_TRUNCATION_STRING_TYPE_MAX_LENGTH.key -> "2") {
          val ds = spark.sql("select * from people order by age desc")
          ds.explain(true)

          // the name should be truncated
          val nameTruncated = ds.limit(2).collect()
          assert(2 == nameTruncated.length)

          val andy = nameTruncated.find(row => row.getAs[Long]("id").equals(1L)).get
          assert("Andy".equals(andy.getAs[String]("name")))
          val michael = nameTruncated.find(row => row.getAs[Long]("id").equals(2L)).get
          assert("Michael".equals(michael.getAs[String]("name")))
        }
      })
  }

  // negative cases
  test("test cases that truncation won't take effects") {
    val createTable = (spark: SparkSession) => {
      import spark.implicits._
      Seq(Person(1, "Andy", 32), Person(2, "Michael", 28), Person(3, "Justin", 19))
        .toDS()
    }

    runWithView(
      "people",
      createTable,
      (spark) => {
        withSQLConf(
          KyuubiEbaySQLConf.FIELDS_TRUNCATION_ENABLED.key -> "true",
          // truncate the name size to 2
          KyuubiEbaySQLConf.FIELDS_TRUNCATION_STRING_TYPE_MAX_LENGTH.key -> "2") {

          // project with order
          var ds = spark.sql("select * from people order by name desc")

          var noTruncate = ds.collect()
          assert(3 == noTruncate.length)

          var andy = noTruncate.find(row => row.getAs[Long]("id").equals(1L)).get
          assert("Andy".equals(andy.getAs[String]("name")))
          var michael = noTruncate.find(row => row.getAs[Long]("id").equals(2L)).get
          assert("Michael".equals(michael.getAs[String]("name")))

          ds = spark.sql("select * from people order by substring(name, 0, 1) desc")

          noTruncate = ds.collect()
          assert(3 == noTruncate.length)

          andy = noTruncate.find(row => row.getAs[Long]("id").equals(1L)).get
          assert("Andy".equals(andy.getAs[String]("name")))
          michael = noTruncate.find(row => row.getAs[Long]("id").equals(2L)).get
          assert("Michael".equals(michael.getAs[String]("name")))
        }
      })
  }

  test("test user customized truncation impl") {
    val createTable = (spark: SparkSession) => {
      import spark.implicits._
      Seq(
        Book(1, "Book1", "Some data from book1".getBytes),
        Book(2, "Book2", "Some data from book2".getBytes),
        Book(3, "Book3", "Some data from book3".getBytes))
        .toDS()
    }

    runWithView(
      "book",
      createTable,
      (spark) => {
        withSQLConf(
          KyuubiEbaySQLConf.FIELDS_TRUNCATION_ENABLED.key -> "true",

          // disable string truncation
          s"${getTypeLevelTruncationEnabledKey(StringType)}" -> "false",
          // below won't work because the string type truncation is disabled
          KyuubiEbaySQLConf.FIELDS_TRUNCATION_STRING_TYPE_MAX_LENGTH.key -> "2",

          // use customized truncate logic
          s"$TRUNCATION_KEY_PREFIX.${BinaryType.typeName}.impl" -> classOf[
            BinaryTypeFieldTruncationHandler].getName) {

          // project with order
          val ds = spark.sql("select * from book order by id asc")

          val dataTruncated = ds.limit(2).collect()
          assert(2 == dataTruncated.length)

          val book1 = dataTruncated.find(row => row.getAs[Long]("id").equals(1L)).get
          // no truncate for name
          assert("Book1".equals(book1.getAs[String]("name")))
          // the data should be truncated
          assert("@Binary".equals(book1.getAs[String]("data")))
        }
      })
  }

}
