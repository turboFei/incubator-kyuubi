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

import org.apache.spark.sql.Row

import org.apache.kyuubi.Utils
import org.apache.kyuubi.config.KyuubiEbayConf
import org.apache.kyuubi.engine.spark.WithSparkSQLEngine
import org.apache.kyuubi.operation.HiveJDBCTestHelper

class SparkOperationEbaySuite extends WithSparkSQLEngine with HiveJDBCTestHelper {

  override protected def jdbcUrl: String = getJdbcUrl

  override def withKyuubiConf: Map[String, String] = Map.empty

  test("DLS-251: KYUUBI DESCRIBE PATH COMMAND") {
    withJdbcStatement() { statement =>
      kyuubiConf.get(KyuubiEbayConf.KYUUBI_DESCRIBE_PATH_DATA_SOURCES).foreach { dsProvider =>
        val tempDir = Utils.createTempDir()
        val table = s"${dsProvider}_t"
        val path = tempDir.toFile.getAbsolutePath
        try {
          spark.sql(
            s"""
               |CREATE TABLE $table(id int)
               |USING $dsProvider
               |LOCATION '$path'
               |""".stripMargin)
          spark.sql(s"INSERT INTO $table values(1)")
          checkAnswer(s"KYUUBI DESCRIBE PATH $dsProvider.`$path`", Seq(Row("id", "int", null)))
          checkAnswer(s"KYUUBI DESCRIBE PATH `$path`", Seq(Row("id", "int", null)))
          checkAnswer(
            s"KYUUBI DESCRIBE PATH EXTENDED `$path`",
            Seq(
              Row("id", "int", null),
              Row("", "", ""),
              Row("# Detailed Path Information", "", ""),
              Row("Datasource Provider", dsProvider, "")))

          var rs = statement.executeQuery(s"KYUUBI DESCRIBE PATH $dsProvider.`$path`")
          assert(rs.next())
          assert(rs.getString(1) == "id")
          assert(rs.getString(2) == "int")
          assert(rs.getString(3) == null)
          assert(!rs.next())

          rs = statement.executeQuery(s"KYUUBI DESCRIBE PATH `$path`")
          assert(rs.next())
          assert(rs.getString(1) == "id")
          assert(rs.getString(2) == "int")
          assert(rs.getString(3) == null)
          assert(!rs.next())

          rs = statement.executeQuery(s"KYUUBI DESCRIBE PATH EXTENDED `$path`")
          assert(rs.next())
          assert(rs.getString(1) == "id")
          assert(rs.getString(2) == "int")
          assert(rs.getString(3) == null)
          assert(rs.next())
          assert(rs.getString(1) == "")
          assert(rs.getString(2) == "")
          assert(rs.getString(3) == "")
          assert(rs.next())
          assert(rs.getString(1) == "# Detailed Path Information")
          assert(rs.getString(2) == "")
          assert(rs.getString(3) == "")
          assert(rs.next())
          assert(rs.getString(1) == "Datasource Provider")
          assert(rs.getString(2) == dsProvider)
          assert(rs.getString(3) == "")
          assert(!rs.next())
        } finally {
          Utils.deleteDirectoryRecursively(tempDir.toFile)
          spark.sql(s"DROP TABLE IF EXISTS $table")
        }
      }
    }
  }

  protected def checkAnswer(query: String, result: Seq[Row]): Unit = {
    assert(spark.sql(query).collect() === result)
  }
}
