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

package org.apache.spark.sql.kyuubi.operation

import org.apache.spark.sql.internal.SQLConf

import org.apache.kyuubi.engine.spark.WithSparkSQLEngine

class ExecuteStatementHelperSuite extends WithSparkSQLEngine {
  override def withKyuubiConf: Map[String, String] = Map.empty

  test("is data query language") {
    var query = "select * from table"
    assert(ExecuteStatementHelper.isDQL(query))
    query = "(select * from table)"
    assert(ExecuteStatementHelper.isDQL(query))
    query = "(WITH TEMP_WITH_VIEW AS (SELECT * from tbl_d) SELECT * FROM TEMP_WITH_VIEW)"
    assert(ExecuteStatementHelper.isDQL(query))
    query = "(WITH TEMP_WITH_VIEW AS (SELECT * from tbl_d)" +
      " INSERT INTO tbl SELECT * FROM TEMP_WITH_VIEW)"
    assert(!ExecuteStatementHelper.isDQL(query))
    query = "cache table tbl"
    assert(!ExecuteStatementHelper.isDQL(query))
    query = "insert into tbl select * from ta"
    assert(!ExecuteStatementHelper.isDQL(query))
  }

  test("is sortable") {
    Seq(true, false).foreach { aqe =>
      spark.sessionState.conf.setConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED, aqe)
      Seq(10, 1000).foreach { topKThreshold =>
        spark.sessionState.conf.setConf(SQLConf.TOP_K_SORT_FALLBACK_THRESHOLD, topKThreshold)
        spark.sql("create or replace temporary view tv AS select * from values(1),(2),(3) as t(id)")
        val topKStatement = s"select * from tv order by id limit ${topKThreshold - 1}"
        var sparkPlan = spark.sql(topKStatement).queryExecution.sparkPlan
        assert(ExecuteStatementHelper.isTopKSort(sparkPlan))
        assert(ExecuteStatementHelper.sortable(sparkPlan))
        assert(ExecuteStatementHelper.existingLimit(sparkPlan) === Option(topKThreshold - 1))
        val collectLimitStatement = s"select * from tv order by id limit $topKThreshold"
        sparkPlan = spark.sql(collectLimitStatement).queryExecution.sparkPlan
        assert(!ExecuteStatementHelper.isTopKSort(sparkPlan))
        assert(ExecuteStatementHelper.sortable(sparkPlan))
        assert(ExecuteStatementHelper.existingLimit(sparkPlan) === Option(topKThreshold))
      }
    }
  }
}
