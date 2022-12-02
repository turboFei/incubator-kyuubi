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

import java.sql.Statement

import org.apache.kyuubi.WithKyuubiServer
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.jdbc.hive.KyuubiSQLException

class PlanOnlyOperationSuite extends WithKyuubiServer with HiveJDBCTestHelper {

  override protected val conf: KyuubiConf = {
    KyuubiConf()
      .set(KyuubiConf.ENGINE_SHARE_LEVEL, "user")
      .set(KyuubiConf.OPERATION_PLAN_ONLY_MODE, OptimizeMode.name)
      .set(KyuubiConf.ENGINE_SHARE_LEVEL_SUBDOMAIN.key, "plan-only")
  }

  override protected def jdbcUrl: String = getJdbcUrl

  test("KYUUBI #1059: Plan only operation with system defaults") {
    withJdbcStatement() { statement =>
      val operationPlan = getOperationPlanWithStatement(statement)
      assert(operationPlan.startsWith("Project") && !operationPlan.contains("Filter"))
    }
  }

  test("KYUUBI #1059: Plan only operation with session conf") {
    withSessionConf()(Map(KyuubiConf.OPERATION_PLAN_ONLY_MODE.key -> AnalyzeMode.name))(Map.empty) {
      withJdbcStatement() { statement =>
        val operationPlan = getOperationPlanWithStatement(statement)
        assert(operationPlan.startsWith("Project") && operationPlan.contains("Filter"))
      }
    }
  }

  test("KYUUBI #1059: Plan only operation with set command") {
    withSessionConf()(Map(KyuubiConf.OPERATION_PLAN_ONLY_MODE.key -> AnalyzeMode.name))(Map.empty) {
      withJdbcStatement() { statement =>
        statement.execute(s"set ${KyuubiConf.OPERATION_PLAN_ONLY_MODE.key}=${ParseMode.name}")
        val operationPlan = getOperationPlanWithStatement(statement)
        assert(operationPlan.startsWith("'Project"))
      }
    }
  }

  test("KYUUBI #1919: Plan only operation with PHYSICAL mode") {
    withSessionConf()(Map(KyuubiConf.OPERATION_PLAN_ONLY_MODE.key -> PhysicalMode.name))(
      Map.empty) {
      withJdbcStatement() { statement =>
        val operationPlan = getOperationPlanWithStatement(statement)
        assert(operationPlan.startsWith("Project") && operationPlan.contains("Scan OneRowRelation"))
      }
    }
  }

  test("KYUUBI #1919: Plan only operation with EXECUTION mode") {
    withSessionConf()(Map(KyuubiConf.OPERATION_PLAN_ONLY_MODE.key -> ExecutionMode.name))(
      Map.empty) {
      withJdbcStatement() { statement =>
        val operationPlan = getOperationPlanWithStatement(statement)
        assert(operationPlan.startsWith("*(1) Project") &&
          operationPlan.contains("*(1) Scan OneRowRelation"))
      }
    }
  }

  test("KYUUBI #1920: Plan only operations with UseStatement or SetNamespaceCommand") {
    withSessionConf()(Map(KyuubiConf.OPERATION_PLAN_ONLY_MODE.key -> NoneMode.name))(Map.empty) {
      withDatabases("test_database") { statement =>
        statement.execute("create database test_database")
        statement.execute(s"set ${KyuubiConf.OPERATION_PLAN_ONLY_MODE.key}=${OptimizeMode.name}")
        val result = statement.executeQuery("use test_database")
        assert(!result.next(), "In contrast to PlanOnly mode, it will returns an empty result")
      }
    }
  }

  test("KYUUBI #1920: Plan only operations with CreateViewStatement or CreateViewCommand") {
    withSessionConf()(
      Map(KyuubiConf.OPERATION_PLAN_ONLY_EXCLUDES.key -> "CreateViewStatement,CreateViewCommand"))(
      Map.empty) {
      withJdbcStatement("temp_view") { statement =>
        val result = statement.executeQuery("create temp view temp_view as select 1")
        assert(!result.next(), "In contrast to PlanOnly mode, it will returns an empty result")
      }
    }
  }

  test("kyuubi #2565: Variable substitution should work in plan only mode") {
    withSessionConf()(Map(KyuubiConf.OPERATION_PLAN_ONLY_MODE.key -> ParseMode.name))(Map.empty) {
      withJdbcStatement() { statement =>
        statement.executeQuery("set x = y")
        val resultSet = statement.executeQuery("select '${x}'")
        assert(resultSet.next())
        resultSet.getString(1).contains("'Project [unresolvedalias(y, None)]")
      }
    }
  }

  test("KYUUBI #3128: Support CostMode for PlanOnlyStatement") {
    withSessionConf()(Map(KyuubiConf.OPERATION_PLAN_ONLY_MODE.key -> OptimizeWithStatsMode.name))(
      Map.empty) {
      withJdbcStatement() { statement =>
        val resultSet = statement.executeQuery(
          "SELECT * FROM VALUES(1),(2),(3) AS t(c1) DISTRIBUTE BY c1")
        assert(resultSet.next())
        val operationPlan = resultSet.getString(1)
        assert(operationPlan.startsWith("RepartitionByExpression")
          && operationPlan.contains("Statistics"))
      }
    }
  }

  test("KYUUBI #3376 : Spark physical Plan outputs in plain style") {
    withSessionConf()(Map(
      KyuubiConf.OPERATION_PLAN_ONLY_MODE.key -> PhysicalMode.name,
      KyuubiConf.OPERATION_PLAN_ONLY_OUT_STYLE.key -> PlainStyle.name))() {
      withJdbcStatement() { statement =>
        val resultSet = statement.executeQuery(
          "SELECT * FROM VALUES(1),(2),(3) AS t(c1) DISTRIBUTE BY c1")
        assert(resultSet.next())
        val operationPlan = resultSet.getString(1)
        assert(operationPlan.startsWith("Exchange hashpartitioning"))
      }
    }
  }

  test("KYUUBI #3376 : Spark physical Plan outputs in json style") {
    withSessionConf()(Map(
      KyuubiConf.OPERATION_PLAN_ONLY_MODE.key -> PhysicalMode.name,
      KyuubiConf.OPERATION_PLAN_ONLY_OUT_STYLE.key -> JsonStyle.name))() {
      withJdbcStatement() { statement =>
        val resultSet = statement.executeQuery(
          "SELECT * FROM VALUES(1),(2),(3) AS t(c1) DISTRIBUTE BY c1")
        assert(resultSet.next())
        val operationPlan = resultSet.getString(1)
        assert(operationPlan.contains(
          "\"class\":\"org.apache.spark.sql.execution.exchange.ShuffleExchangeExec\""))
      }
    }
  }

  test("KYUUBI #3376 : Spark optimized Plan outputs in json style") {
    withSessionConf()(Map(
      KyuubiConf.OPERATION_PLAN_ONLY_MODE.key -> OptimizeMode.name,
      KyuubiConf.OPERATION_PLAN_ONLY_OUT_STYLE.key -> JsonStyle.name))() {
      withJdbcStatement() { statement =>
        val resultSet = statement.executeQuery(
          "SELECT * FROM VALUES(1),(2),(3) AS t(c1) DISTRIBUTE BY c1")
        assert(resultSet.next())
        val operationPlan = resultSet.getString(1)
        assert(operationPlan.contains(
          "\"class\":\"org.apache.spark.sql.catalyst.plans.logical.LocalRelation\""))
      }
    }
  }

  test("KYUUBI #3376 : Spark parse Plan outputs in json style") {
    withSessionConf()(Map(
      KyuubiConf.OPERATION_PLAN_ONLY_MODE.key -> ParseMode.name,
      KyuubiConf.OPERATION_PLAN_ONLY_OUT_STYLE.key -> JsonStyle.name))() {
      withJdbcStatement() { statement =>
        val resultSet = statement.executeQuery(
          "SELECT * FROM VALUES(1),(2),(3) AS t(c1) DISTRIBUTE BY c1")
        assert(resultSet.next())
        val operationPlan = resultSet.getString(1)
        assert(operationPlan.contains(
          "\"class\":\"org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute\""))
      }
    }
  }

  test("kyuubi #3214: Plan only mode with an incorrect value") {
    withSessionConf()(Map(KyuubiConf.OPERATION_PLAN_ONLY_MODE.key -> "parse"))(Map.empty) {
      withJdbcStatement() { statement =>
        statement.executeQuery(s"set ${KyuubiConf.OPERATION_PLAN_ONLY_MODE.key}=parser")
        val e = intercept[KyuubiSQLException](statement.executeQuery("select 1"))
        assert(e.getMessage.contains("Unknown planOnly mode: parser"))
        statement.executeQuery(s"set ${KyuubiConf.OPERATION_PLAN_ONLY_MODE.key}=parse")
        val result = statement.executeQuery("select 1")
        assert(result.next())
        assert(result.getString(1).contains("Project [unresolvedalias(1, None)]"))
      }
    }
  }

  private def getOperationPlanWithStatement(statement: Statement): String = {
    val resultSet = statement.executeQuery("select 1 where true")
    assert(resultSet.next())
    resultSet.getString(1)
  }
}
