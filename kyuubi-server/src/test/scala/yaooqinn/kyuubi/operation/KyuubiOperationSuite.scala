/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package yaooqinn.kyuubi.operation

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.FunctionResource
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.execution.command.CreateFunctionCommand
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.mockito.Mockito.when

import scala.collection.JavaConverters._
import yaooqinn.kyuubi.KyuubiSQLException
import yaooqinn.kyuubi.cli.FetchOrientation.FETCH_NEXT
import yaooqinn.kyuubi.schema.ColumnBasedSet
import yaooqinn.kyuubi.session.{KyuubiSession, SessionManager}
import yaooqinn.kyuubi.spark.SparkSessionWithUGI
import yaooqinn.kyuubi.utils.ReflectUtils

class KyuubiOperationSuite extends OperationSuite {

  var spark: SparkSession = _
  var sparkWithUgi: SparkSessionWithUGI = _

  override protected def beforeAll(): Unit = {
    val sc = ReflectUtils
      .newInstance(classOf[SparkContext].getName, Seq(classOf[SparkConf]), Seq(conf))
      .asInstanceOf[SparkContext]
    spark = ReflectUtils.newInstance(
      classOf[SparkSession].getName,
      Seq(classOf[SparkContext]),
      Seq(sc)).asInstanceOf[SparkSession]
    super.beforeAll()
    sparkWithUgi = new SparkSessionWithUGI(user, conf, sessionMgr.getCacheMgr)
    ReflectUtils.setFieldValue(sparkWithUgi,
      "yaooqinn$kyuubi$spark$SparkSessionWithUGI$$_sparkSession", spark)
    session = new KyuubiSession(
      proto, userName, passwd, conf, "", false, sessionMgr, sessionMgr.getOperationMgr)
    sessionMgr.getCacheMgr.set(userName, spark)
    ReflectUtils.setFieldValue(session, "sparkSessionWithUGI", sparkWithUgi)
  }

  protected override def afterAll(): Unit = {
    spark.stop()
    super.afterAll()
  }

  test("testGetNextRowSet") {
    val op = sessionMgr.getOperationMgr.newExecuteStatementOperation(session, statement)
    val ds = spark.sql(statement)
    ReflectUtils.invokeMethod(op, "setState", Seq(classOf[OperationState]), Seq(RUNNING))
    ReflectUtils.invokeMethod(op, "setState", Seq(classOf[OperationState]), Seq(FINISHED))
    ReflectUtils.setFieldValue(op,
      "iter", ds.toLocalIterator().asScala)
    val rowSet = op.getNextRowSet(FETCH_NEXT, 10)
    assert(rowSet.isInstanceOf[ColumnBasedSet])
    val schema = op.getResultSetSchema
    assert(schema.isInstanceOf[StructType])
  }

  test("testGetSession") {
    val op = sessionMgr.getOperationMgr.newExecuteStatementOperation(session, statement)
    val s = op.getSession.asInstanceOf[KyuubiSession]
    assert(s.sparkSession === spark)
    assert(s.getUserName === userName)
  }

  test("DEFAULT_FETCH_ORIENTATION") {
    assert(KyuubiOperation.DEFAULT_FETCH_ORIENTATION === FETCH_NEXT)
  }

  test("DEFAULT_FETCH_MAX_ROWS") {
    assert(KyuubiOperation.DEFAULT_FETCH_MAX_ROWS === 100)
  }

  test("is resource downloadable") {
    intercept[IllegalArgumentException](KyuubiOperation.isResourceDownloadable(null))
    intercept[IllegalArgumentException](KyuubiOperation.isResourceDownloadable(""))
    assert(KyuubiOperation.isResourceDownloadable("hdfs://a/b/c.jar"))
    assert(!KyuubiOperation.isResourceDownloadable("file://a/b/c.jar"))
    assert(!KyuubiOperation.isResourceDownloadable("dfs://a/b/c.jar"))
  }

  test("transform plan") {
    val op = sessionMgr.getOperationMgr.newExecuteStatementOperation(session, statement)
      .asInstanceOf[KyuubiOperation]

    val parser = new SparkSqlParser(new SQLConf)
    val plan0 = parser.parsePlan("create temporary function a as 'a.b.c'")
    assert(op.transform(plan0) === plan0)

    val plan1 = parser.parsePlan(
      "create temporary function a as 'a.b.c' using file 'hdfs://a/b/c.jar'")
    val e1 = intercept[KyuubiSQLException](op.transform(plan1))
    assert(e1.getMessage.startsWith("Failed to read external resource"))

    val plan2 = parser.parsePlan(
      "create temporary function a as 'a.b.c' using jar 'hdfs://a/b/c.jar'")
    val e2 = intercept[KyuubiSQLException](op.transform(plan2))
    assert(e2.getMessage.startsWith("Failed to read external resource"))

    val resources = mock[Seq[FunctionResource]]
    when(resources.isEmpty).thenReturn(false)

    val command = plan2.asInstanceOf[CreateFunctionCommand].copy(resources = resources)
    val plan4 = op.transform(command)
    assert(plan4 === command)
    assert(plan4.asInstanceOf[CreateFunctionCommand].resources !== resources)

    val plan5 = parser.parsePlan(
      "create temporary function a as 'a.b.c' using archive 'hdfs://a/b/c.jar'")

    val e3 = intercept[KyuubiSQLException](op.transform(plan5))
    assert(e3.getMessage.startsWith("Resource Type"))
  }
}
