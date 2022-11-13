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

import java.sql.SQLException
import java.util.UUID

import org.apache.kyuubi.{Utils, WithKyuubiServer}
import org.apache.kyuubi.config.{KyuubiConf, KyuubiEbayConf}
import org.apache.kyuubi.session.KyuubiSessionImpl

class KyuubiOperationEbaySuite extends WithKyuubiServer with HiveJDBCTestHelper {
  override protected def jdbcUrl: String = getJdbcUrl

  override protected val conf: KyuubiConf = {
    KyuubiConf().set(KyuubiConf.ENGINE_SHARE_LEVEL, "connection")
      .set(KyuubiEbayConf.SESSION_CLUSTER_MODE_ENABLED, true)
      .set(KyuubiEbayConf.SESSION_CLUSTER, "invalid-cluster")
      .set(KyuubiEbayConf.OPERATION_INTERCEPT_ENABLED, true)
      .set(KyuubiEbayConf.SESSION_CLUSTER_LIST, Seq("test"))
  }

  test("open session with cluster selector") {
    withSessionConf(Map.empty)(Map.empty)(Map(
      KyuubiEbayConf.SESSION_CLUSTER.key -> "test")) {
      withJdbcStatement() { statement =>
        val rs = statement.executeQuery("set spark.sql.kyuubi.session.cluster.test")
        assert(rs.next())
        assert(rs.getString(2).equals("yes"))
      }
    }
  }

  test("open session without cluster selector") {
    withSessionConf(Map.empty)(Map.empty)(Map.empty) {
      val sqlException = intercept[SQLException] {
        withJdbcStatement() { _ =>
        }
      }
      assert(sqlException.getMessage.contains("Please specify the cluster to access"))
      assert(sqlException.getMessage.contains("should be one of [test]"))
    }
  }

  test("open session with invalid cluster selector & proxy user") {
    withSessionConf(Map("hive.server2.proxy.user" -> "proxy_user"))(Map.empty)(Map(
      KyuubiEbayConf.SESSION_CLUSTER.key -> "invalid")) {
      val sqlException = intercept[SQLException] {
        withJdbcStatement() { _ =>
        }
      }
      assert(sqlException.getMessage.contains("should be one of [test]"))
    }
  }

  test("open session with invalid cluster selector") {
    withSessionConf(Map.empty)(Map.empty)(Map(
      KyuubiEbayConf.SESSION_CLUSTER.key -> "invalid")) {
      val sqlException = intercept[SQLException] {
        withJdbcStatement() { _ =>
        }
      }
      assert(sqlException.getMessage.contains("should be one of [test]"))
    }
  }

  test("HADP-44681: The client conf should overwrite the server defined configuration") {
    withSessionConf(Map.empty)(Map.empty)(Map(
      KyuubiEbayConf.SESSION_CLUSTER.key -> "test",
      "spark.sql.kyuubi.session.cluster.test" -> "false")) {
      withJdbcStatement() { statement =>
        val rs = statement.executeQuery("set spark.sql.kyuubi.session.cluster.test")
        assert(rs.next())
        assert(rs.getString(2).equals("false"))
      }
    }
  }

  test("HADP-44681: Support cluster user level conf") {
    withSessionConf(Map.empty)(Map.empty)(Map(
      KyuubiEbayConf.SESSION_CLUSTER.key -> "test")) {
      withJdbcStatement() { statement =>
        val rs = statement.executeQuery("set spark.user.test")
        assert(rs.next())
        assert(!rs.getString(2).equals("b"))
      }
      withJdbcStatement() { statement =>
        val rs = statement.executeQuery("set ___userb___.spark.user.test")
        assert(rs.next())
        assert(!rs.getString(2).equals("b"))
      }
    }
  }

  test("HADP-47118: Do not get session cluster from session conf") {
    withSessionConf(Map.empty)(Map.empty)(Map(
      KyuubiEbayConf.SESSION_CLUSTER.key -> "test")) {
      withJdbcStatement() { _ =>
        val session =
          server.backendService.sessionManager.allSessions().head.asInstanceOf[KyuubiSessionImpl]
        assert(session.sessionCluster === Some("test"))
      }
    }
  }

  test("HADP-47172: Intercept the select 1 query in gateway") {
    withSessionConf(Map.empty)(Map.empty)(Map(KyuubiEbayConf.SESSION_CLUSTER.key -> "test")) {
      withJdbcStatement() { statement =>
        val rs = statement.executeQuery("select 1")
        assert(rs.next())
        assert(rs.getInt(1) === 1)
        assert(!rs.next())
        val getSchemaMethod = classOf[org.apache.kyuubi.jdbc.hive.KyuubiBaseResultSet]
          .getDeclaredMethod("getSchema")
        getSchemaMethod.setAccessible(true)
        val schema = getSchemaMethod.invoke(rs).asInstanceOf[
          org.apache.kyuubi.shade.org.apache.hive.service.cli.TableSchema]
        val comment = schema.getColumnDescriptorAt(0).getComment
        assert(comment === "intercepted by gateway")
      }
    }
  }

  test("support queue variable in jdbc url") {
    withSessionConf(Map.empty)(Map.empty)(Map(
      "queue" -> "yarn_queue",
      KyuubiEbayConf.SESSION_CLUSTER.key -> "test")) {
      withJdbcStatement() { statement =>
        val rs = statement.executeQuery("set spark.yarn.queue")
        assert(rs.next())
        assert(rs.getString(2) === "yarn_queue")
      }
    }

    withSessionConf(Map.empty)(Map.empty)(Map(
      "queue" -> "yarn_queue",
      "spark.yarn.queue" -> "queue_2",
      KyuubiEbayConf.SESSION_CLUSTER.key -> "test")) {
      withJdbcStatement() { statement =>
        val rs = statement.executeQuery("set spark.yarn.queue")
        assert(rs.next())
        assert(rs.getString(2) === "queue_2")
      }
    }
  }

  Seq(
    "WITH TEMP_WITH_VIEW AS (SELECT system_user(), session_user()) SELECT * FROM TEMP_WITH_VIEW",
    "SELECT system_user(), session_user()").foreach { stmt =>
    Seq(0, 10 * 1024 * 1024).foreach { minSize =>
      Seq("true", "false").foreach { sortLimitEnabled =>
        test(s"HADP-46611: support to save the result into temp table-" +
          s" $stmt/$minSize/$sortLimitEnabled") {
          withSessionConf(Map.empty)(Map.empty)(Map(
            KyuubiEbayConf.SESSION_CLUSTER.key -> "test",
            KyuubiEbayConf.OPERATION_TEMP_TABLE_DATABASE.key -> "kyuubi",
            KyuubiEbayConf.OPERATION_TEMP_TABLE_COLLECT.key -> "true",
            KyuubiConf.OPERATION_INCREMENTAL_COLLECT.key -> "true",
            KyuubiEbayConf.OPERATION_TEMP_TABLE_COLLECT_FILE_COALESCE_NUM_THRESHOLD.key -> "1",
            KyuubiEbayConf.OPERATION_TEMP_TABLE_COLLECT_SORT_LIMIT_ENABLED.key -> sortLimitEnabled,
            KyuubiEbayConf.OPERATION_TEMP_TABLE_COLLECT_MIN_FILE_SIZE.key -> minSize.toString)) {
            withJdbcStatement() { statement =>
              statement.executeQuery("create database if not exists kyuubi")
              var rs = statement.executeQuery(stmt)
              assert(rs.next())
              assert(rs.getString(1) === Utils.currentUser)
              assert(rs.getString(2) === Utils.currentUser)
              assert(!rs.next())
              // use a new statement to prevent the temp table cleanup
              val statement2 = statement.getConnection.createStatement()
              rs = statement2.executeQuery("show tables in kyuubi")
              assert(rs.next())
              val db = rs.getString(1)
              val tempTableName = rs.getString(2)
              assert(db === "kyuubi")
              assert(tempTableName.startsWith("kyuubi_temp_"))
              assert(!rs.next())
              rs = statement2.executeQuery(s"select * from $db.$tempTableName")
              assert(rs.next())
              assert(rs.getString(1) === Utils.currentUser)
              assert(rs.getString(2) === Utils.currentUser)
              assert(UUID.fromString(rs.getString(3)).isInstanceOf[UUID])
              // cleanup the temp table
              statement.close()
              rs = statement2.executeQuery("show tables in kyuubi")
              assert(!rs.next())
              statement2.executeQuery("create table ta(id int) using parquet")
              statement2.executeQuery("insert into ta(id) values(1),(2)")
              rs = statement2.executeQuery("SELECT * from ta order by id")
              assert(rs.next())
              assert(rs.getInt(1) === 1)
              assert(rs.next())
              assert(rs.getInt(1) === 2)
              assert(!rs.next())
              statement2.executeQuery("drop table ta")
              statement2.executeQuery("drop database kyuubi")
            }
          }
        }
      }
    }
  }
}
