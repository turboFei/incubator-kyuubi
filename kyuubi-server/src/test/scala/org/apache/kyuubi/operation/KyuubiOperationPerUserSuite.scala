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

import org.scalatest.time.SpanSugar._

import org.apache.kyuubi.{Utils, WithKyuubiServer}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.jdbc.hive.KyuubiConnection

class KyuubiOperationPerUserSuite extends WithKyuubiServer with SparkQueryTests {

  override protected def jdbcUrl: String = getJdbcUrl

  override protected val conf: KyuubiConf = {
    KyuubiConf().set(KyuubiConf.ENGINE_SHARE_LEVEL, "user")
  }

  test("kyuubi defined function - system_user/session_user") {
    withJdbcStatement() { statement =>
      val rs = statement.executeQuery("SELECT system_user(), session_user()")
      assert(rs.next())
      assert(rs.getString(1) === Utils.currentUser)
      assert(rs.getString(2) === Utils.currentUser)
    }
  }

  test("ensure two connections in user mode share the same engine") {
    var r1: String = null
    var r2: String = null
    new Thread {
      override def run(): Unit = withJdbcStatement() { statement =>
        val res = statement.executeQuery("set spark.app.name")
        assert(res.next())
        r1 = res.getString("value")
      }
    }.start()

    new Thread {
      override def run(): Unit = withJdbcStatement() { statement =>
        val res = statement.executeQuery("set spark.app.name")
        assert(res.next())
        r2 = res.getString("value")
      }
    }.start()

    eventually(timeout(120.seconds), interval(100.milliseconds)) {
      assert(r1 != null && r2 != null)
    }

    assert(r1 === r2)
  }

  test("ensure open session asynchronously for USER mode still share the same engine") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("SELECT engine_id()")
      assert(resultSet.next())
      val engineId = resultSet.getString(1)

      withSessionConf(Map(
        KyuubiConf.SESSION_ENGINE_LAUNCH_ASYNC.key -> "true"))(Map.empty)(Map.empty) {
        withJdbcStatement() { stmt =>
          val rs = stmt.executeQuery("SELECT engine_id()")
          assert(rs.next())
          assert(rs.getString(1) == engineId)
        }
      }
    }
  }

  test("ensure two connections share the same engine when specifying subdomain.") {
    withSessionConf()(
      Map(
        KyuubiConf.ENGINE_SHARE_LEVEL_SUBDOMAIN.key -> "abc"))(Map.empty) {

      var r1: String = null
      var r2: String = null
      new Thread {
        override def run(): Unit = withJdbcStatement() { statement =>
          val res = statement.executeQuery("set spark.app.name")
          assert(res.next())
          r1 = res.getString("value")
        }
      }.start()

      new Thread {
        override def run(): Unit = withJdbcStatement() { statement =>
          val res = statement.executeQuery("set spark.app.name")
          assert(res.next())
          r2 = res.getString("value")
        }
      }.start()

      eventually(timeout(120.seconds), interval(100.milliseconds)) {
        assert(r1 != null && r2 != null)
      }

      assert(r1 === r2)
    }
  }

  test("ensure engine discovery works when mixed use subdomain") {
    var r1: String = null
    var r2: String = null
    withSessionConf()(Map.empty)(Map.empty) {
      withJdbcStatement() { statement =>
        val res = statement.executeQuery("set spark.app.name")
        assert(res.next())
        r1 = res.getString("value")
      }
    }
    assert(r1 contains "default")

    withSessionConf()(Map(KyuubiConf.ENGINE_SHARE_LEVEL_SUBDOMAIN.key -> "abc"))(Map.empty) {
      withJdbcStatement() { statement =>
        val res = statement.executeQuery("set spark.app.name")
        assert(res.next())
        r2 = res.getString("value")
      }
    }
    assert(r2 contains "abc")

    assert(r1 !== r2)
  }

  test("HADP-44628: Enable the timeout for KyuubiConnection::isValid") {
    withJdbcStatement() { statement =>
      val conn = statement.getConnection
      assert(conn.isInstanceOf[KyuubiConnection])
      assert(!conn.isValid(1))
      assert(conn.isValid(3000))
    }
  }
}
