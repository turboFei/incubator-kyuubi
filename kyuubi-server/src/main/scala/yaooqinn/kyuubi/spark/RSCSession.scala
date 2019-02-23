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

package yaooqinn.kyuubi.spark

import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import org.apache.hadoop.security.UserGroupInformation
import org.apache.livy.{ExecuteSqlRequest, LivyConf}
import org.apache.livy.rsc.driver.Statement
import org.apache.livy.server.interactive.{CreateInteractiveRequest, KyuubiInteractiveSession}
import org.apache.livy.server.recovery.SessionStore
import org.apache.spark.KyuubiConf.BACKEND_SESSTION_INIT_TIMEOUT
import org.apache.spark.SparkConf
import org.apache.spark.scheduler.SparkListener
import scala.concurrent.{Await, Promise}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.util.control.NonFatal

import yaooqinn.kyuubi.Logging
import yaooqinn.kyuubi.utils.KyuubiHadoopUtil


case class RSCSession(userName: String, kyuubiInteractiveSession: KyuubiInteractiveSession,
    sessionUUID: UUID) extends AbstractSession with Logging {

  def executeStament(code: String): Statement = {
    kyuubiInteractiveSession.executeStatement(
      new ExecuteSqlRequest(sessionUUID, code))
  }

  def addSparkListener(listener: SparkListener): Unit = {
    kyuubiInteractiveSession.addSparkListener(listener)
  }

  def configSessionConf(sessionConf: Map[String, String]): Unit = {
    kyuubiInteractiveSession.configSessionConf(sessionUUID, sessionConf)
  }

  def removeSparkSession(): Unit = {
    kyuubiInteractiveSession.removeSparkSession(sessionUUID)
  }

  override def stop(): Unit = {
    try {
      removeSparkSession()
    } catch {
      case NonFatal(e) => error(s"Error Stopping $userName's SparkContext", e)
    } finally {
      System.setProperty("SPARK_YARN_MODE", "true")
    }
  }
}

object RSCSession {

  def create(username: String, kyuubiInteractiveSession: KyuubiInteractiveSession,
             sessionUUID: UUID): RSCSession = {
    new RSCSession(username, kyuubiInteractiveSession, sessionUUID)

  }

  private val sessionId = new AtomicInteger()

  def getAndIncrementSessionId: Int = sessionId.getAndIncrement()

  val sessionStore = new SessionStore(new LivyConf())



}
