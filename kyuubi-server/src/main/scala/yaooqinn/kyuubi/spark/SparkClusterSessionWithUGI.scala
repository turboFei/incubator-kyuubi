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

import org.apache.hadoop.security.UserGroupInformation
import org.apache.livy.{ExecuteRequest, ExecuteSqlRequest, LivyConf}
import org.apache.livy.server.interactive.{CreateInteractiveRequest, KyuubiInteractiveSession}
import org.apache.spark.{KyuubiSparkUtil, SparkConf, SparkContext}
import org.apache.spark.KyuubiConf._
import org.apache.spark.KyuubiSparkUtil._
import org.apache.spark.sql.SparkSession
import org.apache.spark.ui.KyuubiServerTab
import scala.collection.mutable.{HashSet => MHSet}
import scala.concurrent.{Await, Promise}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.util.control.NonFatal

import yaooqinn.kyuubi.{KyuubiSQLException, Logging}
import yaooqinn.kyuubi.author.AuthzHelper
import yaooqinn.kyuubi.ui.{KyuubiServerListener, KyuubiServerMonitor}
import yaooqinn.kyuubi.utils.{KyuubiHadoopUtil, ReflectUtils}

class SparkClusterSessionWithUGI(
    user: UserGroupInformation,
    conf: SparkConf,
    cache: SessionCacheManager) extends Logging {
  private val userName: String = user.getShortUserName
  private var initialDatabase: Option[String] = None
  var _rscSession: RSCSession = null
  var rscSessionConf: Map[String, String] = Map.empty


  var sessionUUID: UUID = SparkClusterSessionWithUGI.getUUID()
  private val promisedInteractiveSession = Promise[KyuubiInteractiveSession]
  var rscException: Option[Throwable] = None

  private lazy val newInteractiveSession: Thread = {
    val threadName = "InteractiveSession-Starter-" + userName
    val appName = Seq(
      "kyuubiCluster", userName, conf.get(FRONTEND_BIND_HOST),
      conf.get(FRONTEND_BIND_PORT)).mkString("|")
    new Thread(threadName) {
      override def run(): Unit = {
        val id = RSCSession.getAndIncrementSessionId
        val owner = userName
        val proxyUser = Some(userName)
        val licyConf = new LivyConf()
        val request = new CreateInteractiveRequest
        val sessionStore = RSCSession.sessionStore

        try {
          promisedInteractiveSession.trySuccess {
            KyuubiInteractiveSession.create(id,
              Some(appName),
              owner,
              proxyUser,
              licyConf,
              request,
              sessionStore)
          }
        } catch {
          case e: Exception =>
            rscException = Some(e)
            throw e
        }
      }
    }
  }

  def stopInteractiveSession(): Unit = {
    promisedInteractiveSession.future.map { session =>
      warn(s"Error occurred during initializing SparkContext for $userName, stopping")
      try {
        session.stopSession()
      } catch {
        case NonFatal(e) => error(s"Error Stopping $userName's SparkContext", e)
      } finally {
        System.setProperty("SPARK_YARN_MODE", "true")
      }

    }
  }

  /**
   * Setting configuration from connection strings before SparkContext init.
   *
   * @param sessionConf configurations for user connection string
   */
  private def configureSparkConf(sessionConf: Map[String, String]): Unit = {
    for ((key, value) <- sessionConf) {
      key match {
        case HIVE_VAR_PREFIX(DEPRECATED_QUEUE) => conf.set(QUEUE, value)
        case HIVE_VAR_PREFIX(k) =>
          if (k.startsWith(SPARK_PREFIX)) {
            conf.set(k, value)
          } else {
            conf.set(SPARK_HADOOP_PREFIX + k, value)
          }
        case USE_DB => initialDatabase = Some("use " + value)
        case _ =>
      }
    }

    // proxy user does not have rights to get token as real user
    conf.remove(KyuubiSparkUtil.KEYTAB)
    conf.remove(KyuubiSparkUtil.PRINCIPAL)
  }

  /**
   * Setting configuration from connection strings for existing SparkSession
   *
   * @param sessionConf configurations for user connection string
   */
  private def configureSparkSession(sessionConf: Map[String, String]): Unit = {
    for ((key, value) <- sessionConf) {
      key match {
        case HIVE_VAR_PREFIX(k) =>
          if (k.startsWith(SPARK_PREFIX)) {
            rscSessionConf += (k -> value)
          } else {
            rscSessionConf += (SPARK_HADOOP_PREFIX + k -> value)
          }
        case USE_DB => initialDatabase = Some("use " + value)
        case _ =>
      }
    }
    _rscSession.configSessionConf(rscSessionConf)

  }

  private def getOrCreate(sessionConf: Map[String, String]): Unit = synchronized {
    val totalRounds = math.max(conf.get(BACKEND_SESSION_WAIT_OTHER_TIMES).toInt, 15)
    var checkRound = totalRounds
    val interval = conf.getTimeAsMs(BACKEND_SESSION_WAIT_OTHER_INTERVAL)
    // if user's sc is being constructed by another
    while (SparkSessionWithUGI.isPartiallyConstructed(userName)) {
      wait(interval)
      checkRound -= 1
      if (checkRound <= 0) {
        throw new KyuubiSQLException(s"A partially constructed SparkContext for [$userName] " +
          s"has last more than ${totalRounds * interval / 1000} seconds")
      }
      info(s"A partially constructed SparkContext for [$userName], $checkRound times countdown.")
    }

    cache.getAndIncrease(userName) match {
      case Some(ss) if ss.isInstanceOf[RSCSession] =>
        val rscSession = ss.asInstanceOf[RSCSession]
        _rscSession = RSCSession.create(userName, rscSession.kyuubiInteractiveSession, sessionUUID)
        configureSparkSession(sessionConf)
      case _ =>
        SparkSessionWithUGI.setPartiallyConstructed(userName)
        notifyAll()
        create(sessionConf)
    }
  }

  private def create(sessionConf: Map[String, String]): Unit = {
    info(s"--------- Create new SparkSession for $userName ----------")
    // kyuubi|user name|canonical host name| port
    val appName = Seq(
      "kyuubiCluster", userName, conf.get(FRONTEND_BIND_HOST),
      conf.get(FRONTEND_BIND_PORT)).mkString("|")
    conf.setAppName(appName)
    configureSparkConf(sessionConf)
    val totalWaitTime: Long = conf.getTimeAsSeconds(BACKEND_SESSTION_INIT_TIMEOUT.key)
    try {
      KyuubiHadoopUtil.doAs(user) {
        newInteractiveSession.start()
        val kyuubiInteractiveSession =
          Await.result(promisedInteractiveSession.future, Duration(totalWaitTime, TimeUnit.SECONDS))
        kyuubiInteractiveSession.start()
        _rscSession = RSCSession.create(userName, kyuubiInteractiveSession, sessionUUID)
      }
      cache.set(userName, _rscSession)
    } catch {
      case e: Exception =>
        stopInteractiveSession()
        val ke = new KyuubiSQLException(
          s"Get SparkSession for [$userName] failed", "08S01", 1001, findCause(e))
        throw ke
    } finally {
      SparkClusterSessionWithUGI.setFullyConstructed(userName)
      newInteractiveSession.join()
    }

    // TODO: add kyuubi ui
//    KyuubiServerMonitor.setListener(userName, new KyuubiServerListener(conf))
//    KyuubiServerMonitor.getListener(userName)
//      .foreach(_rscSession.addSparkListener)
//    val uiTab = new KyuubiServerTab(userName, _sparkSession.sparkContext)
//    KyuubiServerMonitor.addUITab(userName, uiTab)
  }

  @throws[KyuubiSQLException]
  def init(sessionConf: Map[String, String]): Unit = {
    getOrCreate(sessionConf)

    try {
      initialDatabase.foreach { db =>
        KyuubiHadoopUtil.doAs(user) {
          _rscSession.executeStament(db)
        }
      }
    } catch {
      case e: Exception =>
        cache.decrease(userName)
        throw findCause(e)
    }

    // TODO: add rule for rscSession
    // KYUUBI-99: Add authorizer support after use initial db
//    AuthzHelper.get.foreach { auth =>
//      _sparkSession.experimental.extraOptimizations ++= auth.rule
//    }
  }

  def rscSession: RSCSession = _rscSession

}

object SparkClusterSessionWithUGI {
  private val userSparkContextBeingConstruct = new MHSet[String]()

  def setPartiallyConstructed(user: String): Unit = {
    userSparkContextBeingConstruct.add(user)
  }

  def isPartiallyConstructed(user: String): Boolean = {
    userSparkContextBeingConstruct.contains(user)
  }

  def setFullyConstructed(user: String): Unit = {
    userSparkContextBeingConstruct.remove(user)
  }

  def getUUID(): UUID = {
    UUID.randomUUID()
  }
}
