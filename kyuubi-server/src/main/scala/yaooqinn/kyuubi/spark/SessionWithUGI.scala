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

import java.util.concurrent.TimeUnit

import scala.collection.mutable.{HashSet => MHSet}
import scala.concurrent.{Await, Promise}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.util.control.NonFatal

import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.{KyuubiSparkUtil, SparkConf, SparkContext}
import org.apache.spark.KyuubiConf._
import org.apache.spark.KyuubiSparkUtil._
import org.apache.spark.sql.SparkSession
import org.apache.spark.ui.KyuubiServerTab

import yaooqinn.kyuubi.{KyuubiSQLException, Logging}
import yaooqinn.kyuubi.author.AuthzHelper
import yaooqinn.kyuubi.ui.{KyuubiServerListener, KyuubiServerMonitor}
import yaooqinn.kyuubi.utils.{KyuubiHadoopUtil, ReflectUtils}

class SessionWithUGI(
    user: UserGroupInformation,
    conf: SparkConf,
    cache: SessionCacheManager) extends Logging {
  private var _session: AbstractSession = _
  private val userName: String = user.getShortUserName
  private var initialDatabase: Option[String] = None

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
            _session.conf.set(k, value)
          } else {
            _session.conf.set(SPARK_HADOOP_PREFIX + k, value)
          }
        case USE_DB => initialDatabase = Some("use " + value)
        case _ =>
      }
    }
  }




}

object SessionWithUGI {
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
}
