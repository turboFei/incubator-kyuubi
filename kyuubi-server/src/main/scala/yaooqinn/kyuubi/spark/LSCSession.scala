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

import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.{KyuubiConf, SparkConf, SparkContext}
import org.apache.spark.KyuubiConf.BACKEND_SESSTION_INIT_TIMEOUT
import org.apache.spark.sql.SparkSession
import scala.concurrent.{Await, Promise}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.util.control.NonFatal

import yaooqinn.kyuubi.Logging
import yaooqinn.kyuubi.utils.{KyuubiHadoopUtil, ReflectUtils}


class LSCSession(conf: SparkConf,
    user: UserGroupInformation,
    cacheManager: SessionCacheManager) extends Session(conf) with Logging {

  var sparkSession: SparkSession = null

  private var sparkException: Option[Throwable] = None
  private val userName = user.getShortUserName

  val promisedSparkContext = Promise[SparkContext]()

  private lazy val newContext: Thread = {
    val threadName = "SparkContext-Starter-" + userName
    new Thread(threadName) {
      override def run(): Unit = {
        try {
          promisedSparkContext.trySuccess {
            new SparkContext(conf)
          }
        } catch {
          case e: Exception =>
            sparkException = Some(e)
            throw e
        }
      }
    }
  }
  override def init(): Unit = {
    val totalWaitTime: Long = conf.getTimeAsSeconds(BACKEND_SESSTION_INIT_TIMEOUT.key)
    KyuubiHadoopUtil.doAs(user) {
      newContext.start()
      val sparkContext =
        Await.result(promisedSparkContext.future, Duration(totalWaitTime, TimeUnit.SECONDS))
      sparkSession = ReflectUtils.newInstance(
        classOf[SparkSession].getName,
        Seq(classOf[SparkContext]),
        Seq(sparkContext)).asInstanceOf[SparkSession]
    }
    cacheManager.set(userName, this)
  }

  override def stopSession(): Unit = {
    promisedSparkContext.future.map { sc =>
      warn(s"Error occurred during initializing SparkContext for $userName, stopping")
      try {
        sc.stop
      } catch {
        case NonFatal(e) => error(s"Error Stopping $userName's SparkContext", e)
      } finally {
        System.setProperty("SPARK_YARN_MODE", "true")
      }
    }
  }



}
