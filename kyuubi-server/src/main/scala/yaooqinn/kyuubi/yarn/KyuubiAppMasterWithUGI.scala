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

package yaooqinn.kyuubi.yarn

import java.nio.charset.Charset
import java.util.concurrent.TimeUnit

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.locks.InterProcessMutex
import org.apache.curator.utils.EnsurePath
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.api.records.ApplicationId
import org.apache.spark.KyuubiConf._
import org.apache.spark.KyuubiSparkUtil._
import org.apache.spark.SparkConf
import org.apache.spark.deploy.KyuubiSubmit
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Await, Promise}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.util.control.NonFatal

import yaooqinn.kyuubi.{KyuubiSQLException, Logging}
import yaooqinn.kyuubi.ha.HighAvailableService
import yaooqinn.kyuubi.utils.KyuubiHadoopUtil

class KyuubiAppMasterWithUGI(
    user: UserGroupInformation,
    conf: SparkConf) extends Logging {

  import KyuubiAppMasterWithUGI._

  private val userName = user.getUserName
  private var instance: Option[String] = None
  private var instanceParentPath: String = _
  private val promisedKyuubiAmAppId = Promise[ApplicationId]()
  private var zookeeperClient: CuratorFramework = _
  private var lock: InterProcessMutex = _
  private var kyuubiAppMasterException: Option[Throwable] = None
  private var sessionConf: Map[String, String] = _
  private val submitConf = mutable.Map[String, String]()

  private lazy val newKyuubiAppMaster: Thread = {
    val threadName = "KyuubiAppMaster-Starter-" + userName
    new Thread(threadName) {
      override def run(): Unit = {
        try {
          promisedKyuubiAmAppId.trySuccess {
            KyuubiSubmit.submitKyuubiCluster(makeAppMasterArgs)
          }
        } catch {
          case e: Exception =>
            kyuubiAppMasterException = Some(e)
            throw e
        }
      }
    }
  }

  /**
   * Get configuration before kyuubiAppMaster submit.
   */
  private def getSubmitConf(): Unit = {
    submitConf += (DRIVER_MEM -> conf.get(YARN_KYUUBIAPPMASTER_MEMORY.key, "10g"))
    submitConf += (DRIVER_EXTRA_JAVA_OPTIONS -> conf.get(YARN_KYUUBIAPPMASTER_EXTRA_JAVAOPTIONS.key,
      "-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps " +
        "-XX:+PrintTenuringDistribution"))
    submitConf += (DRIVER_MEM_OVERHEAD -> conf.get(YARN_KYUUBIAPPMASTER_MEMORY_OVERHEAD.key, "1g"))
    submitConf += (DRIVER_CORES -> conf.get(YARN_KYUUBIAPPMASTER_CORES.key, "4"))
    submitConf += ("spark.driver.maxResultSize" -> conf.get(YARN_KYUUBIAPPMASTER_MAX_RESULTSIZE.key,
      conf.get("spark.driver.maxResultSize", "1g")))

    for ((key, value) <- sessionConf) {
      key match {
        case HIVE_VAR_PREFIX(DEPRECATED_QUEUE) => conf.set(QUEUE, value)
        case HIVE_VAR_PREFIX(k) =>
          if (k.startsWith(SPARK_PREFIX)) {
            submitConf += (k -> value)
          } else {
            submitConf += (SPARK_HADOOP_PREFIX + k -> value)
          }
        case _ =>
      }
    }

    // Set the relative parameters for KyuubiAppMaster
    submitConf += (YARN_KYUUBIAPPMASTER_MODE.key -> "true")
    submitConf += (YARN_KYUUBIAPPMASTER_USERNAME.key -> userName)
    submitConf += (AUTHENTICATION_METHOD.key -> "NONE")
    submitConf += (YARN_KYUUBISERVER_SESSION_MODE.key -> "client")
    submitConf += (FRONTEND_BIND_PORT.key -> "0")
    // TODO: Fix the bug of unfit defaultFs in kyuubiAppMaster
    submitConf += ("spark.hadoop.fs.defaultFS" -> conf.get("spark.hadoop.fs.defaultFS"))
  }

  private def makeAppMasterArgs: Array[String] = {
    val args = new ArrayBuffer[String]()
    args += "--class"
    args += "yaooqinn.kyuubi.yarn.KyuubiAppMaster"
    args += "--master"
    args += "yarn-cluster"
    getSubmitConf()
    for ((key, value) <- submitConf) {
      args += "--conf"
      args += key + "=" + value
    }
    args += System.getenv("KYUUBI_JAR")
    args.toArray
  }

  private def getOrCreate(): Unit = {
    if (!isIntanceExists()) {
      create()
    }
  }

  private def create(): Unit = {
    val submitTimeOut = conf.getTimeAsSeconds(YARN_CONTAINER_TIMEOUT.key)
    lock.acquire()
    info(s"Get the lock of KyuubiAppMaster for $userName.")
    try {
      if (!isIntanceExists()) {
        KyuubiHadoopUtil.doAs(user) {
          newKyuubiAppMaster.start()
          val appId =
            Await.result(promisedKyuubiAmAppId.future, Duration(submitTimeOut, TimeUnit.SECONDS))

          if (KyuubiHadoopUtil.isYarnAppRunning(appId)) {
            val publishTimeOut = conf.getTimeAsSeconds(YARN_KYUUBIAPPMASTER_PUBLISH_WAITTIME.key)
            var totalWaitTime = math.max(publishTimeOut, 60L)
            // check per second
            val interval = 1000
            while (!isIntanceExists()) {
              wait(interval)
              totalWaitTime -= 1
              if (totalWaitTime <= 0) {
                KyuubiHadoopUtil.killYarnAppByAppId(appId)
                throw new KyuubiSQLException(s"KyuubiAppMaster does not publish server in " +
                  s"$totalWaitTime seconds and kill it by applicationID $appId")
              }
              if (!KyuubiHadoopUtil.isYarnAppRunning(appId)) {
                throw new KyuubiSQLException("KyuubiAppMaster is not running.")
              }
              info(s"KyuubiAppMaster is publishing server, $totalWaitTime times countdown.")
            }
          } else {
            throw new Exception(s"Error when submiting the KyuubiAppMaster for $userName.")
          }
        }
      }
    } catch {
      case e: Exception =>
        KyuubiHadoopUtil.doAs(user) {
          stopKyuubiAppMaster()
        }
        val ke = new KyuubiSQLException(
          s"Get KyuubiAppMaster for [$userName] failed", "08S01", 1001, findCause(e))
        kyuubiAppMasterException.foreach(ke.addSuppressed)
        throw ke
    } finally {
      lock.release()
      info(s"Release the lock of KyuubiAppMaster for $userName.")
      if (newKyuubiAppMaster.isAlive) {
        newKyuubiAppMaster.join()
      }
    }
  }

  /**
   * Kill user's KyuubiAppMaster if not initializing it.
   */
  private[this] def stopKyuubiAppMaster(): Unit = {
    promisedKyuubiAmAppId.future.map { appId =>
      warn(s"Error occured during submit kyuubiAppMaster for $userName, stopping")
      try {
        if (appId != null) {
          KyuubiHadoopUtil.killYarnAppByAppId(appId)
        }
      } catch {
        case NonFatal(e) => error(s"Error Stopping $userName's KyuubiAppMaster", e)
      } finally {
        System.setProperty("SPARK_YARN_MODE", "true")
      }
    }
  }

  private[this] def getLock(conf: SparkConf): Unit = {
    val lockPath = getLockPath(userName, conf)
    val ensurePath = new EnsurePath(lockPath)
    ensurePath.ensure(zookeeperClient.getZookeeperClient)
    lock = new InterProcessMutex(zookeeperClient, lockPath)
  }

  private def isIntanceExists(): Boolean = {
    try {
      zookeeperClient.getChildren.forPath(instanceParentPath) != null &&
      !zookeeperClient.getChildren.forPath(instanceParentPath).isEmpty
    } catch {
      case e: Exception =>
        warn(s"Failed to getChildren for $instanceParentPath.")
        false
    }
  }

  private def getInstance(): Option[String] = {
    if (isIntanceExists()) {
      val znode = zookeeperClient.getChildren.forPath(instanceParentPath).get(0)
      val data = znode.getBytes()
      Some(new String(data, Charset.forName("UTF-8"))
        .split(";")
        .apply(0)
        .split("=")
        .apply(1))
    } else {
      None
    }
  }

  @throws[KyuubiSQLException]
  def init(sessionConf: Map[String, String]): Unit = {
    this.sessionConf = sessionConf
    instanceParentPath = getInstanceNameSpace(userName, conf)
    zookeeperClient = HighAvailableService.newZookeeperClient(conf)
    getLock(conf)
    getOrCreate()
    instance = getInstance()
  }

  def kyuubiAmInstance: Option[String] = instance
}

object KyuubiAppMasterWithUGI {
  private val lock = "LOCK"
  private val uriPath = HighAvailableService.URIPATH

  def getLockPath(user: String, conf: SparkConf): String = {
    "/" + conf.get(YARN_KYUUBIAPPMASTER_NAMESPACE.key) + "/" + user + "/" + lock
  }

  def getInstanceNameSpace(user: String, conf: SparkConf): String = {
    // This should be consistent with the rootNameSpace of HighAvailableService
    "/" + conf.get(YARN_KYUUBIAPPMASTER_NAMESPACE.key) + "/" + user + "/" + uriPath
  }
}
