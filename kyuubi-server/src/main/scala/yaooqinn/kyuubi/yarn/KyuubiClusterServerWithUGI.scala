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

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.api.ACLProvider
import org.apache.curator.framework.recipes.atomic.DistributedAtomicInteger
import org.apache.curator.framework.recipes.locks.InterProcessMutex
import org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode
import org.apache.curator.retry.{ExponentialBackoffRetry, RetryNTimes}
import org.apache.curator.utils.EnsurePath
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationIdPBImpl
import org.apache.hadoop.yarn.api.records.{ApplicationId, YarnApplicationState}
import org.apache.spark.{KyuubiSparkUtil, SparkConf, SparkContext}
import org.apache.spark.KyuubiConf._
import org.apache.spark.KyuubiSparkUtil._
import org.apache.spark.deploy.KyuubiSubmit
import org.apache.zookeeper.data.Stat
import org.apache.zookeeper.{CreateMode, ZooKeeper}

import scala.collection.mutable.{ListBuffer, HashSet => MHSet}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Promise
import scala.util.control.NonFatal
import yaooqinn.kyuubi.{KyuubiSQLException, Logging}
import yaooqinn.kyuubi.ha.HighAvailableService
import yaooqinn.kyuubi.utils.KyuubiHadoopUtil
import yaooqinn.kyuubi.yarn.KyuubiYarnClient

class KyuubiClusterServerWithUGI(
    user: UserGroupInformation,
    conf: SparkConf,
    cache: KyuubiClusterServerCacheManager) extends Logging {
  private val userName: String = user.getShortUserName
  private var kyuubiException: Option[Throwable] = None
  private val zkClient = KyuubiClusterServerWithUGI.getZookeeperClient(conf)

  private val rootPath = "/" + conf.get(HA_AM_ZOOKEEPER_NAMESPACE.key) + "/" + userName + "/"
  private val lockPath = rootPath + KyuubiClusterServerWithUGI.lock
  private val disCountPath = rootPath + KyuubiClusterServerWithUGI.distriCount
  private val instanceURIPth = rootPath + KyuubiClusterServerWithUGI.instanceURI
  private val lastAccessTimePath = rootPath + KyuubiClusterServerWithUGI.lastAccessTime
  private val appIdPath = rootPath + KyuubiClusterServerWithUGI.appId


  private lazy val newKyuubiYarnClient: Thread = {
    val threadName = "KyuubiYarnClient-Starter-" + userName
    new Thread(threadName) {
      override def run(): Unit = {
        try {
          KyuubiYarnClient.startKyuubiAppMaster()
        } catch {
          case e: Exception =>
            kyuubiException = Some(e)
            throw e
        }
      }
    }
  }


  private def isInstanceExists(): Boolean = {
    zkClient.checkExists().forPath(instanceURIPth) != null
  }

  private def ensureLock(): Unit = {
    val ensurePath = new EnsurePath(lockPath)
    ensurePath.ensure(zkClient.getZookeeperClient)
  }

  private def getLock(): InterProcessMutex = {
    new InterProcessMutex(zkClient, lockPath)
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

  }

  private def getOrCreate(sessionConf: Map[String, String]): Unit = synchronized {
    if (!isInstanceExists()) {
      ensureLock()
      val lock = getLock()
      lock.acquire()
      if (!isInstanceExists()) {
        val args = new ListBuffer[String]
        // build args
        args += "--master"
        args += "yarn"
        args += "--deploy-mode"
        args += "cluster"
        args += "--conf"
        args += YARN_KYUUBI_APP_MASTER_USERNAME.key + "=" + userName
        args += "--conf"
        args += YARN_KYUUBI_APP_MASTER_MODE.key + "=" +"true"

        var appId: ApplicationId = null
        try {
          val appId = KyuubiSubmit.startAppMaster(args.toArray)
          if (appId == null) {
            throw new Exception("The AppId for KyuubiAppMaster is null ")
          }
        } catch {
          case e: Throwable =>
            throw new KyuubiSQLException("Something wrong when submiting KyuubiAppMaster", e)
        }

        if (KyuubiHadoopUtil.getYarnAppState(appId) == YarnApplicationState.RUNNING) {
          val timeOut = conf.getTimeAsSeconds(YARN_KYUUBI_APP_MASTER_PUBLISH_WAIT_TIME.key, "60s")
          var totalWaitTime = math.max(timeOut, 15)
          // check per seconds
          val interval = 1000
          while (!isInstanceExists()) {
            wait(interval)
            totalWaitTime -= 1
            if (totalWaitTime <= 0) {
              KyuubiHadoopUtil.killYarnAppById(appId)
              throw new KyuubiSQLException(s"KyuubiAppMaster does not publish server in" +
                s"$totalWaitTime seconds and kill it by applicationID $appId")
            }
            if (KyuubiHadoopUtil.getYarnAppState(appId) != YarnApplicationState.RUNNING) {
              throw new KyuubiSQLException("KyuubiAppMaster is not running.")
            }
            info(s"KyuubiAppMaster is publish server, $totalWaitTime times countdown.")
          }
        }

        if (isInstanceExists()) {
          // Create relative zookeeperNode for count lastAccessTime and appId
          val disCount = new DistributedAtomicInteger(zkClient, disCountPath,
            new RetryNTimes(3, 1000))

          zkClient.create()
            .creatingParentsIfNeeded()
            .withMode(CreateMode.PERSISTENT)
            .forPath(lastAccessTimePath)

          zkClient.create()
            .creatingParentsIfNeeded()
            .withMode(CreateMode.PERSISTENT)
            .forPath(appIdPath, appId.toString.getBytes())
        }
      }

      lock.release()
    }

  }

  private def createApplicationId(appIdString: String): ApplicationIdPBImpl = {

    val appArr = appIdString.split("_")
    val clusterTimeStamp = appArr(1).toLong
    val id = appArr(2).toInt
    new ApplicationIdPBImpl().

  }

  private def create(sessionConf: Map[String, String]): Unit = {
  }

  @throws[KyuubiSQLException]
  def init(sessionConf: Map[String, String]): Unit = {

    getOrCreate(sessionConf)

  }


}

object KyuubiClusterServerWithUGI {

  private val lock = "lock"
  private val distriCount = "distributeCount"
  private val appId = "applicationId"
  private val lastAccessTime = "lastAccessTime"
  private val instanceURI = "instanceURI"

  private[this] var zkClient: CuratorFramework = null

  private val userSparkContextBeingConstruct = new MHSet[String]()

  def getZookeeperClient(conf: SparkConf): CuratorFramework = {
    if (zkClient == null) {
      synchronized {
        if (zkClient == null) {
          zkClient = HighAvailableService.newZookeeperClient(conf)
        }
      }
    }
    zkClient
  }

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

