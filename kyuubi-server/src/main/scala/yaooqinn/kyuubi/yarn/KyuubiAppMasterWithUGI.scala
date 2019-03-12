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

import java.util.concurrent.TimeUnit

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.locks.InterProcessMutex
import org.apache.curator.utils.EnsurePath
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.api.records.ApplicationId
import org.apache.spark.KyuubiConf._
import org.apache.spark.KyuubiSparkUtil.findCause
import org.apache.spark.SparkConf
import org.apache.spark.deploy.KyuubiSubmit
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

  private val userName = user.getUserName
  private var instanceNamespace: String = _
  private var instanceParentPath: String = _
  private var promisedKyuubiAmAppId: Promise[ApplicationId] = _
  private var zookeeperClient: CuratorFramework = _
  private var kyuubiAmNamespacePrefix: String = _
  private var lockPath: String = _
  private var lock: InterProcessMutex = _
  private var kyuubiAppMasterException: Option[Throwable] = None

  private lazy val newKyuubiAppMaster: Thread = {
    val threadName = "KyuubiAppMaster-Starter-" + userName
    new Thread(threadName) {
      override def run(): Unit = {
        try {
          val args = new ArrayBuffer[String]()
          args += "--conf"
          args += YARN_KYUUBIAPPMASTER_MODE.key + "=" + "true"
          args += "--conf"
          args += YARN_KYUUBIAPPMASTER_USERNAME.key + "=" + userName
          args += "--conf"
          args += "spark.driver.memory=30g"
          // TODO: driver.extractOption or driver.maxResultSize?
          promisedKyuubiAmAppId.trySuccess {
            KyuubiSubmit.submitKyuubiCluster(args.toArray)
          }
        } catch {
          case e: Exception =>
            kyuubiAppMasterException = Some(e)
            throw e
        }
      }
    }
  }

  private def getOrCreate(): Unit = {
    if (!isIntanceExists()) {
      lock.acquire()
      if (!isIntanceExists()) {
        create()
      }
      lock.release()
    }
  }

  private def create(): Unit = {
    val submitTimeOut = conf.getTimeAsSeconds(YARN_KYUUBIAPPMASTER_SUBMIT_WAITTIME.key)
    try {
      KyuubiHadoopUtil.doAs(user) {
        newKyuubiAppMaster.start()
        val appId =
          Await.result(promisedKyuubiAmAppId.future, Duration(submitTimeOut, TimeUnit.SECONDS))
        if (appId == null) {
          throw new Exception(s"The AppId for $userName's KyuubiAppMaster is null")
        }
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
              throw new KyuubiSQLException(s"KyuubiAppMaster does not publish server in" +
                s"$totalWaitTime seconds and kill it by applicationID $appId")
            }
            if (!KyuubiHadoopUtil.isYarnAppRunning(appId)) {
              throw new KyuubiSQLException("KyuubiAppMaster is not running.")
            }
            info(s"KyuubiAppMaster is publishing server, $totalWaitTime times countdown.")
          }
        }
      }
    } catch {
      case e: Exception =>
        KyuubiHadoopUtil.doAs(user) {
          stopKyuubiAppMaster()
        }
        val ke = new KyuubiSQLException(
          s"Get SparkSession for [$userName] failed", "08S01", 1001, findCause(e))
        kyuubiAppMasterException.foreach(ke.addSuppressed)
        throw ke
    } finally {
      newKyuubiAppMaster.join()
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
      }
    }
  }

  private[this] def ensureLockExists(): Unit = {
    val ensurePath = new EnsurePath(lockPath)
    ensurePath.ensure(zookeeperClient.getZookeeperClient)
  }

  private def isIntanceExists(): Boolean = {
    try {
      zookeeperClient.getChildren.forPath(instanceParentPath) != null
    } catch {
      case e: Exception =>
        error(s"Failed to getChildren for $instanceParentPath")
        false
    }
  }


  @throws[KyuubiSQLException]
  def init(): Unit = {
    kyuubiAmNamespacePrefix = conf.get(YARN_KYUUBIAPPMASTER_NAMESPACE.key) +
      "/" + userName
    lockPath = "/" + kyuubiAmNamespacePrefix + "/" + KyuubiAppMasterWithUGI.lock
    instanceNamespace = kyuubiAmNamespacePrefix + "/" + KyuubiAppMasterWithUGI.instanceNameSpace
    instanceParentPath = "/" + instanceNamespace
    zookeeperClient = HighAvailableService.newZookeeperClient(conf)
    ensureLockExists()
    lock = new InterProcessMutex(zookeeperClient, lockPath)
    getOrCreate()
  }

  def kyuubiAmInstanceNameSpace: String = instanceNamespace
}

object KyuubiAppMasterWithUGI {
  private val lock = "LOCK"
  private val instanceNameSpace = HighAvailableService.URIPATH
}
