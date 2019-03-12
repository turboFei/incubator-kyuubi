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

import java.util.concurrent.{Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.locks.InterProcessMutex
import org.apache.curator.utils.EnsurePath
import org.apache.spark.KyuubiConf._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import yaooqinn.kyuubi.Logging
import yaooqinn.kyuubi.ha.HighAvailableService
import yaooqinn.kyuubi.service.AbstractService

class KyuubiAmSessionManager private(name: String) extends AbstractService(name) with Logging {

  private val cacheManager =
    Executors.newSingleThreadScheduledExecutor(
      new ThreadFactoryBuilder()
        .setDaemon(true).setNameFormat(getClass.getSimpleName + "-%d").build())

  @volatile
  private var sparkSession: Option[SparkSession] = None
  private var timesCount: AtomicInteger = new AtomicInteger()
  @volatile
  private var latestLogout: Long = _
  private var idleTimeout: Long = _
  private var kyuubiAppMaster: Option[KyuubiAppMaster] = None
  private var kyuubiAmNamespace: String = _
  private var userName: String = _
  private var zookeeperClient: CuratorFramework = _

  def this() = this(classOf[KyuubiAmSessionManager].getSimpleName)

  def this(kyuubiAm: Option[KyuubiAppMaster]) = {
    this()
    this.kyuubiAppMaster = kyuubiAm
  }

  private val sessionCleaner = new Runnable {
    override def run(): Unit = {
      (sparkSession, timesCount) match {
        case (Some(_), times) if times.get() > 0 =>
          debug(s"There are $times active connection(s) bound to the SparkSession instance")

        case (Some(ss), _)
          if latestLogout + idleTimeout <= System.currentTimeMillis() =>
          info(s"Stoping the kyuubiAppMaster")
          ss.stop()
          kyuubiAppMaster match {
            case Some(kam) =>
              val lock = getAmInstanceLock
              lock.acquire()
              kam.offlineHaServiceFirst()
              lock.release()
              kam.stopForIdleTimeOut(s"Stop the kyuubiAppMaster for idle more than " +
                s"$idleTimeout seconds.")
            case _ =>
          }
        case _ =>
      }
    }
  }

  def getAmInstanceLock: InterProcessMutex = {
    // TODO: "LOCK"=KyuubiAppMasterWithUGI.LOCK
    val lockPath = "/" +kyuubiAmNamespace + "/" + userName + "/" + "LOCK"
    val ensurePath = new EnsurePath(lockPath)
    ensurePath.ensure(zookeeperClient.getZookeeperClient)
    new InterProcessMutex(zookeeperClient, lockPath)
  }

  def set(userName: String, session: SparkSession): Unit = {
    this.synchronized {
      sparkSession = Some(session)
      this.userName = userName
    }
  }

  def getAndIncrease(): Option[SparkSession] = {
    (sparkSession, timesCount) match {
      case (Some(ss), times) if !ss.sparkContext.isStopped =>
        val currentTime = times.incrementAndGet()
        info(s"SparkSession is reused for $currentTime time(s) after + 1")
        Some(ss)
      case _ =>
        info(s"SparkSession isn't cached, will create a new one.")
        None
    }
  }

  def decrease(): Unit = {
    (sparkSession, timesCount) match {
      case (Some(ss), times) if !ss.sparkContext.isStopped =>
        latestLogout = System.currentTimeMillis()
        val currentTime = times.decrementAndGet()
        info(s"SparkSession is reused for $currentTime time(s) after - 1")
      case _ =>
        warn(s"SparkSession was not found in the cache.")
    }
  }

  override def init(conf: SparkConf): Unit = {
    idleTimeout = math.max(conf.getTimeAsMs(YARN_KYUUBIAPPMASTER_IDLE_TIMEOUT.key), 60 * 1000)
    kyuubiAmNamespace = conf.get(YARN_KYUUBIAPPMASTER_NAMESPACE.key)
    zookeeperClient = HighAvailableService.newZookeeperClient(conf)
    super.init(conf)
  }

  /**
   * Periodically close idle SparkSessions in 'spark.kyuubi.session.clean.interval(default 1min)'
   */
  override def start(): Unit = {
    // at least 1 minutes
    val interval = math.max(conf.getTimeAsSeconds(BACKEND_SESSION_CHECK_INTERVAL.key), 1)
    info(s"Scheduling SparkSession cache cleaning every $interval seconds")
    cacheManager.scheduleAtFixedRate(sessionCleaner, interval, interval, TimeUnit.SECONDS)
    super.start()
  }

  override def stop(): Unit = {
    info("Stopping SparkSession Cache Manager")
    cacheManager.shutdown()
    sparkSession.foreach( ss => ss.stop())
    super.stop()
  }
}
