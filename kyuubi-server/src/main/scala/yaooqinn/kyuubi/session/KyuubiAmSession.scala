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

package yaooqinn.kyuubi.session

import org.apache.hive.service.cli.thrift.TProtocolVersion
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

import yaooqinn.kyuubi.KyuubiSQLException
import yaooqinn.kyuubi.cli._
import yaooqinn.kyuubi.operation.{OperationHandle, OperationManager}
import yaooqinn.kyuubi.spark.SparkSessionWithUGI

/**
 * An Execution Session with [[SparkSession]] instance inside, which shares [[SparkContext]]
 * with other sessions create by an only user.
 *
 * One user, one [[SparkContext]]
 * One user, multi [[KyuubiAmSession]]s
 *
 * One [[KyuubiAmSession]], one [[SparkSession]]
 * One [[SparkContext]], multi [[SparkSession]]s
 *
 */
private[kyuubi] class KyuubiAmSession(
    protocol: TProtocolVersion,
    username: String,
    password: String,
    conf: SparkConf,
    ipAddress: String,
    withImpersonation: Boolean,
    sessionManager: SessionManager,
    operationManager: OperationManager)
  extends KyuubiSession(protocol,
    username,
    password,
    conf,
    ipAddress,
    withImpersonation,
    sessionManager,
    operationManager) {

  private val amSessionWithUGI =
    new SparkSessionWithUGI(sessionUGI, conf, sessionManager.getAmSessionCacheMgr)

  override def sparkSession: SparkSession = this.amSessionWithUGI.sparkSession

  @throws[KyuubiSQLException]
  override def open(sessionConf: Map[String, String]): Unit = {
    amSessionWithUGI.init(sessionConf)
    lastAccessTime = System.currentTimeMillis
    lastIdleTime = lastAccessTime
  }

}
