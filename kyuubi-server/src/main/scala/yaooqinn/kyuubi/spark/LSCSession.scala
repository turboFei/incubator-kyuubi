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
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.concurrent.{Await, Promise}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.util.control.NonFatal

import yaooqinn.kyuubi.Logging
import yaooqinn.kyuubi.utils.{KyuubiHadoopUtil, ReflectUtils}


class LSCSession(val userName: String, val sparkSession: SparkSession) extends AbstractSession {

  def executeStatement(code: String): DataFrame = {
    sparkSession.sql(code)
  }

}

object LSCSession {
  def create(userName: String, sparkSession: SparkSession): LSCSession = {
    new LSCSession(userName, sparkSession)
  }
}
