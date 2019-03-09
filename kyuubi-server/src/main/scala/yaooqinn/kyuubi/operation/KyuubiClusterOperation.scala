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

package yaooqinn.kyuubi.operation

import java.io.{File, FileNotFoundException}
import java.security.PrivilegedExceptionAction
import java.util.UUID
import java.util.concurrent.{Future, RejectedExecutionException}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.{FileUtil, Path}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAccessControlException
import org.apache.hadoop.hive.ql.session.OperationLog
import org.apache.hive.service.cli.thrift.TProtocolVersion
import org.apache.spark.KyuubiConf._
import org.apache.spark.KyuubiSparkUtil
import org.apache.spark.scheduler.cluster.KyuubiSparkExecutorUtils
import org.apache.spark.sql.{AnalysisException, DataFrame, Row, SparkSQLUtils}
import org.apache.spark.sql.catalyst.catalog.FunctionResource
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.execution.command.AddJarCommand
import org.apache.spark.sql.types._

import yaooqinn.kyuubi.{KyuubiSQLException, Logging}
import yaooqinn.kyuubi.cli.FetchOrientation
import yaooqinn.kyuubi.schema.{RowSet, RowSetBuilder}
import yaooqinn.kyuubi.session.KyuubiSession
import yaooqinn.kyuubi.ui.KyuubiServerMonitor
import yaooqinn.kyuubi.utils.ReflectUtils

class KyuubiClusterOperation(session: KyuubiSession, statement: String)
  extends Operation(session, statement) with Logging {

  import KyuubiClusterOperation._

  // TODO
  protected val operationTimeout: Long = 0L

  override def createOperationLog(): Unit = {

  }

  override def registerCurrentOperationLog(): Unit = {

  }

  override def unregisterOperationLog(): Unit = {

  }

  override def close(): Unit = {

  }

  override def getResultSetSchema: StructType = {
    null
  }

  override def getNextRowSet(order: FetchOrientation, rowSetSize: Long): RowSet = {
    null
  }


  override def execute(): Unit = {

  }

  override def cleanup(state: OperationState): Unit = {

  }

  override def run(): Unit = {

  }



}

object KyuubiClusterOperation {
  val DEFAULT_FETCH_ORIENTATION: FetchOrientation = FetchOrientation.FETCH_NEXT
  val DEFAULT_FETCH_MAX_ROWS = 100

  def isResourceDownloadable(resource: String): Boolean = {
    val scheme = new Path(resource).toUri.getScheme
    StringUtils.equalsIgnoreCase(scheme, "hdfs")
  }
}

