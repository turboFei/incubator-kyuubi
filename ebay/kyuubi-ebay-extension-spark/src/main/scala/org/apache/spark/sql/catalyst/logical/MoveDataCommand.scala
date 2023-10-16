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

package org.apache.spark.sql.catalyst.logical

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.kyuubi.SparkEbayUtils
import org.apache.spark.sql.types.{MetadataBuilder, StringType}

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.config.{KyuubiEbayConf, KyuubiReservedKeys}

/**
 * A MOVE DATA INTO PATH statement, as parsed from SQL
 */
case class MoveDataCommand(
    fromPath: String,
    toDir: String,
    toFileName: Option[String],
    isOverwrite: Boolean) extends RunnableCommand with WithInternalChild with WithInternalChildren {
  override def withNewChildrenInternal(newChildren: IndexedSeq[LogicalPlan]): LogicalPlan = this
  override def withNewChildInternal(newChild: LogicalPlan): LogicalPlan = this
  override val output: Seq[Attribute] = Seq(
    AttributeReference(
      "from",
      StringType,
      nullable = false,
      new MetadataBuilder().putString("comment", "from path").build())(),
    AttributeReference(
      "to",
      StringType,
      nullable = false,
      new MetadataBuilder().putString("comment", "to path").build())())

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val hadoopConf = sparkSession.sessionState.newHadoopConf()

    // Check source file
    val sessionId =
      sparkSession.sparkContext.getLocalProperty(KyuubiEbayConf.KYUUBI_SESSION_ID_KEY)
    val sessionUser =
      sparkSession.sparkContext.getLocalProperty(KyuubiReservedKeys.KYUUBI_SESSION_USER_KEY)
    val sessionScratch =
      SparkEbayUtils.getSessionScratchDir(sparkSession, sessionUser, sessionId)
    val fileSystem = sessionScratch.getFileSystem(hadoopConf)

    val srcPath = new Path(sessionScratch + Path.SEPARATOR + fromPath)
    if (!fileSystem.exists(srcPath)) {
      throw KyuubiSQLException(s"MOVE DATA input path does not exist: $srcPath")
    }

    val destDirPath = new Path(toDir)
    if (!fileSystem.exists(destDirPath)) {
      fileSystem.mkdirs(destDirPath)
    }

    val destPath = new Path(destDirPath, toFileName.getOrElse(fromPath))

    if (!isOverwrite && fileSystem.exists(destPath)) {
      throw KyuubiSQLException(s"Dest path already exists: $destPath")
    }

    fileSystem.delete(destPath, true)
    if (!fileSystem.rename(srcPath, destPath)) {
      throw KyuubiSQLException(s"Could not rename $srcPath to $destPath")
    }
    logInfo(s"Moved data from $srcPath to $destPath")
    Seq(Row(srcPath.toUri.toString, destPath.toUri.toString))
  }
}
