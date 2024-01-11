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

import java.util.Locale

import scala.collection.mutable.ListBuffer

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.util.StringUtils
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.types.{BooleanType, MetadataBuilder, StringType}

case class KyuubiListPathCommand(path: String, limitOpt: Option[Int])
  extends RunnableCommand with WithInternalChild
  with WithInternalChildren {
  import KyuubiListPathCommand._

  override def withNewChildrenInternal(newChildren: IndexedSeq[LogicalPlan]): LogicalPlan = this
  override def withNewChildInternal(newChild: LogicalPlan): LogicalPlan = this

  override def output: Seq[Attribute] = PATH_ATTRIBUTES
  override def run(sparkSession: SparkSession): Seq[Row] = {
    val toList = new Path(path)
    val fs = toList.getFileSystem(sparkSession.sessionState.newHadoopConf())
    val filesIter = fs.listStatusIterator(toList)
    val limit = limitOpt.getOrElse(DEFAULT_LIMIT)

    val rows = ListBuffer[Row]()
    var count = 0
    while (filesIter.hasNext && count < limit) {
      rows += formatPathRow(filesIter.next())
      count += 1
    }
    rows
  }
}

object KyuubiListPathCommand {
  final private val DEFAULT_LIMIT = 1000
  final private val DATE_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm", Locale.US)
  private def formatSize(len: Long): String =
    StringUtils.TraditionalBinaryPrefix.long2String(len, "", 1)

  final val PATH_ATTRIBUTES: Seq[Attribute] =
    Seq(
      AttributeReference(
        "directory",
        BooleanType,
        nullable = false,
        new MetadataBuilder().putString("comment", "directory or file").build())(),
      AttributeReference(
        "permission",
        StringType,
        nullable = false,
        new MetadataBuilder().putString("comment", "permission").build())(),
      AttributeReference(
        "replication",
        StringType,
        nullable = false,
        new MetadataBuilder().putString("comment", "file replication").build())(),
      AttributeReference(
        "owner",
        StringType,
        nullable = false,
        new MetadataBuilder().putString("comment", "owner").build())(),
      AttributeReference(
        "group",
        StringType,
        nullable = false,
        new MetadataBuilder().putString("comment", "group").build())(),
      AttributeReference(
        "size",
        StringType,
        nullable = false,
        new MetadataBuilder().putString("comment", "size").build())(),
      AttributeReference(
        "accessTime",
        StringType,
        nullable = false,
        new MetadataBuilder().putString("comment", "accessTime").build())(),
      AttributeReference(
        "modificationTime",
        StringType,
        nullable = false,
        new MetadataBuilder().putString("comment", "modificationTime").build())(),
      AttributeReference(
        "path",
        StringType,
        nullable = false,
        new MetadataBuilder().putString("comment", "path").build())())

  def formatPathRow(fileStatus: FileStatus): Row = {
    Row(
      fileStatus.isDirectory,
      fileStatus.getPermission.toString,
      if (fileStatus.isFile) fileStatus.getReplication.toString else "-",
      fileStatus.getOwner,
      fileStatus.getGroup,
      formatSize(fileStatus.getLen),
      DATE_FORMAT.format(fileStatus.getAccessTime),
      DATE_FORMAT.format(fileStatus.getModificationTime),
      fileStatus.getPath.toString)
  }
}
