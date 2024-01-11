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
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.RunnableCommand

case class KyuubiCreatePathCommand(path: String) extends RunnableCommand with WithInternalChild
  with WithInternalChildren {
  import KyuubiListPathCommand._
  override def withNewChildrenInternal(newChildren: IndexedSeq[LogicalPlan]): LogicalPlan = this
  override def withNewChildInternal(newChild: LogicalPlan): LogicalPlan = this

  override def output: Seq[Attribute] = PATH_ATTRIBUTES

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val toCreate = new Path(path)
    val fs = toCreate.getFileSystem(sparkSession.sessionState.newHadoopConf())
    fs.mkdirs(toCreate)
    Seq(formatPathRow(fs.getFileStatus(toCreate)))
  }
}
