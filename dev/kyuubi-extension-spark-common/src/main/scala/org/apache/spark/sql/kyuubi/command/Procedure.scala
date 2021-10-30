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

package org.apache.spark.sql.kyuubi.command

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow

trait Procedure {
  val parameters: Seq[ProcedureParameter]
  def exec(args: InternalRow): Seq[Row]
  def description: String
}

case object StopEngineProcedure extends Procedure {
  override val parameters: Seq[ProcedureParameter] = Seq.empty[ProcedureParameter]

  override def exec(args: InternalRow): Seq[Row] = {
    SparkSession.getActiveSession.foreach(_.stop())
    Seq.empty[Row]
  }

  override def description: String = "stop the engine"
}