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

package org.apache.kyuubi.plugin.spark.authz.gen

import org.apache.kyuubi.plugin.spark.authz.OperationType._
import org.apache.kyuubi.plugin.spark.authz.serde._
import org.apache.kyuubi.plugin.spark.authz.serde.FunctionType.{SYSTEM, TEMP}

object FunctionCommands {

  val CreateFunction = {
    val cmd = "org.apache.spark.sql.execution.command.CreateFunctionCommand"
    val functionTypeDesc = FunctionTypeDesc(
      "isTemp",
      classOf[TempMarkerFunctionTypeExtractor],
      Seq(TEMP))
    val databaseDesc =
      DatabaseDesc("databaseName", classOf[StringOptionDatabaseExtractor])
    val functionDesc = FunctionDesc(
      "functionName",
      classOf[StringFunctionExtractor],
      Some(databaseDesc),
      functionTypeDesc = Some(functionTypeDesc))
    val functionIdentifierDesc = FunctionDesc(
      "identifier",
      classOf[FunctionIdentifierFunctionExtractor],
      functionTypeDesc = Some(functionTypeDesc))
    FunctionCommandSpec(cmd, Seq(functionIdentifierDesc, functionDesc), CREATEFUNCTION)
  }

  val DescribeFunction = {
    val cmd = "org.apache.spark.sql.execution.command.DescribeFunctionCommand"
    val skips: Seq[String] = Seq(TEMP, SYSTEM)
    val functionTypeDesc1 =
      FunctionTypeDesc("info", classOf[ExpressionInfoFunctionTypeExtractor], skips)
    val functionDesc1 = FunctionDesc(
      "info",
      classOf[ExpressionInfoFunctionExtractor],
      functionTypeDesc = Some(functionTypeDesc1),
      isInput = true)

    val functionTypeDesc2 =
      FunctionTypeDesc(
        "functionName",
        classOf[FunctionIdentifierFunctionTypeExtractor],
        skips)
    val functionDesc2 = FunctionDesc(
      "functionName",
      classOf[FunctionIdentifierFunctionExtractor],
      functionTypeDesc = Some(functionTypeDesc2),
      isInput = true)
    FunctionCommandSpec(cmd, Seq(functionDesc1, functionDesc2), DESCFUNCTION)
  }

  val DropFunction = {
    val cmd = "org.apache.spark.sql.execution.command.DropFunctionCommand"
    CreateFunction.copy(cmd, opType = DROPFUNCTION)
  }

  val RefreshFunction = {
    val cmd = "org.apache.spark.sql.execution.command.RefreshFunctionCommand"
    val databaseDesc =
      DatabaseDesc("databaseName", classOf[StringOptionDatabaseExtractor])
    val functionDesc = FunctionDesc(
      "functionName",
      classOf[StringFunctionExtractor],
      Some(databaseDesc))
    FunctionCommandSpec(cmd, Seq(functionDesc), RELOADFUNCTION)
  }

  val data: Array[FunctionCommandSpec] = Array(
    CreateFunction,
    DropFunction,
    DescribeFunction,
    RefreshFunction).sortBy(_.classname)
}
