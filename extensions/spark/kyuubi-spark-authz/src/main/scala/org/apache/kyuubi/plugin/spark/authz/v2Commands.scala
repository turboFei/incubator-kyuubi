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

package org.apache.kyuubi.plugin.spark.authz

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

import org.apache.kyuubi.plugin.spark.authz.OperationType._
import org.apache.kyuubi.plugin.spark.authz.PrivilegeObjectActionType.PrivilegeObjectActionType
import org.apache.kyuubi.plugin.spark.authz.PrivilegeObjectType.TABLE_OR_VIEW
import org.apache.kyuubi.plugin.spark.authz.PrivilegesBuilder._
import org.apache.kyuubi.plugin.spark.authz.util.AuthZUtils._
import org.apache.kyuubi.plugin.spark.authz.v2Commands.CommandType.{CommandType, HasChildAsIdentifier, HasQueryAsLogicalPlan, HasTableAsIdentifier, HasTableAsIdentifierOption, HasTableNameAsIdentifier}

/**
 * Building privilege objects
 * for Spark Datasource V2 commands
 */
object v2Commands extends Enumeration {

  /**
   * Command type enum
   * with naming rule as `HasFieldAsReturnType`
   * for hinting privileges building of inputObjs or outputObjs
   */
  object CommandType extends Enumeration {
    type CommandType = Value
    val HasChildAsIdentifier, HasQueryAsLogicalPlan, HasTableAsIdentifier,
        HasTableAsIdentifierOption, HasTableNameAsIdentifier = Value
  }

  import scala.language.implicitConversions

  implicit def valueToCmdPrivilegeBuilder(x: Value): CmdPrivilegeBuilder =
    x.asInstanceOf[CmdPrivilegeBuilder]

  /**
   * check whether commandName is implemented with supported privilege builders
   * and pass the requirement checks (e.g. Spark version)
   *
   * @param commandName name of command
   * @return true if so, false else
   */
  def accept(commandName: String): Boolean = {
    try {
      val command = v2Commands.withName(commandName)

      // check spark version requirements
      passSparkVersionCheck(command.mostVer, command.leastVer)

    } catch {
      case _: NoSuchElementException => false
    }
  }

  val defaultBuildInput: (LogicalPlan, ArrayBuffer[PrivilegeObject], Seq[CommandType]) => Unit =
    (plan, inputObjs, commandTypes) => {
      commandTypes.foreach {
        case HasQueryAsLogicalPlan =>
          val query = getFieldVal[LogicalPlan](plan, "query")
          buildQuery(query, inputObjs)
        case _ =>
      }
    }

  val defaultBuildOutput: (
      LogicalPlan,
      ArrayBuffer[PrivilegeObject],
      Seq[CommandType],
      PrivilegeObjectActionType) => Unit =
    (plan, outputObjs, commandTypes, outputObjsActionType) => {
      commandTypes.foreach {
        case HasTableNameAsIdentifier =>
          val table = invoke(plan, "tableName").asInstanceOf[Identifier]
          outputObjs += v2TablePrivileges(table)

        case HasTableAsIdentifierOption =>
          val table = getFieldVal[AnyRef](plan, "table")
          val tableIdent = getFieldVal[Option[Identifier]](table, "identifier")
          if (tableIdent.isDefined) {
            outputObjs += v2TablePrivileges(tableIdent.get, actionType = outputObjsActionType)
          }

        case HasTableAsIdentifier =>
          val table = getFieldVal[LogicalPlan](plan, "table")
          val tableIdent = getFieldVal[Identifier](table, "identifier")
          outputObjs += v2TablePrivileges(tableIdent)

        case HasChildAsIdentifier =>
          val table = getFieldVal[AnyRef](plan, "child")
          val tableIdent = getFieldVal[Identifier](table, "identifier")
          outputObjs += v2TablePrivileges(tableIdent)

        case _ =>
      }
    }

  /**
   * Command privilege builder
   *
   * @param operationType    OperationType for converting accessType
   * @param leastVer         minimum Spark version required
   * @param mostVer          maximum Spark version supported
   * @param commandTypes     Seq of [[CommandType]] hinting privilege building
   * @param buildInput       input [[PrivilegeObject]] for privilege check
   * @param buildOutput      output [[PrivilegeObject]] for privilege check
   * @param outputActionType [[PrivilegeObjectActionType]] for output [[PrivilegeObject]]
   */
  case class CmdPrivilegeBuilder(
      operationType: OperationType = QUERY,
      leastVer: Option[String] = None,
      mostVer: Option[String] = None,
      commandTypes: Seq[CommandType] = Seq.empty,
      buildInput: (LogicalPlan, ArrayBuffer[PrivilegeObject], Seq[CommandType]) => Unit =
        defaultBuildInput,
      buildOutput: (
          LogicalPlan,
          ArrayBuffer[PrivilegeObject],
          Seq[CommandType],
          PrivilegeObjectActionType) => Unit = defaultBuildOutput,
      outputActionType: PrivilegeObjectActionType = PrivilegeObjectActionType.OTHER)
    extends super.Val {

    def buildPrivileges(
        plan: LogicalPlan,
        inputObjs: ArrayBuffer[PrivilegeObject],
        outputObjs: ArrayBuffer[PrivilegeObject]): Unit = {
      this.buildInput(plan, inputObjs, commandTypes)
      this.buildOutput(plan, outputObjs, commandTypes, outputActionType)
    }
  }

  def v2TablePrivileges(
      table: Identifier,
      columns: Seq[String] = Nil,
      actionType: PrivilegeObjectActionType = PrivilegeObjectActionType.OTHER): PrivilegeObject = {
    PrivilegeObject(TABLE_OR_VIEW, actionType, quote(table.namespace()), table.name(), columns)
  }

  // namespace commands

  val CreateNamespace: CmdPrivilegeBuilder = CmdPrivilegeBuilder(
    operationType = CREATEDATABASE,
    buildOutput = (plan, outputObjs, _, _) => {
      if (isSparkVersionAtLeast("3.3")) {
        val resolvedNamespace = getFieldVal[Any](plan, "name")
        val databases = getFieldVal[Seq[String]](resolvedNamespace, "nameParts")
        outputObjs += databasePrivileges(quote(databases))
      } else {
        val namespace = getFieldVal[Seq[String]](plan, "namespace")
        outputObjs += databasePrivileges(quote(namespace))
      }
    })

  val DropNamespace: CmdPrivilegeBuilder = CmdPrivilegeBuilder(
    operationType = DROPDATABASE,
    buildOutput = (plan, outputObjs, _, _) => {
      val resolvedNamespace = getFieldVal[LogicalPlan](plan, "namespace")
      val databases = getFieldVal[Seq[String]](resolvedNamespace, "namespace")
      outputObjs += databasePrivileges(quote(databases))
    })

  // with V2CreateTablePlan

  val CreateTable: CmdPrivilegeBuilder = CmdPrivilegeBuilder(
    operationType = CREATETABLE,
    commandTypes = Seq(HasTableNameAsIdentifier),
    leastVer = Some("3.3"))

  val CreateV2Table: CmdPrivilegeBuilder = CmdPrivilegeBuilder(
    operationType = CREATETABLE,
    commandTypes = Seq(HasTableNameAsIdentifier),
    mostVer = Some("3.2"))

  val CreateTableAsSelect: CmdPrivilegeBuilder = CmdPrivilegeBuilder(
    operationType = CREATETABLE,
    commandTypes = Seq(HasTableNameAsIdentifier, HasQueryAsLogicalPlan))

  val ReplaceTable: CmdPrivilegeBuilder = CmdPrivilegeBuilder(
    operationType = CREATETABLE,
    commandTypes = Seq(HasTableNameAsIdentifier))

  val ReplaceTableAsSelect: CmdPrivilegeBuilder = CmdPrivilegeBuilder(
    operationType = CREATETABLE,
    commandTypes = Seq(HasTableNameAsIdentifier, HasQueryAsLogicalPlan))

  // with V2WriteCommand

  val AppendData: CmdPrivilegeBuilder = CmdPrivilegeBuilder(
    commandTypes = Seq(HasTableAsIdentifierOption, HasQueryAsLogicalPlan),
    outputActionType = PrivilegeObjectActionType.INSERT)

  val UpdateTable: CmdPrivilegeBuilder = CmdPrivilegeBuilder(
    commandTypes = Seq(HasTableAsIdentifierOption),
    outputActionType = PrivilegeObjectActionType.UPDATE)

  val DeleteFromTable: CmdPrivilegeBuilder = CmdPrivilegeBuilder(
    commandTypes = Seq(HasTableAsIdentifierOption),
    outputActionType = PrivilegeObjectActionType.UPDATE)

  val OverwriteByExpression: CmdPrivilegeBuilder = CmdPrivilegeBuilder(
    commandTypes = Seq(HasTableAsIdentifierOption, HasQueryAsLogicalPlan),
    outputActionType = PrivilegeObjectActionType.UPDATE)

  val OverwritePartitionsDynamic: CmdPrivilegeBuilder = CmdPrivilegeBuilder(
    commandTypes = Seq(HasTableAsIdentifierOption, HasQueryAsLogicalPlan),
    outputActionType = PrivilegeObjectActionType.UPDATE)

  // with V2PartitionCommand

  val AddPartitions: CmdPrivilegeBuilder = CmdPrivilegeBuilder(
    operationType = OperationType.ALTERTABLE_ADDPARTS,
    leastVer = Some("3.2"),
    commandTypes = Seq(HasTableAsIdentifier))

  val DropPartitions: CmdPrivilegeBuilder = CmdPrivilegeBuilder(
    operationType = OperationType.ALTERTABLE_DROPPARTS,
    leastVer = Some("3.2"),
    commandTypes = Seq(HasTableAsIdentifier))

  val RenamePartitions: CmdPrivilegeBuilder = CmdPrivilegeBuilder(
    operationType = OperationType.ALTERTABLE_ADDPARTS,
    leastVer = Some("3.2"),
    commandTypes = Seq(HasTableAsIdentifier))

  val TruncatePartition: CmdPrivilegeBuilder = CmdPrivilegeBuilder(
    operationType = OperationType.ALTERTABLE_DROPPARTS,
    leastVer = Some("3.2"),
    commandTypes = Seq(HasTableAsIdentifier))

  // other table commands

  val CacheTable: CmdPrivilegeBuilder = CmdPrivilegeBuilder(
    operationType = CREATEVIEW,
    leastVer = Some("3.2"),
    buildInput = (plan, inputObjs, _) => {
      val query = getFieldVal[LogicalPlan](plan, "table") // table to cache
      buildQuery(query, inputObjs)
    })

  val CacheTableAsSelect: CmdPrivilegeBuilder = CmdPrivilegeBuilder(
    operationType = CREATEVIEW,
    leastVer = Some("3.2"),
    buildInput = (plan, inputObjs, _) => {
      val query = getFieldVal[LogicalPlan](plan, "plan")
      buildQuery(query, inputObjs)
    })

  val CommentOnNamespace: CmdPrivilegeBuilder = CmdPrivilegeBuilder(
    operationType = ALTERDATABASE,
    buildOutput = (plan, outputObjs, _, _) => {
      val resolvedNamespace = getFieldVal[AnyRef](plan, "child")
      val namespace = getFieldVal[Seq[String]](resolvedNamespace, "namespace")
      outputObjs += databasePrivileges(quote(namespace))
    })

  val CommentOnTable: CmdPrivilegeBuilder = CmdPrivilegeBuilder(
    operationType = ALTERTABLE_PROPERTIES,
    commandTypes = Seq(
      if (isSparkVersionAtLeast("3.2")) HasTableAsIdentifier else HasChildAsIdentifier))

  val DropTable: CmdPrivilegeBuilder = CmdPrivilegeBuilder(
    operationType = DROPTABLE,
    buildOutput = (plan, outputObjs, _, _) => {
      val tableIdent =
        if (isSparkVersionAtLeast("3.1")) {
          val resolvedTable = getFieldVal[LogicalPlan](plan, "child")
          getFieldVal[Identifier](resolvedTable, "identifier")
        } else {
          getFieldVal[Identifier](plan, "ident")
        }
      outputObjs += v2TablePrivileges(tableIdent)
    })
  val MergeIntoTable: CmdPrivilegeBuilder = CmdPrivilegeBuilder(
    buildInput = (plan, inputObjs, _) => {
      val table = getFieldVal[DataSourceV2Relation](plan, "sourceTable")
      buildQuery(table, inputObjs)
    },
    buildOutput = (plan, outputObjs, _, _) => {
      val table = getFieldVal[DataSourceV2Relation](plan, "targetTable")
      if (table.identifier.isDefined) {
        outputObjs += v2TablePrivileges(
          table.identifier.get,
          actionType = PrivilegeObjectActionType.UPDATE)
      }
    })

  val RepairTable: CmdPrivilegeBuilder = CmdPrivilegeBuilder(
    operationType = ALTERTABLE_ADDPARTS,
    leastVer = Some("3.2"),
    commandTypes = Seq(HasChildAsIdentifier))

  val TruncateTable: CmdPrivilegeBuilder = CmdPrivilegeBuilder(
    leastVer = Some("3.2"),
    buildOutput = (plan, outputObjs, _, _) => {
      val table = getFieldVal[Any](plan, "table")
      val tableIdent = getFieldVal[Identifier](table, "identifier")
      outputObjs += v2TablePrivileges(tableIdent, actionType = PrivilegeObjectActionType.UPDATE)
    })

  // with V2AlterTableCommand

  val AlterTable: CmdPrivilegeBuilder = CmdPrivilegeBuilder(
    operationType = ALTERTABLE_ADDCOLS,
    mostVer = Some("3.1"),
    buildOutput = (plan, outputObjs, _, _) => {
      val table = getFieldVal[Any](plan, "table")
      val tableIdent = getFieldVal[Option[Identifier]](table, "identifier")
      if (tableIdent.isDefined) {
        outputObjs += v2TablePrivileges(tableIdent.get)
      }
    })

  val AddColumns: CmdPrivilegeBuilder = CmdPrivilegeBuilder(
    operationType = ALTERTABLE_ADDCOLS,
    leastVer = Some("3.2"),
    commandTypes = Seq(HasTableAsIdentifier))

  val AlterColumn: CmdPrivilegeBuilder = CmdPrivilegeBuilder(
    operationType = ALTERTABLE_ADDCOLS,
    leastVer = Some("3.2"),
    commandTypes = Seq(HasTableAsIdentifier))

  val DropColumns: CmdPrivilegeBuilder = CmdPrivilegeBuilder(
    operationType = ALTERTABLE_ADDCOLS,
    leastVer = Some("3.2"),
    commandTypes = Seq(HasTableAsIdentifier))

  val ReplaceColumns: CmdPrivilegeBuilder = CmdPrivilegeBuilder(
    operationType = ALTERTABLE_REPLACECOLS,
    leastVer = Some("3.2"),
    commandTypes = Seq(HasTableAsIdentifier))

  val RenameColumn: CmdPrivilegeBuilder = CmdPrivilegeBuilder(
    operationType = ALTERTABLE_RENAMECOL,
    leastVer = Some("3.2"),
    commandTypes = Seq(HasTableAsIdentifier))
}
