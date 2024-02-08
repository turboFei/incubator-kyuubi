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

package org.apache.spark.sql.kyuubi

import java.util
import java.util.{Map => JMap}
import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.expressions.{Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.functions.substring
import org.apache.spark.sql.kyuubi.FieldsTruncationHandler.{getImplClassKey, getTypeLevelTruncationEnabledKey}
import org.apache.spark.sql.kyuubi.KyuubiEbaySQLConf.FIELDS_TRUNCATION_STRING_TYPE_MAX_LENGTH
import org.apache.spark.sql.types.{DataType, StringType}

import org.apache.kyuubi.Logging

trait FieldTruncationHandler {

  def truncate(column: Column): Option[Column]

}

class StringTypeFieldTruncationHandler extends FieldTruncationHandler with SQLConfHelper {

  private def maxLimit(): Int = {
    conf.getConf(FIELDS_TRUNCATION_STRING_TYPE_MAX_LENGTH)
  }

  def truncate(column: Column): Option[Column] = {
    column.expr.dataType match {
      case _: StringType => Some(substring(column, 0, maxLimit()))
      case _ => None
    }
  }

}

object FieldsTruncationHandler {

  final val TRUNCATION_KEY_PREFIX = "spark.sql.execution.result.truncation"

  def getImplClassKey(dataType: DataType): String = {
    s"$TRUNCATION_KEY_PREFIX.${dataType.typeName}.impl"
  }

  def getTypeLevelTruncationEnabledKey(dataType: DataType): String = {
    s"$TRUNCATION_KEY_PREFIX.${dataType.typeName}.enabled"
  }

}
class FieldsTruncationHandler extends SQLConfHelper with Logging {

  private val typeAndTruncationHandlers: JMap[String, FieldTruncationHandler] =
    new ConcurrentHashMap()

  private def tryBuildTruncationHandler(dataType: DataType): Option[FieldTruncationHandler] = {

    val implClass = dataType match {
      // official supported handler
      case _: StringType => Some(conf.getConf(KyuubiEbaySQLConf.FIELDS_TRUNCATION_STRING_TYPE_IMPL))
      // other type, can be defined by user
      case otherType => Option(conf.getConfString(getImplClassKey(otherType), null))
    }

    implClass match {
      case Some(clazz) =>
        try {
          Some(Class.forName(clazz).getConstructor().newInstance().asInstanceOf[
            FieldTruncationHandler])
        } catch {
          case t: Throwable =>
            error(s"Failed to create field truncation handler" +
              s" of type: ${dataType.typeName} because: ${t.getMessage}")
            None
        }
      case _ => None
    }
  }

  private def getTruncationHandlerByType(dataType: DataType): Option[FieldTruncationHandler] = {
    Option(typeAndTruncationHandlers.get(dataType.typeName)) match {
      case exitingHandler @ Some(_) => exitingHandler
      case None =>
        tryBuildTruncationHandler(dataType) match {
          case builtSuccess @ Some(handler) =>
            typeAndTruncationHandlers.put(dataType.typeName, handler)
            builtSuccess
          case None => None
        }
    }
  }

  def tryTruncateOrIgnore(column: Column): Option[Column] = {
    val dataType = column.expr.dataType
    getTruncationHandlerByType(dataType) match {
      case Some(handler) => handler.truncate(column)
      case _ =>
        debug(s"No handler for data type: ${dataType.typeName}")
        None
    }
  }
}

case class TruncateFieldsOnCollectResult(session: SparkSession) extends Rule[LogicalPlan] {

  private val handler: FieldsTruncationHandler = new FieldsTruncationHandler()

  private def isTruncationEnabledOnDataType(dataType: DataType): Boolean = {
    conf.getConfString(getTypeLevelTruncationEnabledKey(dataType), "true").toBoolean
  }

  private def tryRewriteChildProjectIfMatch(plan: LogicalPlan): LogicalPlan = {

    val truncationEnabledOnType: JMap[String, Boolean] = new util.HashMap[String, Boolean]()

    def checkTruncationEnabledOnType(dataType: DataType): Boolean = {
      truncationEnabledOnType.computeIfAbsent(
        dataType.typeName,
        (_) => isTruncationEnabledOnDataType(dataType))
    }

    @inline
    def rewriteProject(project: Project, orderByList: Seq[Expression]): LogicalPlan = {
      val truncatedProject = project.mapExpressions {
        case attr: NamedExpression if checkTruncationEnabledOnType(attr.dataType) =>
          orderByList.find(_.references.contains(attr.toAttribute)) match {
            case Some(order) =>
              logWarning(
                s"Skip field ${attr.name} as it is referred by the order expression: $order")
              attr
            case None =>
              handler.tryTruncateOrIgnore(new Column(attr)) match {
                case Some(truncated) =>
                  // Preserve the original column name
                  truncated.alias(attr.name).expr
                case _ => attr
              }
          }
        case other => other
      }
      truncatedProject
    }

    plan match {
      case globalLimit @ GlobalLimit(_, localLimit @ LocalLimit(_, project @ Project(_, _))) =>
        val rewrittenChild = rewriteProject(project, Seq.empty)
        globalLimit.copy(child = localLimit.copy(child = rewrittenChild))
      case globalLimit @ GlobalLimit(_, project: Project) =>
        val rewrittenChild = rewriteProject(project, Seq.empty)
        globalLimit.copy(child = rewrittenChild)
      case globalLimit @ GlobalLimit(
            _,
            localLimit @ LocalLimit(_, sort @ Sort(order, _, project @ Project(_, _)))) =>
        // if the field is in the sort expression, skip truncate
        val rewrittenChild = rewriteProject(project, order.map(order => order.child))
        globalLimit.copy(child = localLimit.copy(child = sort.copy(child = rewrittenChild)))
      case globalLimit @ GlobalLimit(_, sort @ Sort(order, _, project @ Project(_, _))) =>
        // if the field is in the sort expression, skip truncate
        val rewrittenChild = rewriteProject(project, order.map(order => order.child))
        globalLimit.copy(child = sort.copy(child = rewrittenChild))
      case sort @ Sort(order, _, project @ Project(_, _)) =>
        val rewrittenChild = rewriteProject(project, order.map(order => order.child))
        sort.copy(child = rewrittenChild)
      case project: Project =>
        rewriteProject(project, Seq.empty)
      case _ => plan
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (conf.getConf(KyuubiEbaySQLConf.FIELDS_TRUNCATION_ENABLED)) {
      logInfo("Field truncation enabled, applying rule...")
      applyTruncationRule(plan)
    } else {
      plan
    }
  }

  private def applyTruncationRule(plan: LogicalPlan): LogicalPlan = {
    tryRewriteChildProjectIfMatch(plan)
  }
}
