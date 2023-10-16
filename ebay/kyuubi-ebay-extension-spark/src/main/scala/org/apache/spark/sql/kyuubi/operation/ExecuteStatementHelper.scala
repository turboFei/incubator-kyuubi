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

package org.apache.spark.sql.kyuubi.operation

import scala.collection.JavaConverters._

import org.apache.spark.sql.catalyst.parser.SqlBaseParser
import org.apache.spark.sql.catalyst.parser.SqlBaseParser.{StatementContext, StatementDefaultContext}
import org.apache.spark.sql.execution.{CollectLimitExec, SparkPlan, SparkSqlParser, TakeOrderedAndProjectExec}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.kyuubi.SparkEbayUtils

import org.apache.kyuubi.Logging

object ExecuteStatementHelper extends Logging {
  class DQLParser extends SparkSqlParser {
    override def parse[T](command: String)(toResult: SqlBaseParser => T): T = {
      super.parse(command)(toResult)
    }

    def isStatementQuery(statement: String): Boolean = {
      parse(statement)(_.statement()).isInstanceOf[StatementDefaultContext]
    }

    def getRuleContext(sqlString: String): StatementContext = {
      parse(sqlString)(_.statement())
    }
  }

  private lazy val parser = new DQLParser()

  /**
   * Whether is DQL(data query language), including withCte, select, union
   */
  def isDQL(statement: String): Boolean = {
    try {
      parser.isStatementQuery(statement)
    } catch {
      case e: Throwable =>
        debug(s"error checking whether query $statement is DQL: ${e.getMessage}")
        false
    }
  }

  /**
   * Sortable for select query, noly
   */
  def sortable(sparkPlan: SparkPlan): Boolean = sparkPlan match {
    case c: CollectLimitExec => sortable(c.child)
    case InMemoryTableScanExec(_, _, relation) => relation.outputOrdering.nonEmpty
    case ap: AdaptiveSparkPlanExec => sortable(ap.inputPlan)
    case plan => DownloadDataHelper.sortableForWriteData(plan)
  }

  def existingLimit(sparkPlan: SparkPlan): Option[Int] = sparkPlan match {
    case tp: TakeOrderedAndProjectExec => Option(tp.limit)
    case c: CollectLimitExec => Option(c.limit)
    case ap: AdaptiveSparkPlanExec => existingLimit(ap.inputPlan)
    case _ => None
  }

  def isTopKSort(sparkPlan: SparkPlan): Boolean = sparkPlan match {
    case _: TakeOrderedAndProjectExec => true
    case ap: AdaptiveSparkPlanExec => isTopKSort(ap.inputPlan)
    case _ => false
  }

  // HADP-50714: Truncate the comma at the tail of statement when using temp table collection
  def normalizeStatement(statement: String): String = {
    val statementSeq = SparkEbayUtils.splitSemiColon(statement)
    require(statementSeq.size == 1, s"Only one statement expected: $statementSeq")
    statementSeq.asScala.head
  }
}
