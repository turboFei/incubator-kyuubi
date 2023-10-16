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

import java.io.Closeable
import java.util.{ArrayList => JArrayList, List => JList}

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.command.{DataWritingCommandExec, ExecutedCommandExec}
import org.apache.spark.sql.execution.datasources.InsertIntoDataSourceCommand
import org.apache.spark.sql.execution.datasources.v2.{AppendDataExecV1, OverwriteByExpressionExecV1, V2TableWriteExec}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.util.Utils

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf

object SparkEbayUtils extends Logging {
  lazy val kyuubiConf: KyuubiConf = KyuubiConf()

  def getSessionScratchDir(
      spark: SparkSession,
      user: String,
      sessionId: String): Path = {
    val scratchPath =
      new Path(spark.sessionState.conf.getConfString("spark.scratchdir", "/tmp/spark"))
    val fileSystem = scratchPath.getFileSystem(spark.sparkContext.hadoopConfiguration)
    fileSystem.makeQualified(new Path(scratchPath, Seq(user, sessionId).mkString(Path.SEPARATOR)))
  }

  def clearTempTables(spark: SparkSession): Unit = {
    try {
      spark.sqlContext.getClass.getMethod("clearTempTables").invoke(spark.sqlContext)
    } catch {
      case _: NoSuchMethodException => warn("No clearTempTables method defined.")
    }
  }

  def bytesToString(size: BigInt): String = Utils.bytesToString(size)

  def tryWithResource[R <: Closeable, T](createResource: => R)(f: R => T): T =
    Utils.tryWithResource(createResource)(f)

  def splitSemiColon(line: String): JList[String] = {
    var insideSingleQuote = false
    var insideDoubleQuote = false
    var insideSimpleComment = false
    var bracketedCommentLevel = 0
    var escape = false
    var beginIndex = 0
    var leavingBracketedComment = false
    var isStatement = false
    val ret = new JArrayList[String]

    def insideBracketedComment: Boolean = bracketedCommentLevel > 0
    def insideComment: Boolean = insideSimpleComment || insideBracketedComment
    def statementInProgress(index: Int): Boolean = isStatement || (!insideComment &&
      index > beginIndex && !s"${line.charAt(index)}".trim.isEmpty)

    for (index <- 0 until line.length) {
      // Checks if we need to decrement a bracketed comment level; the last character '/' of
      // bracketed comments is still inside the comment, so `insideBracketedComment` must keep true
      // in the previous loop and we decrement the level here if needed.
      if (leavingBracketedComment) {
        bracketedCommentLevel -= 1
        leavingBracketedComment = false
      }

      if (line.charAt(index) == '\'' && !insideComment) {
        // take a look to see if it is escaped
        // See the comment above about SPARK-31595
        if (!escape && !insideDoubleQuote) {
          // flip the boolean variable
          insideSingleQuote = !insideSingleQuote
        }
      } else if (line.charAt(index) == '\"' && !insideComment) {
        // take a look to see if it is escaped
        // See the comment above about SPARK-31595
        if (!escape && !insideSingleQuote) {
          // flip the boolean variable
          insideDoubleQuote = !insideDoubleQuote
        }
      } else if (line.charAt(index) == '-') {
        val hasNext = index + 1 < line.length
        if (insideDoubleQuote || insideSingleQuote || insideComment) {
          // Ignores '-' in any case of quotes or comment.
          // Avoids to start a comment(--) within a quoted segment or already in a comment.
          // Sample query: select "quoted value --"
          //                                    ^^ avoids starting a comment if it's inside quotes.
        } else if (hasNext && line.charAt(index + 1) == '-') {
          // ignore quotes and ; in simple comment
          insideSimpleComment = true
        }
      } else if (line.charAt(index) == ';') {
        if (insideSingleQuote || insideDoubleQuote || insideComment) {
          // do not split
        } else {
          if (isStatement) {
            // split, do not include ; itself
            ret.add(line.substring(beginIndex, index))
          }
          beginIndex = index + 1
          isStatement = false
        }
      } else if (line.charAt(index) == '\n') {
        // with a new line the inline simple comment should end.
        if (!escape) {
          insideSimpleComment = false
        }
      } else if (line.charAt(index) == '/' && !insideSimpleComment) {
        val hasNext = index + 1 < line.length
        if (insideSingleQuote || insideDoubleQuote) {
          // Ignores '/' in any case of quotes
        } else if (insideBracketedComment && line.charAt(index - 1) == '*') {
          // Decrements `bracketedCommentLevel` at the beginning of the next loop
          leavingBracketedComment = true
        } else if (hasNext && !insideBracketedComment && line.charAt(index + 1) == '*') {
          bracketedCommentLevel += 1
        }
      }
      // set the escape
      if (escape) {
        escape = false
      } else if (line.charAt(index) == '\\') {
        escape = true
      }

      isStatement = statementInProgress(index)
    }
    if (isStatement) {
      ret.add(line.substring(beginIndex))
    }
    ret
  }

  def withMetrics(
      qe: QueryExecution,
      setNumOutputRows: (Map[String, SQLMetric], String) => Unit): Unit = {
    try {
      val checkedSparkPlan = qe.executedPlan match {
        case a: AdaptiveSparkPlanExec =>
          a.executedPlan
        case plan => plan
      }
      checkedSparkPlan match {
        case DataWritingCommandExec(cmd, _) =>
          setNumOutputRows(cmd.metrics, "numOutputRows")
        case ExecutedCommandExec(cmd) =>
          cmd match {
            case c: InsertIntoDataSourceCommand =>
              setNumOutputRows(c.metrics, "numOutputRows")
            case _ => // TODO: Support delta Update(WithJoin)Command/Delete(WithJoin)Command
          }
        case a: AppendDataExecV1 =>
          setNumOutputRows(a.metrics, "numOutputRows")
        case a: OverwriteByExpressionExecV1 =>
          setNumOutputRows(a.metrics, "numOutputRows")
        case a: V2TableWriteExec =>
          setNumOutputRows(a.metrics, "numOutputRows")
        case _ =>
      }
    } catch {
      case e: Throwable => error("Error updating query metrics", e)
    }
  }
}
