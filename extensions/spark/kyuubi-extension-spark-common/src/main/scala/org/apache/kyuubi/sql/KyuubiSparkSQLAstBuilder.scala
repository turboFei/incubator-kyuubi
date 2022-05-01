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

package org.apache.kyuubi.sql

import java.time.LocalDate
import java.util.Locale

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.control.NonFatal

import org.antlr.v4.runtime.ParserRuleContext
import org.antlr.v4.runtime.tree.{ParseTree, TerminalNode}
import org.apache.commons.codec.binary.Hex
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.parser.ParserUtils.{string, stringWithoutUnescape, withOrigin}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project, Sort}
import org.apache.spark.sql.catalyst.util.DateTimeUtils.{getZoneId, localDateToDays, stringToTimestamp}
import org.apache.spark.sql.catalyst.util.IntervalUtils
import org.apache.spark.sql.hive.HiveAnalysis.conf
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import org.apache.kyuubi.sql.KyuubiSparkSQLParser._
import org.apache.kyuubi.sql.zorder.{OptimizeZorderStatement, OptimizeZorderStatementBase, Zorder, ZorderBase}

abstract class KyuubiSparkSQLAstBuilderBase extends KyuubiSparkSQLBaseVisitor[AnyRef] {
  def buildZorder(child: Seq[Expression]): ZorderBase
  def buildOptimizeZorderStatement(
      tableIdentifier: Seq[String],
      query: LogicalPlan): OptimizeZorderStatementBase

  /**
   * Create an expression from the given context. This method just passes the context on to the
   * visitor and only takes care of typing (We assume that the visitor returns an Expression here).
   */
  protected def expression(ctx: ParserRuleContext): Expression = typedVisit(ctx)

  protected def multiPart(ctx: ParserRuleContext): Seq[String] = typedVisit(ctx)

  override def visitSingleStatement(ctx: SingleStatementContext): LogicalPlan = {
    visit(ctx.statement()).asInstanceOf[LogicalPlan]
  }

  override def visitOptimizeZorder(
      ctx: OptimizeZorderContext): LogicalPlan = withOrigin(ctx) {
    val tableIdent = multiPart(ctx.multipartIdentifier())
    val table = UnresolvedRelation(tableIdent)

    val whereClause =
      if (ctx.whereClause() == null) {
        None
      } else {
        Option(expression(ctx.whereClause().booleanExpression()))
      }

    val tableWithFilter = whereClause match {
      case Some(expr) => Filter(expr, table)
      case None => table
    }

    val zorderCols = ctx.zorderClause().order.asScala
      .map(visitMultipartIdentifier)
      .map(UnresolvedAttribute(_))
      .toSeq

    val orderExpr =
      if (zorderCols.length == 1) {
        zorderCols.head
      } else {
        buildZorder(zorderCols)
      }
    val query =
      Sort(
        SortOrder(orderExpr, Ascending, NullsLast, Seq.empty) :: Nil,
        conf.getConf(KyuubiSQLConf.ZORDER_GLOBAL_SORT_ENABLED),
        Project(Seq(UnresolvedStar(None)), tableWithFilter))

    buildOptimizeZorderStatement(tableIdent, query)
  }

  override def visitPassThrough(ctx: PassThroughContext): LogicalPlan = null

  override def visitQuery(ctx: QueryContext): Expression = withOrigin(ctx) {
    val left = new UnresolvedAttribute(multiPart(ctx.multipartIdentifier()))
    val right = expression(ctx.constant())
    val operator = ctx.comparisonOperator().getChild(0).asInstanceOf[TerminalNode]
    operator.getSymbol.getType match {
      case KyuubiSparkSQLParser.EQ =>
        EqualTo(left, right)
      case KyuubiSparkSQLParser.NSEQ =>
        EqualNullSafe(left, right)
      case KyuubiSparkSQLParser.NEQ | KyuubiSparkSQLParser.NEQJ =>
        Not(EqualTo(left, right))
      case KyuubiSparkSQLParser.LT =>
        LessThan(left, right)
      case KyuubiSparkSQLParser.LTE =>
        LessThanOrEqual(left, right)
      case KyuubiSparkSQLParser.GT =>
        GreaterThan(left, right)
      case KyuubiSparkSQLParser.GTE =>
        GreaterThanOrEqual(left, right)
    }
  }

  override def visitLogicalBinary(ctx: LogicalBinaryContext): Expression = withOrigin(ctx) {
    val expressionType = ctx.operator.getType
    val expressionCombiner = expressionType match {
      case KyuubiSparkSQLParser.AND => And.apply _
      case KyuubiSparkSQLParser.OR => Or.apply _
    }

    // Collect all similar left hand contexts.
    val contexts = ArrayBuffer(ctx.right)
    var current = ctx.left
    def collectContexts: Boolean = current match {
      case lbc: LogicalBinaryContext if lbc.operator.getType == expressionType =>
        contexts += lbc.right
        current = lbc.left
        true
      case _ =>
        contexts += current
        false
    }
    while (collectContexts) {
      // No body - all updates take place in the collectContexts.
    }

    // Reverse the contexts to have them in the same sequence as in the SQL statement & turn them
    // into expressions.
    val expressions = contexts.reverseMap(expression)

    // Create a balanced tree.
    def reduceToExpressionTree(low: Int, high: Int): Expression = high - low match {
      case 0 =>
        expressions(low)
      case 1 =>
        expressionCombiner(expressions(low), expressions(high))
      case x =>
        val mid = low + x / 2
        expressionCombiner(
          reduceToExpressionTree(low, mid),
          reduceToExpressionTree(mid + 1, high))
    }
    reduceToExpressionTree(0, expressions.size - 1)
  }

  override def visitMultipartIdentifier(ctx: MultipartIdentifierContext): Seq[String] =
    withOrigin(ctx) {
      ctx.parts.asScala.map(_.getText)
    }

  override def visitZorderClause(ctx: ZorderClauseContext): Seq[UnresolvedAttribute] =
    withOrigin(ctx) {
      val res = ListBuffer[UnresolvedAttribute]()
      ctx.multipartIdentifier().forEach { identifier =>
        res += UnresolvedAttribute(identifier.parts.asScala.map(_.getText))
      }
      res
    }

  /**
   * Create a NULL literal expression.
   */
  override def visitNullLiteral(ctx: NullLiteralContext): Literal = withOrigin(ctx) {
    Literal(null)
  }

  /**
   * Create a Boolean literal expression.
   */
  override def visitBooleanLiteral(ctx: BooleanLiteralContext): Literal = withOrigin(ctx) {
    if (ctx.getText.toBoolean) {
      Literal.TrueLiteral
    } else {
      Literal.FalseLiteral
    }
  }

  /**
   * Create a typed Literal expression. A typed literal has the following SQL syntax:
   * {{{
   *   [TYPE] '[VALUE]'
   * }}}
   * Currently Date, Timestamp, Interval and Binary typed literals are supported.
   */
  override def visitTypeConstructor(ctx: TypeConstructorContext): Literal = withOrigin(ctx) {
    val value = string(ctx.STRING)
    val valueType = ctx.identifier.getText.toUpperCase(Locale.ROOT)

    def toLiteral[T](f: UTF8String => Option[T], t: DataType): Literal = {
      f(UTF8String.fromString(value)).map(Literal(_, t)).getOrElse {
        throw new ParseException(s"Cannot parse the $valueType value: $value", ctx)
      }
    }
    try {
      valueType match {
        case "DATE" =>
          toLiteral(stringToDate, DateType)
        case "TIMESTAMP" =>
          val zoneId = getZoneId(SQLConf.get.sessionLocalTimeZone)
          toLiteral(stringToTimestamp(_, zoneId), TimestampType)
        case "INTERVAL" =>
          val interval =
            try {
              IntervalUtils.stringToInterval(UTF8String.fromString(value))
            } catch {
              case e: IllegalArgumentException =>
                val ex = new ParseException("Cannot parse the INTERVAL value: " + value, ctx)
                ex.setStackTrace(e.getStackTrace)
                throw ex
            }
          Literal(interval, CalendarIntervalType)
        case "X" =>
          val padding = if (value.length % 2 != 0) "0" else ""

          Literal(Hex.decodeHex(padding + value))
        case other =>
          throw new ParseException(s"Literals of type '$other' are currently not supported.", ctx)
      }
    } catch {
      case e: IllegalArgumentException =>
        val message = Option(e.getMessage).getOrElse(s"Exception parsing $valueType")
        throw new ParseException(message, ctx)
    }
  }

  /**
   * Create a String literal expression.
   */
  override def visitStringLiteral(ctx: StringLiteralContext): Literal = withOrigin(ctx) {
    Literal(createString(ctx))
  }

  /**
   * Create a decimal literal for a regular decimal number.
   */
  override def visitDecimalLiteral(ctx: DecimalLiteralContext): Literal = withOrigin(ctx) {
    Literal(BigDecimal(ctx.getText).underlying())
  }

  /** Create a numeric literal expression. */
  private def numericLiteral(
      ctx: NumberContext,
      rawStrippedQualifier: String,
      minValue: BigDecimal,
      maxValue: BigDecimal,
      typeName: String)(converter: String => Any): Literal = withOrigin(ctx) {
    try {
      val rawBigDecimal = BigDecimal(rawStrippedQualifier)
      if (rawBigDecimal < minValue || rawBigDecimal > maxValue) {
        throw new ParseException(
          s"Numeric literal ${rawStrippedQualifier} does not " +
            s"fit in range [${minValue}, ${maxValue}] for type ${typeName}",
          ctx)
      }
      Literal(converter(rawStrippedQualifier))
    } catch {
      case e: NumberFormatException =>
        throw new ParseException(e.getMessage, ctx)
    }
  }

  /**
   * Create a Byte Literal expression.
   */
  override def visitTinyIntLiteral(ctx: TinyIntLiteralContext): Literal = {
    val rawStrippedQualifier = ctx.getText.substring(0, ctx.getText.length - 1)
    numericLiteral(
      ctx,
      rawStrippedQualifier,
      Byte.MinValue,
      Byte.MaxValue,
      ByteType.simpleString)(_.toByte)
  }

  /**
   * Create an integral literal expression. The code selects the most narrow integral type
   * possible, either a BigDecimal, a Long or an Integer is returned.
   */
  override def visitIntegerLiteral(ctx: IntegerLiteralContext): Literal = withOrigin(ctx) {
    BigDecimal(ctx.getText) match {
      case v if v.isValidInt =>
        Literal(v.intValue)
      case v if v.isValidLong =>
        Literal(v.longValue)
      case v => Literal(v.underlying())
    }
  }

  /**
   * Create a Short Literal expression.
   */
  override def visitSmallIntLiteral(ctx: SmallIntLiteralContext): Literal = {
    val rawStrippedQualifier = ctx.getText.substring(0, ctx.getText.length - 1)
    numericLiteral(
      ctx,
      rawStrippedQualifier,
      Short.MinValue,
      Short.MaxValue,
      ShortType.simpleString)(_.toShort)
  }

  /**
   * Create a Long Literal expression.
   */
  override def visitBigIntLiteral(ctx: BigIntLiteralContext): Literal = {
    val rawStrippedQualifier = ctx.getText.substring(0, ctx.getText.length - 1)
    numericLiteral(
      ctx,
      rawStrippedQualifier,
      Long.MinValue,
      Long.MaxValue,
      LongType.simpleString)(_.toLong)
  }

  /**
   * Create a Double Literal expression.
   */
  override def visitDoubleLiteral(ctx: DoubleLiteralContext): Literal = {
    val rawStrippedQualifier = ctx.getText.substring(0, ctx.getText.length - 1)
    numericLiteral(
      ctx,
      rawStrippedQualifier,
      Double.MinValue,
      Double.MaxValue,
      DoubleType.simpleString)(_.toDouble)
  }

  /**
   * Create a BigDecimal Literal expression.
   */
  override def visitBigDecimalLiteral(ctx: BigDecimalLiteralContext): Literal = {
    val raw = ctx.getText.substring(0, ctx.getText.length - 2)
    try {
      Literal(BigDecimal(raw).underlying())
    } catch {
      case e: AnalysisException =>
        throw new ParseException(e.message, ctx)
    }
  }

  /**
   * Create a String from a string literal context. This supports multiple consecutive string
   * literals, these are concatenated, for example this expression "'hello' 'world'" will be
   * converted into "helloworld".
   *
   * Special characters can be escaped by using Hive/C-style escaping.
   */
  private def createString(ctx: StringLiteralContext): String = {
    if (conf.escapedStringLiterals) {
      ctx.STRING().asScala.map(stringWithoutUnescape).mkString
    } else {
      ctx.STRING().asScala.map(string).mkString
    }
  }

  private def typedVisit[T](ctx: ParseTree): T = {
    ctx.accept(this).asInstanceOf[T]
  }

  private def stringToDate(s: UTF8String): Option[Int] = {
    def isValidDigits(segment: Int, digits: Int): Boolean = {
      // An integer is able to represent a date within [+-]5 million years.
      var maxDigitsYear = 7
      (segment == 0 && digits >= 4 && digits <= maxDigitsYear) ||
      (segment != 0 && digits > 0 && digits <= 2)
    }
    if (s == null || s.trimAll().numBytes() == 0) {
      return None
    }
    val segments: Array[Int] = Array[Int](1, 1, 1)
    var sign = 1
    var i = 0
    var currentSegmentValue = 0
    var currentSegmentDigits = 0
    val bytes = s.trimAll().getBytes
    var j = 0
    if (bytes(j) == '-' || bytes(j) == '+') {
      sign = if (bytes(j) == '-') -1 else 1
      j += 1
    }
    while (j < bytes.length && (i < 3 && !(bytes(j) == ' ' || bytes(j) == 'T'))) {
      val b = bytes(j)
      if (i < 2 && b == '-') {
        if (!isValidDigits(i, currentSegmentDigits)) {
          return None
        }
        segments(i) = currentSegmentValue
        currentSegmentValue = 0
        currentSegmentDigits = 0
        i += 1
      } else {
        val parsedValue = b - '0'.toByte
        if (parsedValue < 0 || parsedValue > 9) {
          return None
        } else {
          currentSegmentValue = currentSegmentValue * 10 + parsedValue
          currentSegmentDigits += 1
        }
      }
      j += 1
    }
    if (!isValidDigits(i, currentSegmentDigits)) {
      return None
    }
    if (i < 2 && j < bytes.length) {
      // For the `yyyy` and `yyyy-[m]m` formats, entire input must be consumed.
      return None
    }
    segments(i) = currentSegmentValue
    try {
      val localDate = LocalDate.of(sign * segments(0), segments(1), segments(2))
      Some(localDateToDays(localDate))
    } catch {
      case NonFatal(_) => None
    }
  }
}

class KyuubiSparkSQLAstBuilder extends KyuubiSparkSQLAstBuilderBase {
  override def buildZorder(child: Seq[Expression]): ZorderBase = {
    Zorder(child)
  }

  override def buildOptimizeZorderStatement(
      tableIdentifier: Seq[String],
      query: LogicalPlan): OptimizeZorderStatementBase = {
    OptimizeZorderStatement(tableIdentifier, query)
  }
}
