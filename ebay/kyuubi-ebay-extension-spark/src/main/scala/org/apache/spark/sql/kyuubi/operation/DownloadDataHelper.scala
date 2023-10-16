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

import java.util.{Map => JMap}

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.execution.{CollectLimitExec, LimitExec, ProjectExec, SortExec, SparkPlan, TakeOrderedAndProjectExec}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.kyuubi.SparkEbayUtils
import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, ByteType, CalendarIntervalType, DoubleType, FloatType, IntegerType, LongType, MapType, NullType, ShortType, StringType, StructType}

import org.apache.kyuubi.{KyuubiSQLException, Logging}

object DownloadDataHelper extends Logging {
  private def needToRepartition(sparkPlan: SparkPlan): Boolean = {
    sparkPlan match {
      case _: SortExec => false
      case _: TakeOrderedAndProjectExec => false
      case ProjectExec(_, _: SortExec) => false
      case _: ShuffleExchangeExec => false
      case ProjectExec(_, _: ShuffleExchangeExec) => false
      case _: CollectLimitExec => false
      case _: LimitExec => false
      case ap: AdaptiveSparkPlanExec => needToRepartition(ap.inputPlan)
      case _ => true
    }
  }

  def writeDataKeepDataType(
      spark: SparkSession,
      fs: FileSystem,
      tableName: String,
      query: String,
      format: String,
      path: Path,
      options: JMap[String, String],
      writeOptions: Map[String, String],
      dataDownloadMaxSize: Long): (DataFrame, String, Path) = {
    val numFiles = writeOptions.get("numFiles").map(_.toInt)

    val result = (Option(tableName), Option(query), Option(format), Option(options)) match {
      case (Some(t), None, _, _) =>
        spark.table(t)
      case (None, Some(q), _, _) =>
        spark.sql(q)
      case _ =>
        throw KyuubiSQLException(s"Invalid arguments: ($tableName, $query, $format, $options).")
    }

    val schemaStr = result.schema.map(_.name).mkString(writeOptions("delimiter"))
    needToRepartition(result.queryExecution.sparkPlan)
    val needRepartition = needToRepartition(result.queryExecution.sparkPlan)
    // Background: according to the official Hadoop FileSystem API spec,
    // rename op's destination path must have a parent that exists,
    // otherwise we may get unexpected result on the rename API.
    // When downloading dataset as parquet format, if we configure a
    // quota-free path and adopt FileOutputCommitter V1 algorithm, we will
    // get the "IOException: Failed to rename FileStatus".
    // Hence, the parent path should exist (see CARMEL-5150).
    if (!fs.exists(path) && !fs.mkdirs(path)) {
      warn(s"Failed to create parent download path ${path}")
    }
    val outputPath = new Path(path, "output")
    val outputFormat = Option(format).getOrElse("csv")

    val writePlan =
      if (!needRepartition) {
        result
      } else if (numFiles.nonEmpty) {
        result.repartition(numFiles.get)
      } else {
        result.repartition()
      }

    writePlan.write
      .options(writeOptions)
      .format(outputFormat)
      .mode(SaveMode.Overwrite)
      .save(outputPath.toString)

    val contentSummary = fs.getContentSummary(outputPath)
    val dataSize = contentSummary.getLength
    if (dataSize > dataDownloadMaxSize) {
      throw downloadDataSizeExceeded(dataSize, dataDownloadMaxSize)
    }

    (result, schemaStr, outputPath)
  }

  def writeData(
      spark: SparkSession,
      fs: FileSystem,
      tableName: String,
      query: String,
      format: String,
      path: Path,
      options: JMap[String, String],
      writeOptions: Map[String, String],
      dataDownloadMaxSize: Long): (DataFrame, String, Path) = {
    val result = (Option(tableName), Option(query), Option(format), Option(options)) match {
      case (Some(t), None, _, _) =>
        spark.table(t)
      case (None, Some(q), _, _) =>
        spark.sql(q)
      case _ =>
        throw KyuubiSQLException(s"Invalid arguments: ($tableName, $query, $format, $options).")
    }

    val schemaStr = result.schema.map(_.name).mkString(writeOptions("delimiter"))
    val isSortable = sortableForWriteData(result.queryExecution.sparkPlan)
    // Background: according to the official Hadoop FileSystem API spec,
    // rename op's destination path must have a parent that exists,
    // otherwise we may get unexpected result on the rename API.
    // When downloading dataset as parquet format, if we configure a
    // quota-free path and adopt FileOutputCommitter V1 algorithm, we will
    // get the "IOException: Failed to rename FileStatus".
    // Hence, the parent path should exist (see CARMEL-5150).
    if (!fs.exists(path) && !fs.mkdirs(path)) {
      warn(s"Failed to create parent download path ${path}")
    }
    val step1Path = new Path(path, "step1")
    val step2Path = new Path(path, "step2")
    val outputFormat = Option(format).getOrElse("csv")
    val (castCols, readSchema) =
      if (outputFormat.equalsIgnoreCase("csv")) {
        // Support duplicate columns
        val names = result.schema.map(_.name)
        val renameDuplicateNames =
          if (names.distinct.length != names.length) {
            val duplicateColumns = names.groupBy(identity).collect {
              case (x, ys) if ys.length > 1 => x
            }
            result.logicalPlan.output.zipWithIndex.map {
              case (col, index) if duplicateColumns.exists(_.equals(col.name)) =>
                col.withName(col.name + index)
              case (col, _) => col
            }
          } else {
            result.logicalPlan.output
          }
        // Support Complex types for csv file
        val output = renameDuplicateNames.map { col =>
          col.dataType match {
            case BinaryType => Column(col).cast(StringType).alias(col.name)
            case NullType => Column(col).cast(StringType).alias(col.name)
            case CalendarIntervalType => Column(col).cast(StringType).alias(col.name)
            case ArrayType(_, _) => Column(col).cast(StringType).alias(col.name)
            case MapType(_, _, _) => Column(col).cast(StringType).alias(col.name)
            case StructType(_) => Column(col).cast(StringType).alias(col.name)
            case _ => Column(col).alias(col.name)
          }
        }
        (
          output,
          StructType(StructType.fromAttributes(renameDuplicateNames)
            .map(_.copy(dataType = StringType))))
      } else if (outputFormat.equalsIgnoreCase("parquet")) {
        val output = result.logicalPlan.output.map { col =>
          col.dataType match {
            case BooleanType | ByteType | ShortType | IntegerType
                | LongType | FloatType | DoubleType | BinaryType => Column(col).alias(col.name)
            case _ => Column(col).cast(StringType).alias(col.name)
          }
        }
        val newSchema = result.schema.map(s =>
          s.dataType match {
            case BooleanType | ByteType | ShortType | IntegerType
                | LongType | FloatType | DoubleType | BinaryType => s
            case _ => s.copy(dataType = StringType)
          })
        (output, StructType(newSchema))
      } else {
        val output = result.logicalPlan.output.map(col => Column(col).alias(col.name))
        (output, result.schema)
      }

    result.select(castCols: _*).write
      .options(writeOptions)
      .option("header", "false")
      .format(outputFormat)
      .mode(SaveMode.Overwrite)
      .save(step1Path.toString)
    val contentSummary = fs.getContentSummary(step1Path)
    val dataSize = contentSummary.getLength
    val fileCount = contentSummary.getFileCount
    if (dataSize > dataDownloadMaxSize) {
      throw downloadDataSizeExceeded(dataSize, dataDownloadMaxSize)
    }

    val coalesceNum = writeOptions.get("numFiles").map(_.toInt) match {
      case Some(num) =>
        if (num == 0) {
          num
        } else {
          if (dataSize / num > 10L * 1024 * 1024 * 1024) {
            throw downloadSingleDataSizeExceeded(num, dataSize)
          }
          num
        }
      case _ if dataSize / fileCount > writeOptions("minFileSize").toLong =>
        -1
      case _ =>
        math.max(dataSize / (100L * 1024 * 1024), 1).toInt
    }

    if (!isSortable && coalesceNum > 0) {
      spark.read
        .schema(readSchema)
        .format(outputFormat)
        .options(writeOptions)
        .option("header", "false")
        .load(step1Path.toString)
        .coalesce(coalesceNum)
        .write
        .options(writeOptions)
        .format(outputFormat)
        .mode(SaveMode.Overwrite)
        .save(step2Path.toString)

      (result, schemaStr, step2Path)
    } else {
      (result, schemaStr, step1Path)
    }
  }

  /**
   * Refer carmel org.apache.spark.sql.hive.thriftserver.SparkDownloadDataOperation
   */
  private[kyuubi] def sortableForWriteData(sparkPlan: SparkPlan): Boolean = sparkPlan match {
    case _: SortExec => true
    case _: TakeOrderedAndProjectExec => true
    case ProjectExec(_, _: SortExec) => true
    case ap: AdaptiveSparkPlanExec => sortableForWriteData(ap.inputPlan)
    case _ => false
  }

  def downloadDataSizeExceeded(dataSize: Long, maxSize: Long): KyuubiSQLException = {
    KyuubiSQLException(
      s"Too much download data requested: " +
        s"${SparkEbayUtils.bytesToString(dataSize)}, " +
        s"which exceeds ${SparkEbayUtils.bytesToString(maxSize)}",
      vendorCode = 500001)
  }

  def downloadSingleDataSizeExceeded(num: Int, dataSize: Long): KyuubiSQLException = {
    KyuubiSQLException(
      s"The numFiles($num) too small, please try to increase the " +
        s"number of downloaded files for ${dataSize / (9L * 1024 * 1024 * 1024)}.",
      vendorCode = 500002)
  }
}
