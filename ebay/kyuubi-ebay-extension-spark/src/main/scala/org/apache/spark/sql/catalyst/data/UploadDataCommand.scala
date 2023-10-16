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
package org.apache.spark.sql.catalyst.data

import java.io.FileInputStream
import java.util.Locale

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkException
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.command.{CommandUtils, RunnableCommand}
import org.apache.spark.sql.execution.datasources.CreateTempViewUsing
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.StoreAssignmentPolicy
import org.apache.spark.sql.kyuubi.SparkEbayUtils
import org.apache.spark.sql.types.{MetadataBuilder, NumericType, StringType}

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.config.{KyuubiEbayConf, KyuubiReservedKeys}
import org.apache.kyuubi.config.KyuubiEbayConf._

case class UploadDataCommand(
    table: TableIdentifier,
    path: String,
    isOverwrite: Boolean,
    partitionSpec: Option[TablePartitionSpec],
    optionSpec: Option[Map[String, String]]) extends RunnableCommand {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[LogicalPlan]): LogicalPlan = this
  private lazy val kyuubiConf = SparkEbayUtils.kyuubiConf

  override val output: Seq[Attribute] = Seq(
    AttributeReference(
      "table",
      StringType,
      nullable = false,
      new MetadataBuilder().putString("comment", "name of the table").build())(),
    AttributeReference(
      "path",
      StringType,
      nullable = false,
      new MetadataBuilder().putString("comment", "the remote path of file uploaded").build())())

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    val targetTable = catalog.getTableMetadata(table)
    val tableIdentwithDB = targetTable.identifier.quotedString

    if (targetTable.tableType == CatalogTableType.VIEW) {
      throw KyuubiSQLException("Target table in UPLOAD DATA " +
        s"cannot be a view: $tableIdentwithDB")
    }
    if (targetTable.tableType.name.toLowerCase(Locale.ROOT) == "TEMPORARY") {
      throw KyuubiSQLException("Target table in UPLOAD DATA cannot be a temporary table: " +
        s"$tableIdentwithDB")
    }

    if (!kyuubiConf.get(DATA_UPLOAD_DYNAMIC_PARTITION_ENABLED)) {
      if (targetTable.partitionColumnNames.nonEmpty) {
        if (partitionSpec.isEmpty) {
          throw KyuubiSQLException(s"UPLOAD DATA target table $tableIdentwithDB is " +
            s"partitioned, but no partition spec is provided")
        }
        if (targetTable.partitionColumnNames.size != partitionSpec.get.size) {
          throw KyuubiSQLException(s"UPLOAD DATA target table $tableIdentwithDB " +
            s"is partitioned, " +
            s"but number of columns in provided partition spec (${partitionSpec.get.size}) " +
            s"do not match number of partitioned columns in table " +
            s"(${targetTable.partitionColumnNames.size})")
        }
        partitionSpec.get.keys.foreach { colName =>
          if (!targetTable.partitionColumnNames.contains(colName)) {
            throw KyuubiSQLException(s"UPLOAD DATA target table $tableIdentwithDB is " +
              s"partitioned, but the specified partition spec refers to a column " +
              s"that is not partitioned: '$colName'")
          }
        }
      } else {
        if (partitionSpec.nonEmpty) {
          throw KyuubiSQLException(s"UPLOAD DATA target table $tableIdentwithDB is not " +
            s"partitioned, but a partition spec was provided.")
        }
      }
    }

    // Upload
    val location = upload(sparkSession, path, targetTable)

    // Refresh the metadata cache to ensure the data visible to the users
    catalog.refreshTable(targetTable.identifier)

    CommandUtils.updateTableStats(sparkSession, targetTable)
    Seq(Row(tableIdentwithDB, location.toUri.toString))
  }

  def upload(sparkSession: SparkSession, path: String, targetTable: CatalogTable): Path = {
    val hadoopConf = sparkSession.sessionState.newHadoopConf()

    // Check source file
    val sessionId =
      sparkSession.sparkContext.getLocalProperty(KyuubiEbayConf.KYUUBI_SESSION_ID_KEY)
    val sessionUser =
      sparkSession.sparkContext.getLocalProperty(KyuubiReservedKeys.KYUUBI_SESSION_USER_KEY)
    val sessionScratch =
      SparkEbayUtils.getSessionScratchDir(sparkSession, sessionUser, sessionId)
    val fileSystem = sessionScratch.getFileSystem(hadoopConf)

    val srcPath = new Path(sessionScratch + Path.SEPARATOR + path)
    if (!fileSystem.exists(srcPath)) {
      throw KyuubiSQLException(s"UPLOAD DATA input path does not exist: $srcPath")
    }

    val defaultFs = FileSystem.getDefaultUri(hadoopConf).getScheme
    val isDefaultLocal = defaultFs == null || defaultFs == "file"
    val isLocal = ((isDefaultLocal && srcPath.toUri.getScheme == null) ||
      srcPath.toUri.getScheme == "file")

    // Upload
    val location = performUpload(sparkSession, fileSystem, targetTable, srcPath, isLocal)
    logInfo(s"Uploaded data to table ${targetTable.identifier.quotedString}, " +
      s"path: $location with $sessionId")
    location
  }

  def performUpload(
      sparkSession: SparkSession,
      fileSystem: FileSystem,
      targetTable: CatalogTable,
      srcPath: Path,
      isLocal: Boolean): Path = {
    try {
      if (validateCSV(sparkSession, fileSystem, targetTable, srcPath, isLocal)) {
        performUploadCSV(sparkSession, targetTable, srcPath)
      } else {
        throw new SparkException(s"Not valid csv file detected: ${srcPath}. " +
          s"Tips: 1. Remove all blank rows " +
          s"2. Add ignoreTrailingWhiteSpace=true option " +
          s"3. Remove whitespace and other specific chars from header line")
      }
    } finally {
      fileSystem.delete(srcPath, false)
    }
  }

  def performUploadCSV(
      sparkSession: SparkSession,
      targetTable: CatalogTable,
      srcPath: Path): Path = {
    // Create temp view
    val tempView = s"${table.table}_${path.replaceAll("-", "")}"
    val createTempViewUsing = CreateTempViewUsing(
      new TableIdentifier(tempView),
      if (optionSpec.getOrElse(Map.empty).get("header").getOrElse("false") == "true") {
        None
      } else {
        Some(targetTable.schema)
      },
      true,
      false,
      "csv",
      optionSpec.getOrElse(Map.empty) + ("path" -> srcPath.toUri.getPath))
    new QueryExecution(sparkSession, createTempViewUsing).executedPlan.execute()

    val conf = sparkSession.sessionState.conf
    val catalog = sparkSession.sessionState.catalog
    val view = catalog.getTempViewOrPermanentTableMetadata(TableIdentifier(tempView))
    val overwrite = if (isOverwrite) "OVERWRITE TABLE" else "INTO";

    val fields =
      if (partitionSpec.isDefined) {
        // Static partition insert
        targetTable.schema.fields.map(t =>
          if (partitionSpec.getOrElse(Map.empty).contains(t.name)) {
            if (t.dataType.isInstanceOf[NumericType]) {
              s"${partitionSpec.getOrElse(Map.empty).get(t.name).get} as ${t.name}"
            } else {
              s"'${partitionSpec.getOrElse(Map.empty).get(t.name).get}' as ${t.name}"
            }
          } else {
            if (view.schema.fields.filter(_.name == t.name).length > 0) {
              t.name
            } else {
              s"null as ${t.name}"
            }
          }).mkString(", ")
      } else {
        // Dynamic partition insert
        if (kyuubiConf.get(DATA_UPLOAD_DYNAMIC_PARTITION_ENABLED)) {
          if (targetTable.partitionColumnNames.nonEmpty) {
            targetTable.partitionColumnNames.foreach { colName =>
              if (view.schema.fields.filter(_.name == colName).length == 0) {
                throw KyuubiSQLException(s"UPLOAD DATA target table " +
                  s"${targetTable.identifier.quotedString} is partitioned, " +
                  s"but no partition spec found in file refers to a column " +
                  s"that is not partitioned: '$colName'")
              }
            }
          }
        }

        targetTable.schema.fields.map(t =>
          if (view.schema.fields.filter(_.name.equalsIgnoreCase(t.name)).length > 0) {
            t.name
          } else {
            s"null as ${t.name}"
          }).mkString(", ")
      }

    val oldStoreAssignmentPolicy = conf.getConf(SQLConf.STORE_ASSIGNMENT_POLICY)
    try {
      conf.setConf(SQLConf.STORE_ASSIGNMENT_POLICY, StoreAssignmentPolicy.LEGACY.toString)

      sparkSession.sql(s"INSERT ${overwrite} ${targetTable.identifier.quotedString} " +
        s"SELECT ${fields} FROM ${tempView}")
    } finally {
      conf.setConf(SQLConf.STORE_ASSIGNMENT_POLICY, oldStoreAssignmentPolicy)
    }

    new Path(targetTable.storage.locationUri.get.getPath)
  }

  private def validateCSV(
      sparkSession: SparkSession,
      fileSystem: FileSystem,
      targetTable: CatalogTable,
      srcPath: Path,
      isLocal: Boolean): Boolean = {

    if (!validateCSVOptions) {
      throw new SparkException("Not valid csv options. Not allowed options: " +
        s"${kyuubiConf.get(DATA_UPLOAD_NOT_ALLOWED_CSV_OPTIONS)}}")
    }

    val inputStream =
      if (isLocal) {
        new FileInputStream(srcPath.toUri.getPath)
      } else {
        fileSystem.open(srcPath)
      }

    val sampleLines = new ArrayBuffer[String]()
    val inputReader = new java.io.InputStreamReader(inputStream)
    val bf = new java.io.BufferedReader(inputReader)
    try {
      var str = bf.readLine
      while (str != null && sampleLines.size < 10) {
        sampleLines.append(str)
        str = bf.readLine
      }
    } finally {
      bf.close
      inputReader.close
    }

    if (sampleLines.size == 0 ||
      (sampleLines.size == 1 &&
        optionSpec.getOrElse(Map.empty).get("header").getOrElse("false") == "true")) {
      return false
    }

    import sparkSession.implicits._
    val sampleDataSet = sparkSession.sparkContext.parallelize(sampleLines).toDS()

    val df = sparkSession.read
      .format("csv")
      .schema(if (optionSpec.getOrElse(Map.empty).get("header").getOrElse("false") == "true") {
        null
      } else {
        targetTable.schema
      })
      .options(if (optionSpec.nonEmpty) optionSpec.get
      else scala.collection.Map("header" -> "false", "delimiter" -> ","))
      .csv(sampleDataSet)

    val iterator = df.takeAsList(10).iterator()
    var result = iterator.hasNext
    while (iterator.hasNext) {
      val sampleRow = iterator.next()
      if (sampleRow == null || sampleRow.toSeq.forall(_ == null)) {
        result = false
      }
    }
    result
  }

  def validateCSVOptions: Boolean = {
    val notAllowedOptions = kyuubiConf.get(DATA_UPLOAD_NOT_ALLOWED_CSV_OPTIONS).split(",")
    optionSpec.isEmpty ||
    optionSpec.get.keys.map(_.toLowerCase(Locale.ROOT)).forall(
      !notAllowedOptions.contains(_))
  }
}
