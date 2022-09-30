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

package org.apache.kyuubi.tpcds.benchmark

import java.net.InetAddress

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

case class RunConfig(
    db: String = null,
    benchmarkName: String = "tpcds-v2.4-benchmark",
    filter: Option[String] = None,
    iterations: Int = 3,
    breakdown: Boolean = false,
    resultsDir: String = "/spark/sql/performance",
    queries: Set[String] = Set.empty)

// scalastyle:off
/**
 * Usage:
 * <p>
 * Run following command to benchmark TPC-DS sf10 with exists database `tpcds_sf10`.
 * {{{
 *   $SPARK_HOME/bin/spark-submit \
 *      --class org.apache.kyuubi.tpcds.benchmark.RunBenchmark \
 *      kyuubi-tpcds_*.jar --db tpcds_sf10
 * }}}
 */
object RunBenchmark {
  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[RunConfig]("tpcds-benchmark") {
      head("tpcds-benchmark", "")
      opt[String]('d', "db")
        .action { (x, c) => c.copy(db = x) }
        .text("the test data location")
        .required()
      opt[String]('b', "benchmark")
        .action { (x, c) => c.copy(benchmarkName = x) }
        .text("the name of the benchmark to run")
      opt[String]('f', "filter")
        .action((x, c) => c.copy(filter = Some(x)))
        .text("a filter on the name of the queries to run")
      opt[Boolean]('B', "breakdown")
        .action((x, c) => c.copy(breakdown = x))
        .text("whether to record breakdown results of an execution")
      opt[Int]('i', "iterations")
        .action((x, c) => c.copy(iterations = x))
        .text("the number of iterations to run")
      opt[String]('r', "results-dir")
        .action((x, c) => c.copy(resultsDir = x))
        .text("dir to store benchmark results, e.g. hdfs://hdfs-nn:9870/pref")
      opt[String]('q', "queries")
        .action { case (x, c) =>
          c.copy(queries = x.split(",").map(_.trim).filter(_.nonEmpty).toSet)
        }
        .text("name of the queries to run, use , split multiple name")
      help("help")
        .text("prints this usage text")
    }

    parser.parse(args, RunConfig()) match {
      case Some(config) => run(config)
      case None => sys.exit(1)
    }
  }

  def run(config: RunConfig): Unit = {
    val conf = new SparkConf()
      .setAppName(config.benchmarkName)

    val sparkSession = SparkSession.builder.config(conf).enableHiveSupport().getOrCreate()
    import sparkSession.implicits._

    sparkSession.conf.set("spark.sql.perf.results", config.resultsDir)

    val benchmark = new TPCDS(sparkSession)

    println("== USING DATABASES ==")
    println(config.db)
    sparkSession.sql(s"use ${config.db}")

    val allQueries = config.filter.map { f =>
      benchmark.tpcds2_4Queries.filter(_.name contains f)
    } getOrElse {
      benchmark.tpcds2_4Queries
    }

    val runQueries =
      if (config.queries.nonEmpty) {
        allQueries.filter(q => config.queries.contains(q.name.split('-')(0)))
      } else {
        allQueries
      }

    println("== QUERY LIST ==")
    runQueries.foreach(q => println(q.name))

    val experiment = benchmark.runExperiment(
      executionsToRun = runQueries,
      includeBreakdown = config.breakdown,
      iterations = config.iterations,
      tags = Map("host" -> InetAddress.getLocalHost.getHostName))

    println("== STARTING EXPERIMENT ==")
    experiment.waitForFinish(1000 * 60 * 30)

    sparkSession.conf.set("spark.sql.shuffle.partitions", "1")

    val toShow = experiment.getCurrentRuns()
      .withColumn("result", explode($"results"))
      .select("result.*")
      .groupBy("name")
      .agg(
        min($"executionTime") as 'minTimeMs,
        max($"executionTime") as 'maxTimeMs,
        avg($"executionTime") as 'avgTimeMs,
        stddev($"executionTime") as 'stdDev,
        (stddev($"executionTime") / avg($"executionTime") * 100) as 'stdDevPercent)
      .orderBy("name")

    println("Showing at most 1000 query results now")
    toShow.show(1000, false)

    // print benchmark result as csv format
    var index = 0
    toShow.collect().foreach { row =>
      if (index == 0) {
        println()
        // print head
        println(toShow.schema.fields.map(_.name).mkString(", "))
      }
      println(row.toSeq.mkString(", "))
      index = 1
    }

    println()
    println(s"""Results: spark.read.json("${experiment.resultPath}")""")
  }
}
// scalastyle:on
