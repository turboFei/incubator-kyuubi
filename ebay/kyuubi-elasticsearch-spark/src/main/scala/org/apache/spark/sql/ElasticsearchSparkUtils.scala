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

package org.apache.spark.sql

import java.io.File
import java.util.Properties

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf, SparkFiles}

import org.apache.kyuubi.{Logging, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.elasticsearch.ElasticsearchSparkConf._

object ElasticsearchSparkUtils extends Logging {
  @volatile
  private var _esDefaultPropertiesLoaded: Boolean = false
  @volatile
  private var _esPropertiesFileLoaded: Boolean = false

  private def elasticsearchEnabled(conf: KyuubiConf, spark: SparkSession): Boolean = {
    spark.conf.getOption(ELASTICSEARCH_ENABLED.key) match {
      case Some(v) => v.equalsIgnoreCase(true.toString)
      case None => conf.get(ELASTICSEARCH_ENABLED)
    }
  }

  def elasticsearchInitSql(conf: KyuubiConf, spark: SparkSession): Seq[String] =
    if (elasticsearchEnabled(conf, spark)) {
      spark.conf.getOption(ELASTICSEARCH_INITIALIZE_SQL.key) match {
        case Some(v) => v.split(";")
        case None => conf.get(ELASTICSEARCH_INITIALIZE_SQL)
      }
    } else {
      Seq.empty
    }

  def initElasticsearchSpark(conf: KyuubiConf, spark: SparkSession): Unit =
    Utils.tryLogNonFatalError {
      if (elasticsearchEnabled(conf, spark)) {
        info("Elasticsearch spark is enabled")

        val sparkConf = spark.sparkContext.conf

        spark.conf.getOption(ELASTICSEARCH_VIP.key)
          .orElse(conf.get(ELASTICSEARCH_VIP)).foreach { vip =>
            sparkConf.setIfMissing("spark.es.nodes", vip)
          }

        val port = spark.conf.getOption(ELASTICSEARCH_PORT.key)
          .getOrElse(conf.get(ELASTICSEARCH_PORT).toString)
        sparkConf.setIfMissing("spark.es.port", port)

        val ssl = spark.conf.getOption(ELASTICSEARCH_SSL.key)
          .getOrElse(conf.get(ELASTICSEARCH_SSL).toString)
        sparkConf.setIfMissing("spark.es.net.ssl", ssl)

        spark.conf.getOption(ELASTICSEARCH_AUTH_USERNAME.key)
          .orElse(conf.get(ELASTICSEARCH_AUTH_USERNAME)).foreach { userName =>
            sparkConf.setIfMissing("spark.es.net.http.auth.user", userName)
          }

        spark.conf.getOption(ELASTICSEARCH_AUTH_PASSWORD.key)
          .orElse(conf.get(ELASTICSEARCH_AUTH_PASSWORD)).foreach { password =>
            sparkConf.setIfMissing("spark.es.net.http.auth.pass", password)
          }

        if (!_esDefaultPropertiesLoaded) {
          _esDefaultPropertiesLoaded = true
          loadDefaultsElasticsearchProperties(sparkConf)
        }

        spark.conf.getOption(ELASTICSEARCH_PROPERTIES_FILE.key).orElse(conf.get(
          ELASTICSEARCH_PROPERTIES_FILE)) match {
          case Some(file) =>
            if (!_esPropertiesFileLoaded) {
              _esPropertiesFileLoaded = true
              try {
                spark.sparkContext.addFile(file)
                val propertiesFile = new File(SparkFiles.get(new Path(file).getName))

                Utils.getPropertiesFromFile(Some(propertiesFile)).foreach { case (key, value) =>
                  sparkConf.set(key, value)
                }
                info(s"Loaded elasticsearch properties from file $file")
              } catch {
                case e: Throwable =>
                  error(s"Error loading elasticsearch properties from file $file", e)
              }
            }

          case None =>
        }
      }
    }

  private def loadDefaultsElasticsearchProperties(sparkConf: SparkConf): Unit = {
    val path = "es-defaults.properties"
    try {
      val prop = new Properties()
      prop.load(getClass.getClassLoader.getResourceAsStream(path))
      prop.stringPropertyNames().asScala.foreach { key =>
        val finalKey = s"spark.$key"
        val value = prop.getProperty(key).trim
        info(s"set if missing $finalKey -> $value")
        sparkConf.setIfMissing(finalKey, value)
      }
    } catch {
      case e: Throwable => warn(s"Error loading defaults elasticsearch properties[$path]", e)
    }
  }
}
