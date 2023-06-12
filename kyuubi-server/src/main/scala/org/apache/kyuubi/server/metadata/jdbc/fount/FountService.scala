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

package org.apache.kyuubi.server.metadata.jdbc.fount

import com.ebay.fount.fountclient.{DecryptionDirective, FountClient}
import com.ebay.fount.managedfountclient.ManagedFountClientBuilder
import com.ebay.fount.managedfountclient.event.{FountDatasourceChangeEvent, FountDatasourceChangeListener}
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}

import org.apache.kyuubi.{Logging, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.server.metadata.jdbc.{JDBCMetadataStore, JDBCMetadataStoreConf}
import org.apache.kyuubi.server.metadata.jdbc.JDBCMetadataStoreConf.METADATA_STORE_JDBC_DRIVER

object FountService extends FountDatasourceChangeListener with Logging {
  private var conf: KyuubiConf = _
  private var jdbcMetadataStore: JDBCMetadataStore = _

  def initialize(conf: KyuubiConf, jdbcMetadataStore: JDBCMetadataStore): Unit = {
    this.conf = conf
    this.jdbcMetadataStore = jdbcMetadataStore
  }

  private lazy val fountClient = getFountClient()

  private def getFountClient(): FountClient = {
    new ManagedFountClientBuilder()
      .addFountDatasourceChangeListener(this)
      .dbEnv(conf.get(FountConf.FOUNT_ENV))
      .appName(conf.get(FountConf.FOUNT_APP))
      .decryptionDirective(DecryptionDirective.DECRYPT)
      .logicalDsNames(conf.get(FountConf.FOUNT_DATASOURCE))
      .build()
  }

  private[jdbc] def getHikariDataSource(): HikariDataSource = {
    val fountConfig = fountClient.getDatasourceConfig(conf.get(FountConf.FOUNT_DATASOURCE))
    val datasourceProperties = JDBCMetadataStoreConf.getMetadataStoreJDBCDataSourceProperties(conf)
    val hikariConfig = new HikariConfig(datasourceProperties)
    hikariConfig.setDriverClassName(
      conf.get(METADATA_STORE_JDBC_DRIVER).getOrElse("com.mysql.jdbc.Driver"))
    hikariConfig.setJdbcUrl(fountConfig.getUrl.replace("amp;", ""))
    hikariConfig.setUsername(fountConfig.getUser)
    hikariConfig.setPassword(fountConfig.getPassword)
    hikariConfig.setPoolName("fount-jdbc-metadata-store-pool")
    new HikariDataSource(hikariConfig)
  }

  override def datasourceChanged(event: FountDatasourceChangeEvent): Unit = {
    info(s"Receive fount data source change event: $event")
    try {
      val newHikariDataSource = getHikariDataSource()
      Utils.tryLogNonFatalError {
        Option(jdbcMetadataStore.getHikariDataSource).foreach(_.close())
      }
      jdbcMetadataStore.setHikariDataSource(newHikariDataSource)
    } catch {
      case e: Throwable => error("Error reloading data source", e)
    }
  }
}
