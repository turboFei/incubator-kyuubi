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

package org.apache.kyuubi.service.authentication

import java.sql.{Connection, PreparedStatement, Statement}
import java.util.Properties
import javax.security.sasl.AuthenticationException

import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import org.apache.commons.lang3.StringUtils

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._

class JdbcAuthenticationProviderImpl(conf: KyuubiConf) extends PasswdAuthenticationProvider
  with Logging {

  private val driverClass = conf.get(AUTHENTICATION_JDBC_DRIVER)
  private val jdbcUrl = conf.get(AUTHENTICATION_JDBC_URL)
  private val jdbcUsername = conf.get(AUTHENTICATION_JDBC_USERNAME)
  private val jdbcUserPassword = conf.get(AUTHENTICATION_JDBC_PASSWORD)
  private val authQuerySql = conf.get(AUTHENTICATION_JDBC_QUERY)

  private val SQL_PLACEHOLDER_REGEX = """\$\{.+?}""".r
  private val USERNAME_SQL_PLACEHOLDER = "${username}"
  private val PASSWORD_SQL_PLACEHOLDER = "${password}"

  checkJdbcConfigs()

  private[kyuubi] val hikariDataSource = getHikariDataSource

  /**
   * The authenticate method is called by the Kyuubi Server authentication layer
   * to authenticate users for their requests.
   * If a user is to be granted, return nothing/throw nothing.
   * When a user is to be disallowed, throw an appropriate [[AuthenticationException]].
   *
   * @param user     The username received over the connection request
   * @param password The password received over the connection request
   * @throws AuthenticationException When a user is found to be invalid by the implementation
   */
  @throws[AuthenticationException]
  override def authenticate(user: String, password: String): Unit = {
    if (StringUtils.isBlank(user)) {
      throw new AuthenticationException(s"Error validating, user is null" +
        s" or contains blank space")
    }

    if (StringUtils.isBlank(password)) {
      throw new AuthenticationException(s"Error validating, password is null" +
        s" or contains blank space")
    }

    var connection: Connection = null
    var queryStatement: PreparedStatement = null

    try {
      connection = hikariDataSource.getConnection

      queryStatement = getAndPrepareQueryStatement(connection, user, password)

      val resultSet = queryStatement.executeQuery()

      if (resultSet == null || !resultSet.next()) {
        // auth failed
        throw new AuthenticationException(s"Password does not match or no such user. user:" +
          s" $user , password length: ${password.length}")
      }

      // auth passed

    } catch {
      case e: AuthenticationException =>
        throw e
      case e: Exception =>
        error("Cannot get user info", e);
        throw e
    } finally {
      closeDbConnection(connection, queryStatement)
    }
  }

  private def checkJdbcConfigs(): Unit = {
    def configLog(config: String, value: String): String = s"JDBCAuthConfig: $config = '$value'"

    debug(configLog("Driver Class", driverClass.orNull))
    debug(configLog("JDBC URL", jdbcUrl.orNull))
    debug(configLog("Database username", jdbcUsername.orNull))
    debug(configLog("Database password length", jdbcUserPassword.getOrElse("").length.toString))
    debug(configLog("Query SQL", authQuerySql.orNull))

    // Check if JDBC parameters valid
    if (driverClass.isEmpty) {
      throw new IllegalArgumentException("JDBC driver class is not configured.")
    }

    if (jdbcUrl.isEmpty) {
      throw new IllegalArgumentException("JDBC url is not configured")
    }

    if (jdbcUsername.isEmpty || jdbcUserPassword.isEmpty) {
      throw new IllegalArgumentException("JDBC username or password is not configured")
    }

    // Check Query SQL
    if (authQuerySql.isEmpty) {
      throw new IllegalArgumentException("Query SQL is not configured")
    }
    val querySqlInLowerCase = authQuerySql.get.trim.toLowerCase
    if (!querySqlInLowerCase.startsWith("select")) { // allow select query sql only
      throw new IllegalArgumentException("Query SQL must start with \"SELECT\"");
    }
    if (!querySqlInLowerCase.contains("where")) {
      warn("Query SQL does not contains \"WHERE\" keyword");
    }
    if (!querySqlInLowerCase.contains("${username}")) {
      warn("Query SQL does not contains \"${username}\" placeholder");
    }
  }

  private def getPlaceholderList(sql: String): List[String] = {
    SQL_PLACEHOLDER_REGEX.findAllMatchIn(sql)
      .map(m => m.matched)
      .toList
  }

  private def getAndPrepareQueryStatement(
      connection: Connection,
      user: String,
      password: String): PreparedStatement = {

    val preparedSql: String = {
      SQL_PLACEHOLDER_REGEX.replaceAllIn(authQuerySql.get, "?")
    }
    debug(s"prepared auth query sql: $preparedSql")

    val stmt = connection.prepareStatement(preparedSql)
    stmt.setMaxRows(1) // minimum result size required for authentication

    // Extract placeholder list and fill parameters to placeholders
    val placeholderList: List[String] = getPlaceholderList(authQuerySql.get)
    for (i <- placeholderList.indices) {
      val param = placeholderList(i) match {
        case USERNAME_SQL_PLACEHOLDER => user
        case PASSWORD_SQL_PLACEHOLDER => password
        case otherPlaceholder =>
          throw new IllegalArgumentException(
            s"Unrecognized Placeholder In Query SQL: $otherPlaceholder")
      }

      stmt.setString(i + 1, param)
    }

    stmt
  }

  private def closeDbConnection(connection: Connection, statement: Statement): Unit = {
    if (statement != null && !statement.isClosed) {
      try {
        statement.close()
      } catch {
        case e: Exception =>
          error("Cannot close PreparedStatement to auth database ", e)
      }
    }

    if (connection != null && !connection.isClosed) {
      try {
        connection.close()
      } catch {
        case e: Exception =>
          error("Cannot close connection to auth database ", e)
      }
    }
  }

  private def getHikariDataSource: HikariDataSource = {
    val datasourceProperties = new Properties()
    val hikariConfig = new HikariConfig(datasourceProperties)
    hikariConfig.setDriverClassName(driverClass.orNull)
    hikariConfig.setJdbcUrl(jdbcUrl.orNull)
    hikariConfig.setUsername(jdbcUsername.orNull)
    hikariConfig.setPassword(jdbcUserPassword.orNull)
    hikariConfig.setPoolName("jdbc-auth-pool")

    new HikariDataSource(hikariConfig)
  }
}
