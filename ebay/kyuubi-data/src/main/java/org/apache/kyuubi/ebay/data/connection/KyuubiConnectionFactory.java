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

package org.apache.kyuubi.ebay.data.connection;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import jline.console.ConsoleReader;
import org.apache.commons.lang3.StringUtils;
import org.apache.kyuubi.jdbc.hive.JdbcConnectionParams;
import org.apache.kyuubi.jdbc.hive.KyuubiConnection;
import org.apache.kyuubi.jdbc.hive.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KyuubiConnectionFactory implements ConnectionFactory {
  private static final Logger LOG = LoggerFactory.getLogger(KyuubiConnectionFactory.class);

  private String url;
  private String user;
  private String password;

  public KyuubiConnectionFactory(String url, String user, String password) {
    this.url = url;
    this.user = user;
    this.password = password;
  }

  @Override
  public Connection getConnection() throws SQLException, IOException {
    Properties properties = new Properties();
    // set the operation language to SQL in case it is not in engine side
    properties.put("hivevar:kyuubi.operation.language", "SQL");
    LOG.info("Connecting to " + url);
    if (Utils.parsePropertyFromUrl(url, JdbcConnectionParams.AUTH_PRINCIPAL) == null) {
      String urlForPrompt = url.substring(0, url.contains(";") ? url.indexOf(';') : url.length());
      if (StringUtils.isBlank(user) || StringUtils.isBlank(password)) {
        ConsoleReader consoleReader = new ConsoleReader();
        if (StringUtils.isBlank(user)) {
          user = consoleReader.readLine("Enter username for " + urlForPrompt + ": ");
        }
        if (StringUtils.isBlank(password)) {
          password =
              consoleReader.readLine(
                  "Enter password for " + urlForPrompt + ": ", new Character('*'));
        }
      }
      properties.put(JdbcConnectionParams.AUTH_USER, user);
      properties.put(JdbcConnectionParams.AUTH_PASSWD, password);
    }
    KyuubiConnection connection = new KyuubiConnection(url, properties);
    LOG.info("Success get jdbc connection: " + connection.getEngineUrl());
    connection.markLaunchEngineOpCompleted();
    return connection;
  }

  @Override
  public void stop() {}
}
