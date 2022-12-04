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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectionFactoryHelper {
  private static final Logger logger = LoggerFactory.getLogger(ConnectionFactoryHelper.class);

  public static Boolean tryWithConnectionFactory(
      ConnectionFactory factory, Callable<Boolean> runnable) throws Exception {
    try {
      return runnable.call();
    } finally {
      factory.stop();
    }
  }

  public static <T> T tryWithConnection(
      ConnectionFactory factory, ConnectionOperation<T> connectionOperation) throws Exception {
    Connection conn = null;
    try {
      conn = factory.getConnection();
      return connectionOperation.execute(conn);
    } finally {
      if (conn != null) {
        try {
          conn.close();
        } catch (SQLException e) {
          logger.error("Failed to close conn", e);
        }
      }
    }
  }
}
