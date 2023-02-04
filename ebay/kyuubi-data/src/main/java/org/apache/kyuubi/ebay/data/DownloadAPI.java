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

package org.apache.kyuubi.ebay.data;

import java.util.HashMap;
import java.util.Map;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.kyuubi.ebay.data.connection.ConnectionFactory;
import org.apache.kyuubi.ebay.data.connection.KyuubiConnectionFactory;
import org.apache.kyuubi.ebay.data.utils.CommandLineUtils;
import org.apache.kyuubi.jdbc.hive.KyuubiConnection;

public class DownloadAPI {
  public static void main(String[] args) throws Exception {
    // Parse command line
    Options options = CommandLineUtils.buildDownloadCommandLineOptions();
    CommandLine commandLine = CommandLineUtils.parse(args, options);
    if (commandLine.hasOption('h')) {
      CommandLineUtils.printDownloadHelp(options);
      System.exit(0);
    }

    if (commandLine.hasOption('v')) {
      Package aPackage = DownloadAPI.class.getPackage();
      System.out.println("Current version: " + aPackage.getImplementationVersion());
      System.exit(0);
    }

    // Prepare inputs
    String url = commandLine.getOptionValue('u', "jdbc:hive2://127.0.0.1:10009/default");
    String user = commandLine.getOptionValue('n', "");
    String password = ""; // #3. User Password
    if (commandLine.hasOption('p')) {
      password = commandLine.getOptionValue('p', "");
    } else if (commandLine.hasOption('w')) {
      password = CommandLineUtils.obtainPasswordFromFile(commandLine.getOptionValue('w', ""));
    }

    String table = commandLine.getOptionValue('t', null);
    String query = commandLine.getOptionValue('q', null);
    String format = commandLine.getOptionValue('f', null);
    String optionsInfo = commandLine.getOptionValue('O', null);
    String localFilePath = commandLine.getOptionValue('d');
    if (commandLine.hasOption('S')) {
      System.setProperty(KyuubiConnection.DATA_SILENT_MODE_PROPERTY, "true");
    }

    Map<String, String> optionMap = null;
    if (optionsInfo != null && !optionsInfo.trim().isEmpty()) {
      optionMap = new HashMap<>();
      for (String str : optionsInfo.split(",")) {
        String[] kvs = str.split("=", 2);
        if (kvs.length == 2) {
          if (kvs[0].equals("delimiter")) {
            optionMap.put(kvs[0], StringEscapeUtils.unescapeJava(kvs[1]));
          } else {
            optionMap.put(kvs[0], kvs[1]);
          }
        } else {
          System.out.println("Invalid options: " + str);
        }
      }
    }

    ConnectionFactory connectionFactory = new KyuubiConnectionFactory(url, user, password);
    KyuubiConnection conn = (KyuubiConnection) connectionFactory.getConnection();
    conn.downloadDataFromThriftServer(table, query, format, optionMap, localFilePath);
    conn.close();
  }
}
