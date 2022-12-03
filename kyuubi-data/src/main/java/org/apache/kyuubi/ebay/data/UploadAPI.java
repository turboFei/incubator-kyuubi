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
import java.util.concurrent.Callable;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.kyuubi.ebay.data.connection.ConnectionFactory;
import org.apache.kyuubi.ebay.data.connection.ConnectionFactoryHelper;
import org.apache.kyuubi.ebay.data.connection.KyuubiConnectionFactory;
import org.apache.kyuubi.ebay.data.utils.CommandLineUtils;
import org.apache.kyuubi.ebay.data.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UploadAPI {
  private static Logger logger = LoggerFactory.getLogger(UploadAPI.class);
  private static Options options;
  private static CommandLine commandLine;
  private static Map<String, String> configuredOptions = new HashMap<>();

  public static void main(String[] args) throws Exception {
    // Parse command line
    options = CommandLineUtils.buildUploadCommandLineOptions();
    commandLine = CommandLineUtils.parse(args, options);
    if (commandLine.hasOption('h')) {
      CommandLineUtils.printUploadHelp(options);
      System.exit(0);
    }

    if (commandLine.hasOption('v')) {
      Package aPackage = UploadAPI.class.getPackage();
      System.out.println("Current version: " + aPackage.getImplementationVersion());
      System.exit(0);
    }

    // Prepare inputs
    String config = getOptionValue("conf", ""); // #0. Config file
    if (StringUtils.isNotBlank(config)) {
      Utils.parseDataFromFile(
          config,
          new Utils.LineHandler() {
            @Override
            public void handle(String line) {
              if (StringUtils.isBlank(line)) {
                return;
              }
              line = line.trim();

              String[] arr = StringUtils.split(line, "= ");
              if (arr.length <= 1) {
                return;
              }
              String name = arr[0].trim();
              String value = arr[1].trim();
              configuredOptions.put(name, value);
            }
          });
    }

    String url = getOptionValue('u', "jdbc:hive2://127.0.0.1:10009/default"); // #1. JDBC Url
    String user = getOptionValue('n', ""); // #2. User Name
    String password = ""; // #3. User Password
    if (hasOption('p')) {
      password = getOptionValue('p', "");
    } else if (hasOption('w')) {
      password = CommandLineUtils.obtainPasswordFromFile(getOptionValue('w', ""));
    }

    String targetTable = getOptionValue('t', ""); // #4. Target Workspace Table
    String partitionsInfo = getOptionValue('P', ""); // #5. Partition Info
    String optionsInfo = getOptionValue('O', ""); // #6. Partition Info
    Boolean isOverwrite = hasOption('o'); // #7. Overwrite
    String[] localFilePaths = getOptionValues('f'); // #8. Local File Name

    // Perform upload operation
    ConnectionFactory connectionFactory = new KyuubiConnectionFactory(url, user, password);
    UploadAPIClient uploadAPIClient = new UploadAPIClient(connectionFactory);

    Boolean res =
        ConnectionFactoryHelper.tryWithConnectionFactory(
            connectionFactory,
            new Callable<Boolean>() {
              @Override
              public Boolean call() {
                try {
                  return uploadAPIClient.upload(
                      targetTable, partitionsInfo, optionsInfo, isOverwrite, localFilePaths);
                } catch (Exception e) {
                  logger.error("Failed to upload data", e);
                  return false;
                }
              }
            });
    if (!res) {
      logger.error("Failed to upload data, abort");
      System.exit(1);
    }
  }

  private static String getOptionValue(String opt, String defaultVal) {
    String val = commandLine.getOptionValue(opt, defaultVal);
    if (StringUtils.equals(val, defaultVal)) {
      String v = getOptionValueFromConfigMap(String.valueOf(opt));
      if (StringUtils.isNotBlank(v)) {
        val = v;
      }
    }
    return val;
  }

  private static String getOptionValue(char opt, String defaultVal) {
    return getOptionValue(String.valueOf(opt), defaultVal);
  }

  private static String[] getOptionValues(char opt) {
    String[] result = commandLine.getOptionValues(String.valueOf(opt));
    if (result == null) {
      return StringUtils.split(getOptionValueFromConfigMap(String.valueOf(opt)));
    }
    return result;
  }

  private static boolean hasOption(char opt) {
    Boolean val = commandLine.hasOption(String.valueOf(opt));
    if (!val) {
      val = checkOptionValueFromConfigMap(String.valueOf(opt));
    }
    return val;
  }

  private static String getOptionValueFromConfigMap(String opt) {
    final String[] val = {""};
    options.getOptions().stream()
        .filter(
            option ->
                StringUtils.equals(String.valueOf(opt), ((Option) option).getOpt())
                    || StringUtils.equals(String.valueOf(opt), ((Option) option).getLongOpt()))
        .forEach(
            option -> {
              if (configuredOptions.containsKey(((Option) option).getOpt())) {
                val[0] = configuredOptions.get(((Option) option).getOpt());
              }
              if (configuredOptions.containsKey(((Option) option).getLongOpt())) {
                val[0] = configuredOptions.get(((Option) option).getLongOpt());
              }
            });
    return val[0];
  }

  private static Boolean checkOptionValueFromConfigMap(String opt) {
    String v = getOptionValueFromConfigMap(opt);
    if (StringUtils.isNotBlank(v)) {
      return Boolean.valueOf(v);
    }
    return false;
  }
}
