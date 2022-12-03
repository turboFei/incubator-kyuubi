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

package org.apache.kyuubi.ebay.data.utils;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.commons.cli.*;
import org.apache.kyuubi.ebay.data.DownloadAPI;
import org.apache.kyuubi.ebay.data.UploadAPI;

public class CommandLineUtils {
  public static CommandLine parse(String[] args, Options options) throws ParseException {
    CommandLineParser parser = new DefaultParser();
    // Parse the program arguments
    return parser.parse(options, args);
  }

  private static Options buildConnectCommandLineOptions() {
    Options options = new Options();
    options.addOption("h", "help", false, "Print this usage information");
    options.addOption("v", "version", false, "current version");

    options.addOption("u", "url", true, "JDBC url. e.g. jdbc:hive2://127.0.0.1:10009/default");
    options.addOption("n", "user", true, "[ldap auth] User name");
    options.addOption(
        Option.builder()
            .argName("password")
            .optionalArg(true)
            .desc("[ldap auth] User password")
            .longOpt("password")
            .option("p")
            .build());
    options.addOption(
        "w", "password-file", true, "[ldap auth] The password file to read User password");
    return options;
  }

  public static Options buildUploadCommandLineOptions() {
    Options options = buildConnectCommandLineOptions();
    options.addOption("t", "table", true, "Target table name. e.g. p_kyuubi.iris_data");
    options.addOption("P", "partitions", true, "Partition info, e.g. dt=20200103,n=2");
    options.addOption("O", "options", true, "CSV Options info, e.g. header=false,delimiter=;");
    options.addOption("o", "overwrite", false, "Overwrite target table or partition. e.g. true");
    options.addOption("conf", true, "The config file path.");
    options.addOption(
        Option.builder()
            .argName("files")
            .hasArgs()
            .desc("local file paths. e.g. /Users/kyuubi/iris_data.csv /Users/kyuubi/iris_data.csv2")
            .longOpt("files")
            .option("f")
            .build());
    return options;
  }

  public static Options buildDownloadCommandLineOptions() {
    Options options = buildConnectCommandLineOptions();
    options.addOption("t", "table", true, "<optional> Table name. e.g. p_kyuubi.iris_data");
    options.addOption("q", "query", true, "<optional> Query. e.g. SELECT ID FROM RANGE(100)");
    options.addOption(
        "f", "format", true, "<optional> Output format. e.g. parquet, csv, orc, json");
    options.addOption(
        "O",
        "options",
        true,
        "<optional> Options info. e.g. header=false,delimiter=;,numFiles=1,timestampFormat=yyyy/MM/dd HH:mm:ss,dateFormat=yyyy/MM/dd,compression=snappy");
    // options.addOption("o", "overwrite", false, "Overwrite target table or partition. e.g. true");
    options.addOption("d", "dest", true, "Local file path. e.g./tmp/test");
    return options;
  }

  public static void printUploadHelp(Options options) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp(UploadAPI.class.getName() + " [OPTION]...", options);
  }

  public static void printDownloadHelp(Options options) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp(DownloadAPI.class.getName() + " [OPTION]...", options);
  }

  /** Obtains a password from the passed file path. */
  public static String obtainPasswordFromFile(String passwordFilePath) {
    try {
      Path path = Paths.get(passwordFilePath);
      byte[] passwordFileContents = Files.readAllBytes(path);
      return new String(passwordFileContents, "UTF-8").trim();
    } catch (Exception e) {
      throw new RuntimeException(
          "Unable to read user password from the password file: " + passwordFilePath, e);
    }
  }
}
