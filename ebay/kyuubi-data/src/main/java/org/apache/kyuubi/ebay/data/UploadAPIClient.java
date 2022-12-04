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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.kyuubi.ebay.data.connection.ConnectionFactory;
import org.apache.kyuubi.ebay.data.connection.ConnectionFactoryHelper;
import org.apache.kyuubi.ebay.data.connection.ConnectionOperation;
import org.apache.kyuubi.jdbc.hive.KyuubiConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UploadAPIClient {
  private static Logger LOG = LoggerFactory.getLogger(UploadAPIClient.class);
  private final String notAllowedCsvOptions = "path";
  private final Long MAX_PARTITION_BYTES = 10 * 1024 * 1024L;
  private final Set<String> notAllowedCsvOptionSet = new HashSet<>();
  private ConnectionFactory connectionFactory;

  public UploadAPIClient(ConnectionFactory connectionFactory) {
    this.connectionFactory = connectionFactory;
    for (String option : notAllowedCsvOptions.split(",")) {
      notAllowedCsvOptionSet.add(option);
    }
  }

  public static ArrayList<ArrayList<String>> query(Connection conn, String sql)
      throws SQLException {
    ArrayList<ArrayList<String>> result = new ArrayList<>();
    try {
      Statement st = conn.createStatement();
      ResultSet res = st.executeQuery(sql);
      while (res.next()) {
        ArrayList<String> rowResult = new ArrayList<>();
        for (int i = 0; i < res.getMetaData().getColumnCount(); i++) {
          rowResult.add(res.getString(i + 1));
        }
        result.add(rowResult);
      }
      res.close();
      st.close();
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      throw e;
    }
    return result;
  }

  public Boolean upload(
      String targetTable,
      String partitionsInfo,
      String optionsInfo,
      Boolean isOverwrite,
      String[] localFilePaths)
      throws Exception {
    Map<String, String> partitions = new HashMap<>();
    if (StringUtils.isNotBlank(partitionsInfo)) {
      try {
        for (String partition : StringUtils.split(partitionsInfo, ";,")) {
          String[] arr = StringUtils.split(partition, ":= ");
          if (arr.length == 0) {
            continue;
          }
          partitions.put(arr[0].trim(), arr[1].trim());
        }
      } catch (Exception e) {
        LOG.error("Failed to parse partitions, e.g. dt=20200103,n=2: " + e.getMessage());
      }
    }

    Map<String, String> options = new HashMap<>();
    if (StringUtils.isNotBlank(optionsInfo)) {
      try {
        for (String option : StringUtils.split(optionsInfo, ",")) {
          String[] arr = StringUtils.split(option, "= ");
          if (arr.length == 0) {
            continue;
          }
          String name = arr[0].trim();
          if (notAllowedCsvOptionSet.contains(name)) {
            throw new Exception(
                String.format(
                    "Not allowed option: %s, not allowed options: %s",
                    name, notAllowedCsvOptionSet));
          }
          if (StringUtils.equalsIgnoreCase("delimiter", name) && arr.length == 1) {
            options.put(name, ",");
            continue;
          }
          if (StringUtils.equalsIgnoreCase("header", name) && arr.length == 1) {
            options.put(name, "false");
            continue;
          }
          String value = arr[1].trim();
          options.put(name, value);
        }
      } catch (Exception e) {
        LOG.error(
            "Failed to parse partitions, e.g. header=false,delimiter=; details: " + e.getMessage());
      }
    }

    LOG.info(
        String.format(
            "Uploading data from %s to table %s with partitions %s, options %s, overwrite: %s",
            Arrays.asList(localFilePaths), targetTable, partitions, options, isOverwrite));

    Class.forName("org.apache.kyuubi.jdbc.KyuubiHiveDriver");

    for (String localFilePath : localFilePaths) {
      LOG.info("Start to upload file " + localFilePath);
      Boolean res = upload(targetTable, partitions, options, isOverwrite, localFilePath);
      if (!res) {
        return false;
      }
      // set isOverwrite to false if >1 files are uploaded
      isOverwrite = false;
    }
    return true;
  }

  private Boolean upload(
      String targetTable,
      Map<String, String> partitions,
      Map<String, String> options,
      Boolean isOverwrite,
      String localFilePath)
      throws Exception {
    /* Call the new data upload API provided in KyuubiConnection */
    for (Integer retryCount = 0; retryCount <= 5; retryCount++) {
      Integer finalRetryCount = retryCount;
      Boolean result =
          ConnectionFactoryHelper.tryWithConnection(
              connectionFactory,
              new ConnectionOperation<Boolean>() {
                @Override
                public Boolean execute(Connection conn) throws Exception {
                  try {
                    query(conn, "set spark.sql.files.maxPartitionBytes=" + MAX_PARTITION_BYTES);
                    String remoteFilePath =
                        ((KyuubiConnection) conn)
                            .uploadData(
                                localFilePath, targetTable, partitions, options, isOverwrite);
                    LOG.info(
                        String.format(
                            "Uploaded[%s], remote path: %s", localFilePath, remoteFilePath));
                    return true;
                  } catch (Exception e) {
                    LOG.error(
                        String.format(
                            "Failed to upload [%s], will retry, current retry count: %d. details: %s",
                            localFilePath, finalRetryCount, e.getMessage()));
                    if (finalRetryCount >= 5) {
                      throw new Exception(
                          "Failed to upload data after 5 retries, please retry later.", e);
                    } else {
                      LOG.info("Sleeping 30 seconds before next retry");
                      Thread.sleep(30000);
                    }
                  }
                  return false;
                }
              });
      if (result) {
        return true;
      }
    }
    return false;
  }
}
