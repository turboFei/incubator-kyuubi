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

package org.apache.kyuubi.client.util;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import org.apache.kyuubi.client.api.v1.dto.Batch;
import org.apache.kyuubi.client.exception.KyuubiRestException;

public final class BatchUtils {
  /** The batch has not been submitted to resource manager yet. */
  public static String PENDING_STATE = "PENDING";

  /** The batch has been submitted to resource manager and is running. */
  public static String RUNNING_STATE = "RUNNING";

  /** The batch has been finished successfully. */
  public static String FINISHED_STATE = "FINISHED";

  /** The batch met some issue and failed. */
  public static String ERROR_STATE = "ERROR";

  /** The batch was closed by `DELETE /batches/${batchId}` api. */
  public static String CANCELED_STATE = "CANCELED";

  public static List<String> terminalBatchStates =
      Arrays.asList(FINISHED_STATE, ERROR_STATE, CANCELED_STATE);

  public static String KYUUBI_BATCH_ID_KEY = "kyuubi.batch.id";

  public static String KYUUBI_BATCH_DUPLICATED_KEY = "kyuubi.batch.duplicated";

  public static boolean isPendingState(String state) {
    return PENDING_STATE.equalsIgnoreCase(state);
  }

  public static boolean isRunningState(String state) {
    return RUNNING_STATE.equalsIgnoreCase(state);
  }

  public static boolean isFinishedState(String state) {
    return FINISHED_STATE.equalsIgnoreCase(state);
  }

  public static boolean isTerminalState(String state) {
    return state != null && terminalBatchStates.contains(state.toUpperCase(Locale.ROOT));
  }

  public static boolean isDuplicatedSubmission(Batch batch) {
    return "true".equalsIgnoreCase(batch.getBatchInfo().get(KYUUBI_BATCH_DUPLICATED_KEY));
  }

  /** Using SPARK_BATCH_ETL_SQL_ENCODED_STATEMENTS_KEY instead */
  @Deprecated
  public static String SPARK_BATCH_ETL_SQL_STATEMENTS_KEY = "spark.kyuubi.batch.etl.sql.statements";

  /** Base64 encoded etl sql statements. */
  public static String SPARK_BATCH_ETL_SQL_ENCODED_STATEMENTS_KEY =
      "spark.kyuubi.batch.etl.sql.encoded.statements";

  public static String getStatementsFromFiles(List<String> fileNames) {
    StringBuilder sb = new StringBuilder();
    for (String fileName : fileNames) {
      try {
        Path path = Paths.get(fileName);
        sb.append(new String(Files.readAllBytes(path), StandardCharsets.UTF_8));
        sb.append("\n;");
      } catch (IOException e) {
        throw new KyuubiRestException("Error reading statements from " + fileName, e);
      }
    }
    return sb.toString();
  }
}
