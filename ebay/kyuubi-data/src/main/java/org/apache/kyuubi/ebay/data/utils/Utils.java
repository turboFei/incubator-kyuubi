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

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils {
  private static Logger logger = LoggerFactory.getLogger(Utils.class);

  public static void parseDataFromFile(String path, LineHandler lineHandler) throws IOException {
    FileInputStream inputStream = null;
    BufferedReader bufferedReader = null;
    try {
      inputStream = new FileInputStream(path);
      bufferedReader = new BufferedReader(new InputStreamReader(inputStream));

      String str = null;
      while ((str = bufferedReader.readLine()) != null) {
        str = str.trim();
        lineHandler.handle(str);
      }
    } catch (Exception e) {
      logger.error(String.format("Failed to parse data from path: %s", path), e);
      throw e;
    } finally {
      if (bufferedReader != null) {
        try {
          bufferedReader.close();
        } catch (Exception e) {
          logger.error(String.format("Failed to close buffered Reader for path: %s", path), e);
          throw e;
        }
      }
      if (inputStream != null) {
        try {
          inputStream.close();
        } catch (Exception e) {
          logger.error(String.format("Failed to close file input stream for path: %s", path), e);
          throw e;
        }
      }
    }
  }

  public static interface LineHandler {
    void handle(String line);
  }
}
