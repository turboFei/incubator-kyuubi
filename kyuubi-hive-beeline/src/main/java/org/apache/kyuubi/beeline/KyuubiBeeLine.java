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

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * This source file is based on code taken from SQLLine 1.0.2
 * See SQLLine notice in LICENSE
 */
package org.apache.kyuubi.beeline;

import java.io.IOException;
import java.io.InputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hive.beeline.BeeLine;

public class KyuubiBeeLine extends BeeLine {
  private static final Logger LOG = LoggerFactory.getLogger(KyuubiBeeLine.class.getName());
  public static final String PROPERTY_PREFIX = "beeline.";
  public static final String PROPERTY_NAME_EXIT = PROPERTY_PREFIX + "system.exit";
  public static final String KYUUBI_HIVE_DRIVER = "org.apache.kyuubi.jdbc.KyuubiHiveDriver";

  public KyuubiBeeLine() {
    super();
    try {
      Class.forName(KYUUBI_HIVE_DRIVER);
      addLocalDriverClazz(KYUUBI_HIVE_DRIVER);
    } catch (ClassNotFoundException e) {
      LOG.error("Failed to find class for Kyuubi hive dirver with name:" + KYUUBI_HIVE_DRIVER);
    }
  }

  public static void main(String[] args) throws IOException {
    mainWithInputRedirection(args, null);
  }

  public static void mainWithInputRedirection(String[] args, InputStream inputStream)
    throws IOException {
    KyuubiBeeLine beeLine = new KyuubiBeeLine();

    try {
      int status = beeLine.begin(args, inputStream);

      if (!Boolean.getBoolean(PROPERTY_NAME_EXIT)) {
        System.exit(status);
      }
    } finally {
      beeLine.close();
    }
  }
}
