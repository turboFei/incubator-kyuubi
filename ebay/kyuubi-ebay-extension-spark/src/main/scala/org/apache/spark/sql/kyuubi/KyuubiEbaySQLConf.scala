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

package org.apache.spark.sql.kyuubi

import org.apache.spark.sql.internal.SQLConf._
import org.apache.spark.sql.kyuubi.FieldsTruncationHandler.TRUNCATION_KEY_PREFIX

object KyuubiEbaySQLConf {

  val FIELDS_TRUNCATION_ENABLED =
    buildConf(s"$TRUNCATION_KEY_PREFIX.enabled")
      .doc("Whether truncate the result before collect to the driver. " +
        "The truncation is based on the data type," +
        " and users can specify the impl class for each data type")
      .version("1.9.0")
      .booleanConf
      .createWithDefault(false)

  val FIELDS_TRUNCATION_STRING_TYPE_IMPL =
    buildConf(s"$TRUNCATION_KEY_PREFIX.StringType.impl")
      .doc(
        "The truncation impl for string type field")
      .version("1.9.0")
      .stringConf
      .createWithDefault(classOf[StringTypeFieldTruncationHandler].getName)

  val FIELDS_TRUNCATION_STRING_TYPE_MAX_LENGTH =
    buildConf(s"$TRUNCATION_KEY_PREFIX.StringType.maxLength")
      .doc(
        "The max length(include) of string type field," +
          " the part out of length limit will be truncated")
      .version("1.9.0")
      .intConf
      .createWithDefault(256)
}
