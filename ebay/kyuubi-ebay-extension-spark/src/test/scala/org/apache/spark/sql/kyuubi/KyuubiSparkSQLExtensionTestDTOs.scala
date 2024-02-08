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

import org.apache.spark.sql.Column
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{BinaryType, StringType}

case class Person(id: Long, name: String, age: Long)
case class Book(id: Long, name: String, data: Array[Byte])

class BinaryTypeFieldTruncationHandler extends FieldTruncationHandler {

  def truncate(column: Column): Option[Column] = {
    column.expr.dataType match {
      case _: BinaryType => Some(udf(
          new UDF1[Any, String] {
            override def call(input: Any): String = {
              if (input != null) "@Binary" else null
            }
          },
          StringType).apply(column))
      case _ => None
    }
  }

}
