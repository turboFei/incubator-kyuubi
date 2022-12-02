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

package org.apache.kyuubi

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, StandardOpenOption}
import java.sql.ResultSet

import scala.collection.mutable.ArrayBuffer

import com.jakewharton.fliptables.FlipTable
import org.scalatest.Assertions.convertToEqualizer

object TestUtils {

  def verifyOutput(markdown: Path, newOutput: ArrayBuffer[String], agent: String): Unit = {
    if (System.getenv("KYUUBI_UPDATE") == "1") {
      val writer = Files.newBufferedWriter(
        markdown,
        StandardCharsets.UTF_8,
        StandardOpenOption.TRUNCATE_EXISTING,
        StandardOpenOption.CREATE)
      try {
        newOutput.foreach { line =>
          writer.write(line)
          writer.newLine()
        }
      } finally {
        writer.close()
      }
    } else {
      val expected = new ArrayBuffer[String]()

      val reader = Files.newBufferedReader(markdown, StandardCharsets.UTF_8)
      var line = reader.readLine()
      while (line != null) {
        expected += line
        line = reader.readLine()
      }
      reader.close()
      val hint = s"$markdown out of date, please update doc with " +
        s"KYUUBI_UPDATE=1 build/mvn clean install -Pflink-provided,spark-provided,hive-provided " +
        s"-DwildcardSuites=$agent"
      assert(newOutput.size === expected.size, hint)

      newOutput.zip(expected).foreach { case (out, in) => assert(out === in, hint) }
    }
  }

  def displayResultSet(resultSet: ResultSet): Unit = {
    if (resultSet == null) throw new NullPointerException("resultSet == null")
    val resultSetMetaData = resultSet.getMetaData
    val columnCount: Int = resultSetMetaData.getColumnCount
    val headers = (1 to columnCount).map(resultSetMetaData.getColumnName).toArray
    val data = ArrayBuffer.newBuilder[Array[String]]
    while (resultSet.next) {
      data += (1 to columnCount).map(resultSet.getString).toArray
    }
    // scalastyle:off println
    println(FlipTable.of(headers, data.result().toArray))
    // scalastyle:on println
  }
}
