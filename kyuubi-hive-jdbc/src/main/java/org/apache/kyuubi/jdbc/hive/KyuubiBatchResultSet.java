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

package org.apache.kyuubi.jdbc.hive;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.stream.Collectors;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.TableSchema;
import org.apache.hive.service.rpc.thrift.TTableSchema;

public class KyuubiBatchResultSet extends KyuubiBaseResultSet {
  private Iterator<Object[]> fetchedRowsItr;

  public KyuubiBatchResultSet(RowSet rowSet, TTableSchema tableSchema) {
    this.fetchedRowsItr = rowSet.iterator();
    setSchema(new TableSchema(tableSchema));
    this.columnNames =
        getSchema().getColumnDescriptors().stream()
            .map(desc -> desc.getName())
            .collect(Collectors.toList());
    this.columnTypes =
        getSchema().getColumnDescriptors().stream()
            .map(desc -> desc.getTypeName())
            .collect(Collectors.toList());
    this.columnAttributes = new ArrayList(getSchema().getSize());
    for (int i = 0; i < getSchema().getSize(); i++) {
      columnAttributes.add(null);
    }
  }

  @Override
  public boolean next() throws SQLException {
    if (fetchedRowsItr.hasNext()) {
      row = fetchedRowsItr.next();
      return true;
    } else {
      return false;
    }
  }

  public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
    // JDK 1.7
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
    // JDK 1.7
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void close() throws SQLException {}
}
