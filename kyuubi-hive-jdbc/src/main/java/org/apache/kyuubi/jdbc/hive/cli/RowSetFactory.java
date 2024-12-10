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

package org.apache.kyuubi.jdbc.hive.cli;

import static org.apache.kyuubi.shaded.hive.service.rpc.thrift.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V6;

import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TProtocolVersion;
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TRowSet;
import org.apache.kyuubi.shaded.thrift.TException;

public class RowSetFactory {

  // This call is accessed from client (jdbc) side
  public static RowSet create(TRowSet results, TProtocolVersion version) throws TException {
    if (version.getValue() >= HIVE_CLI_SERVICE_PROTOCOL_V6.getValue()) {
      return new ColumnBasedSet(results);
    }
    return new RowBasedSet(results);
  }
}
