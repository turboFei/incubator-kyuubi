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

package org.apache.kyuubi.ebay.carmel.gateway.config;

import java.util.LinkedHashSet;
import java.util.Set;

public class StringSet implements Validator {

  private final boolean caseSensitive;
  private final Set<String> expected = new LinkedHashSet<>();

  public StringSet(String... values) {
    this(true, values);
  }

  public StringSet(boolean caseSensitive, String... values) {
    this.caseSensitive = caseSensitive;
    for (String value : values) {
      expected.add(caseSensitive ? value : value.toLowerCase());
    }
  }

  @Override
  public String validate(String value) {
    if (value == null || !expected.contains(caseSensitive ? value : value.toLowerCase())) {
      return "Invalid value.. expects one of " + expected;
    }
    return null;
  }
}
