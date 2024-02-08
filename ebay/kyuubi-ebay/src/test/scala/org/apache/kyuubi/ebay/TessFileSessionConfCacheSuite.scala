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

package org.apache.kyuubi.ebay

import org.apache.kyuubi.KyuubiFunSuite

class TessFileSessionConfCacheSuite extends KyuubiFunSuite {
  test("get context conf only") {
    assert(TessFileSessionConfCache.getTessContextSessionConf("35") ==
      Map("kyuubi.kubernetes.context" -> "35", "spark.k1" -> "v1"))
  }

  test("get context and namespace conf") {
    assert(TessFileSessionConfCache.getTessContextSessionConf("35", "k8s_namespace") ==
      Map("kyuubi.kubernetes.context" -> "35", "spark.k1" -> "v2"))
  }
}
