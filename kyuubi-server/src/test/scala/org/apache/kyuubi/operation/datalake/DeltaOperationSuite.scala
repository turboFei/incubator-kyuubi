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

package org.apache.kyuubi.operation.datalake

import org.apache.kyuubi.WithKyuubiServer
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.operation.DeltaMetadataTests
import org.apache.kyuubi.tags.DeltaTest

@DeltaTest
class DeltaOperationSuite extends WithKyuubiServer with DeltaMetadataTests {
  override protected val conf: KyuubiConf = {
    val kyuubiConf = KyuubiConf().set(KyuubiConf.ENGINE_IDLE_TIMEOUT, 20000L)
    extraConfigs.foreach { case (k, v) => kyuubiConf.set(k, v) }
    kyuubiConf
  }

  override protected def extraConfigs: Map[String, String] = {
    super.extraConfigs ++ Map("kyuubi.engineEnv.SPARK_TESTING" -> "true")
  }

  override def jdbcUrl: String = getJdbcUrl
}
