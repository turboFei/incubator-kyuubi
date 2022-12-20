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

import java.util.{Map => JMap}

import org.apache.kyuubi.ebay.NoopGroupProvider.GROUP_PLACEHOLDER
import org.apache.kyuubi.plugin.GroupProvider

/**
 * To align with community and prevent the group overhead.
 * kyuubi.session.group.provider=org.apache.kyuubi.ebay.NoopGroupProvider
 */
class NoopGroupProvider extends GroupProvider {
  override def primaryGroup(user: String, sessionConf: JMap[String, String]): String =
    GROUP_PLACEHOLDER
}

object NoopGroupProvider {
  final val GROUP_PLACEHOLDER: String = "__ebay__"
}
