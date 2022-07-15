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

package org.apache.kyuubi.ha.client.zookeeper

import org.apache.curator.framework.api.ACLProvider
import org.apache.zookeeper.ZooDefs
import org.apache.zookeeper.data.ACL

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.ha.HighAvailabilityConf
import org.apache.kyuubi.ha.client.AuthTypes

class ZookeeperACLProvider(conf: KyuubiConf) extends ACLProvider {

  /**
   * Return the ACL list to use by default.
   *
   * @return default ACL list
   */
  override lazy val getDefaultAcl: java.util.List[ACL] = {
    val nodeAcls = new java.util.ArrayList[ACL]

    def addACL(): Unit = {
      // Read all to the world
      nodeAcls.addAll(ZooDefs.Ids.READ_ACL_UNSAFE)
      // Create/Delete/Write/Admin to the authenticated user
      nodeAcls.addAll(ZooDefs.Ids.CREATOR_ALL_ACL)
    }

    if (conf.get(HighAvailabilityConf.HA_ENGINE_REF_ID).isEmpty && enabledServerAcls()) {
      addACL()
    } else if (conf.get(HighAvailabilityConf.HA_ENGINE_REF_ID).nonEmpty && enabledEngineAcls()) {
      addACL()
    } else {
      // ACLs for znodes on a non-kerberized cluster
      // Create/Read/Delete/Write/Admin to the world
      nodeAcls.addAll(ZooDefs.Ids.OPEN_ACL_UNSAFE)
    }
    nodeAcls
  }

  private def enabledServerAcls(): Boolean = AuthTypes
    .withName(conf.get(HighAvailabilityConf.HA_ZK_AUTH_TYPE)) match {
    case AuthTypes.NONE => false
    case _ => true
  }

  private def enabledEngineAcls(): Boolean = AuthTypes
    .withName(conf.get(HighAvailabilityConf.HA_ZK_ENGINE_AUTH_TYPE)) match {
    case AuthTypes.NONE => false
    case _ => true
  }

  override def getAclForPath(path: String): java.util.List[ACL] = getDefaultAcl
}
