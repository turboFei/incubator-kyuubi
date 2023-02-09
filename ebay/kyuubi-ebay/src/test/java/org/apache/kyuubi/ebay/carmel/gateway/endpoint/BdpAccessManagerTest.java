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

package org.apache.kyuubi.ebay.carmel.gateway.endpoint;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.base.Charsets;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class BdpAccessManagerTest {
  BdpAccessManager accessManager;

  @Before
  public void setup() throws Exception {
    accessManager = new BdpAccessManager("https://bdp.vip.ebay.com", "hermesrno");
  }

  @Test
  public void testParseQueueSuccess() throws IOException {
    String result =
        "{\"success\":true,"
            + "\"result\":"
            + "[{\"clusterId\":16,"
            + "\"clusterName\":\"hermesrno\","
            + "\"queueName\":\"hdmi-data\","
            + "\"parentQueueName\":\"root\","
            + "\"submitApps\":\" hdmi-data-hermes\","
            + "\"budgetGroupId\":29,"
            + "\"budgetGroup\":\"Data\","
            + "\"info\":null,\"version\":1,"
            + "\"leafQueue\":true,"
            + "\"defaultQueue\":false,"
            + "\"state\":\"RUNNING\","
            + "\"absCapacity\":0.04,"
            + "\"maxAbsCapacity\":1.0,"
            + "\"nodeLabel\":null,"
            + "\"labelInfo\":{},"
            + "\"owned\":false,"
            + "\"accessible\":true,\"applied\":false,\"sameBudget\":false,"
            + "\"owners\":null,\"subQueues\":null}]}";
    List<QueueInfo> queues =
        accessManager.parseQueueResult(new ByteArrayInputStream(result.getBytes(Charsets.UTF_8)));
    assertTrue(queues.size() == 1);
  }

  @Test(expected = CarmelRuntimeException.class)
  public void testParseQueueFail() throws IOException {
    String result =
        "{\"success\":false,"
            + "\"result\":[],"
            + "\"message\":\"lvsdpead004.apd.ebay.com:389; nested exception is javax.naming.CommunicationException\"}";
    accessManager.parseQueueResult(new ByteArrayInputStream(result.getBytes(Charsets.UTF_8)));
  }

  @Test(expected = CarmelRuntimeException.class)
  public void testParseServiceMappingFail() throws IOException {
    String result =
        "{\"success\":false,"
            + "\"result\":[],"
            + "\"message\":\"lvsdpead004.apd.ebay.com:389; nested exception is javax.naming.CommunicationException\"}";
    accessManager.parseAccountMappingResult(
        "user", new ByteArrayInputStream(result.getBytes(Charsets.UTF_8)));
  }

  @Test
  public void testParseBatchAccountMapping() throws Exception {
    String result =
        "{\"success\":true," + "\"result\":[\"b_kylin\",\"b_carmel\"]," + "\"message\":\"\"}";
    List<String> mappingResult =
        accessManager.parseAccountMappingResult(
            "testAccount", new ByteArrayInputStream(result.getBytes(Charsets.UTF_8)));
    assertEquals("b_kylin", mappingResult.get(0));
    assertEquals("b_carmel", mappingResult.get(1));
  }

  @Test
  public void testParseGroupUser() throws Exception {
    String result =
        "{\"success\":true,\"result\":[{\"members\":[\"aigong\",\"mocheng\",\"nansong\",\"ruoxu\",\"wmin\",\"xiaoyang\",\"yapzhou\",\"yawei\",\"yqin4\"],\"groups\":[],\"groupName\":\"brisk_impersonation\",\"appliable\":true,\"info\":\"used for BMS platform\"}],\"message\":\"\"}";
    List<String> groupUsers =
        accessManager.parseGroupUsersResult(
            "risk_platform/brisk_impersonation",
            new ByteArrayInputStream(result.getBytes(Charsets.UTF_8)));
    Set<String> groupUsersSet = new HashSet<>(groupUsers);
    assertEquals(9, groupUsers.size());
    assertTrue(groupUsersSet.contains("aigong"));
  }

  @Ignore
  public void testGetGroupUsers() throws Exception {
    Set<String> users = accessManager.getGroupUsers("risk_platform/brisk_impersonation");
    System.out.println(users);
  }

  @Test
  public void testParseAllUsersQueues() throws Exception {
    String result =
        "{\"success\":true,\"result\":[{\"name\":\"sohdas\",\"queues\":[\"hdmi-default\"]},{\"name\":\"jjestribek\",\"queues\":[\"hdmi-default\",\"hdmi-gmo\"]},{\"name\":\"ruirli\",\"queues\":[\"hdmi-gcx\",\"hdmi-default\"]}],\"message\":null}\n";
    Map<String, BdpAccessManager.UserQueuesCache> res =
        accessManager.parseAllQueueResult(
            new ByteArrayInputStream(result.getBytes(Charsets.UTF_8)));

    assertEquals(3, res.size());
    assertTrue(res.containsKey("jjestribek"));
    assertTrue(res.get("jjestribek").queues.size() == 2);
  }
}
