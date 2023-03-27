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

import static org.apache.kyuubi.ebay.carmel.gateway.config.CarmelConfig.ConfVars.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.Set;
import org.apache.kyuubi.ebay.carmel.gateway.config.CarmelConfig;
import org.junit.Before;
import org.junit.Test;

public class EndpointManagerTest {
  private EndpointManager endpointManager;

  private CarmelConfig config;

  @Before
  public void setup() {
    config = CarmelConfig.getInstance();
    endpointManager = new EndpointManager(config);
  }

  @Test
  public void testIgnoreQueues() {
    config.setVar(CARMEL_GATEWAY_AUTO_QUEUE_IGNORE_LIST, "test,test1");
    endpointManager = new EndpointManager(config);
    UserInfo userInfo = new UserInfo("ganma", "");
    try {
      endpointManager.createEndpoint(userInfo, 0);
      assertTrue("should throw exception here", false);
    } catch (Exception e) {
      // must go here
    }
    userInfo.setAssignedQueue("test");
    SparkEndpoint endpoint = endpointManager.createEndpoint(userInfo, 0);
    assertEquals("test", endpoint.getQueue().getName());
  }

  @Test
  public void testUserStick() {
    UserInfo userInfo = new UserInfo("ganma", "");
    userInfo.setAssignedQueue("test");
    SparkEndpoint endpoint = endpointManager.createEndpoint(userInfo, 0);

    for (int i = 0; i < 10; i++) {
      SparkEndpoint newEndpoint = endpointManager.createEndpoint(userInfo, 0);
      assertEquals(endpoint.getServerUrl(), newEndpoint.getServerUrl());
    }

    userInfo = new UserInfo("b_carmel", "");
    endpoint = endpointManager.createEndpoint(userInfo, 0);

    for (int i = 0; i < 10; i++) {
      SparkEndpoint newEndpoint = endpointManager.createEndpoint(userInfo, 0);
      assertEquals(endpoint.getServerUrl(), newEndpoint.getServerUrl());
    }

    userInfo = new UserInfo("anonymous", "");
    endpoint = endpointManager.createEndpoint(userInfo, 0);

    for (int i = 0; i < 10; i++) {
      SparkEndpoint newEndpoint = endpointManager.createEndpoint(userInfo, 0);
      assertEquals(endpoint.getServerUrl(), newEndpoint.getServerUrl());
    }
  }

  @Test
  public void testTagEndpointSelection() {
    config.setVar(CARMEL_GATEWAY_ENDPOINT_SELECT_STRATEGY, "tag");
    config.setVar(CARMEL_GATEWAY_AUTO_QUEUE_IGNORE_LIST, "");
    endpointManager = new EndpointManager(config);
    UserInfo userInfo = new UserInfo("jingpli", "");
    userInfo.setTags(Lists.newArrayList("schedule"));
    SparkEndpoint endpoint = endpointManager.createEndpoint(userInfo, 0);
    assertEquals("localhost:10002", endpoint.getServerUrl());

    // no tag
    userInfo = new UserInfo("jingpli", "");
    endpoint = endpointManager.createEndpoint(userInfo, 0);
    assertEquals("localhost:10003", endpoint.getServerUrl());

    userInfo = new UserInfo("anonymous", "");
    endpoint = endpointManager.createEndpoint(userInfo, 0);
    assertEquals("localhost:10004", endpoint.getServerUrl());

    userInfo = new UserInfo("ganma", "");
    Set<String> testEndpoints = Sets.newHashSet("localhost:10000", "localhost:10001");
    for (int i = 0; i < 4; i++) {
      endpoint = endpointManager.createEndpoint(userInfo, 0);
      assertTrue(testEndpoints.contains(endpoint.getServerUrl()));
    }

    userInfo = new UserInfo("jingpli", "");
    userInfo.setTags(Lists.newArrayList("abc"));
    Set<String> hadoopEndpoints = Sets.newHashSet("localhost:10002", "localhost:10003");
    for (int i = 0; i < 4; i++) {
      endpoint = endpointManager.createEndpoint(userInfo, 0);
      assertTrue(hadoopEndpoints.contains(endpoint.getServerUrl()));
    }

    userInfo = new UserInfo("zuding", "");
    userInfo.setTags(Lists.newArrayList("schedule"));
    endpoint = endpointManager.createEndpoint(userInfo, 0);
    assertEquals("localhost:10005", endpoint.getServerUrl());

    userInfo = new UserInfo("zuding", "");
    userInfo.setTags(Lists.newArrayList());
    endpoint = endpointManager.createEndpoint(userInfo, 0);
    assertEquals("localhost:10006", endpoint.getServerUrl());
  }
}
