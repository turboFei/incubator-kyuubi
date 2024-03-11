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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.kyuubi.ebay.carmel.gateway.config.CarmelConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EndpointManager {
  private static final Logger LOG = LoggerFactory.getLogger(EndpointManager.class);

  private CarmelConfig config;
  private AccessManager accessManager;
  private ThriftDiscovery discovery;

  // todo need to clean the fail servers properly?
  private ConcurrentMap<String, ServerFailure> failServers;
  private EndpointSelectStrategy endpointSelectStrategy;
  private final String clusterDefaultQueue;
  // queues that only be selected when user explicitly specified
  // the auto selection strategy will ignore these queues.
  private final Set<String> queueSelectionIgnorelist;

  public EndpointManager(CarmelConfig config) {
    this.config = config;
    this.failServers = Maps.newConcurrentMap();
    this.endpointSelectStrategy = initEndpointSelectStrategy(config);
    try {
      this.discovery = initDiscovery(config);
      this.accessManager = initAccessManager(config);
    } catch (Exception e) {
      throw new CarmelRuntimeException("error when init endpoint manager", e);
    }
    this.clusterDefaultQueue =
        config.getVar(CarmelConfig.ConfVars.CARMEL_GATEWAY_CLUSTER_DEFAULT_QUEUE);
    String queueIgnoreString =
        config.getVar(CarmelConfig.ConfVars.CARMEL_GATEWAY_AUTO_QUEUE_IGNORE_LIST);
    if (StringUtils.isEmpty(queueIgnoreString)) {
      this.queueSelectionIgnorelist = new HashSet<>();
    } else {
      String[] configIgnoreQueueList = queueIgnoreString.split(",");
      this.queueSelectionIgnorelist = new HashSet<>(Arrays.asList(configIgnoreQueueList));
    }
  }

  private EndpointSelectStrategy initEndpointSelectStrategy(CarmelConfig config) {
    if (config.getVar(CARMEL_GATEWAY_ENDPOINT_SELECT_STRATEGY).equals("tag")) {
      LOG.info("use tag base endpoint selection strategy");
      return new TagBasedSelectStrategy();
    } else {
      return new RandomSelectStrategy();
    }
  }

  private ThriftDiscovery initDiscovery(CarmelConfig config) throws Exception {
    if (config.getVar(CARMEL_GATEWAY_THRIFT_DISCOVERY).equals("file")) {
      return new FileThriftDiscovery(config.getVar(CARMEL_GATEWAY_THRIFT_FILE));
    } else {
      return new ZKThriftDiscovery(config);
    }
  }

  private AccessManager initAccessManager(CarmelConfig config) throws Exception {
    switch (config.getVar(CARMEL_GATEWAY_ACCESS_MODE)) {
      case "bdp":
        accessManager =
            new BdpAccessManager(
                config.getVar(CARMEL_GATEWAY_ACCESS_BDP_URL),
                config.getVar(CARMEL_GATEWAY_ACCESS_BDP_CLUSTER_NAME));
        break;
      case "file":
      default:
        accessManager = new FileAccessManager(config.getVar(CARMEL_GATEWAY_ACCESS_FILE));
    }
    return accessManager;
  }

  public SparkEndpoint createEndpoint(UserInfo userInfo, int retryCnt) {
    return findEndpoint(userInfo, retryCnt != 0);
  }

  public List<SparkEndpoint> getAllEndpointsByUser(UserInfo userInfo, boolean reload) {
    List<SparkEndpoint> result = Lists.newArrayList();
    try {
      if (reload) {
        accessManager.invalidate(userInfo.username);
      }
      List<QueueInfo> queues = accessManager.getUserQueues(userInfo.username);
      if (queues == null || queues.isEmpty()) {
        throw new CarmelRuntimeException(
            "no queue user can run in, please apply the queue access in BDP");
      }
      String assignQueue = userInfo.getAssignedQueue();
      boolean hasMatchQueue = false;
      for (QueueInfo queue : queues) {
        String queueName = queue.getName();
        if (reload) {
          discovery.invalidate(queueName);
        }
        if (assignQueue != null && !assignQueue.equals(queueName)) {
          continue;
        }
        // if no assigned queue, and the queue is in the ignore list, will ignore the queue
        if (assignQueue == null && queueSelectionIgnorelist.contains(queueName)) {
          LOG.info("ignore the queue:{}, because it is in the ignore list", queueName);
          continue;
        }
        hasMatchQueue = true;
        List<ThriftServerInfo> allServers = discovery.getServers(queueName);
        for (ThriftServerInfo server : allServers) {
          result.add(new SparkEndpoint(config, queue, server, userInfo));
        }
      }
      if (assignQueue != null && !hasMatchQueue) {
        throw new CarmelRuntimeException(
            "cannot access the queue:" + assignQueue + ", please go to BDP to request the access");
      } else if (!hasMatchQueue) {
        throw new CarmelRuntimeException(
            "cannot find proper queue, please go to BDP to request the queue access");
      }
    } catch (CarmelRuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new CarmelRuntimeException(
          "error when find endpoints for user:" + userInfo.getUsername(), e);
    }
    return result;
  }

  public List<String> getBatchAccounts(String serviceAccount) throws Exception {
    return accessManager.getBatchAccountsForServiceAccount(serviceAccount);
  }

  public void markFailServer(String serverUrl) {
    LOG.info("the server:{} is marked as a fail server", serverUrl);
    ServerFailure prevFailure = failServers.putIfAbsent(serverUrl, new ServerFailure(serverUrl));
    if (prevFailure != null) {
      prevFailure.incFailCnt();
      prevFailure.updateLastCheckTime();
    }
  }

  public void markGoodServer(String serverUrl) {
    failServers.remove(serverUrl);
  }

  private SparkEndpoint findEndpoint(UserInfo userInfo, boolean reload) {
    List<SparkEndpoint> endpoints = getAllEndpointsByUser(userInfo, reload);
    LOG.info("find candidate endpoints: {} for user: {}", endpoints, userInfo.username);
    if (endpoints == null || endpoints.isEmpty()) {
      throw new CarmelRuntimeException(
          "cannot find proper thrift endpoint for user:" + userInfo.username);
    }
    SparkEndpoint result = selectEndpoint(userInfo, endpoints);
    LOG.info("choose spark endpoint: {} for user: {}", result, userInfo.username);
    if (result == null) {
      throw new CarmelRuntimeException(
          "cannot find proper thrift endpoint for user:" + userInfo.username);
    }
    return result;
  }

  public SparkEndpoint selectEndpoint(UserInfo userInfo, List<SparkEndpoint> endpoints) {
    int size = endpoints.size();
    if (size == 0) {
      return null;
    }
    if (size == 1) {
      return endpoints.get(0);
    }
    List<SparkEndpoint> defQueueEndpoints = Lists.newArrayList();
    List<SparkEndpoint> otherEndpoints = Lists.newArrayList();
    for (SparkEndpoint endpoint : endpoints) {
      if (StringUtils.equalsIgnoreCase(clusterDefaultQueue, endpoint.getQueue().getName())) {
        defQueueEndpoints.add(endpoint);
      } else {
        otherEndpoints.add(endpoint);
      }
    }
    // first select non-default queue endpoints, if not found select default queue endpoints
    SparkEndpoint result = endpointSelectStrategy.selectEndpoint(userInfo, otherEndpoints);
    if (result != null) {
      return result;
    }
    return endpointSelectStrategy.selectEndpoint(userInfo, defQueueEndpoints);
  }

  private static class ServerFailure {
    public String serverUrl;
    private long lastCheckFailureTime;
    private AtomicInteger failCnt = new AtomicInteger(1);

    public ServerFailure(String serverUrl) {
      this.serverUrl = serverUrl;
    }

    public void incFailCnt() {
      failCnt.incrementAndGet();
    }

    public void updateLastCheckTime() {
      this.lastCheckFailureTime = System.currentTimeMillis();
    }
  }

  public interface QueueSelectStrategy {
    /**
     * @param user
     * @param queues
     * @return null if no queue can be selected
     */
    String selectQueue(String user, List<String> queues);
  }

  public class FirstQueueSelectStrategy implements QueueSelectStrategy {

    @Override
    public String selectQueue(String user, List<String> queues) {
      return queues.get(0);
    }
  }

  public interface EndpointSelectStrategy {
    /**
     * @param userInfo
     * @param endpoints
     * @return null if no server can be selected
     */
    SparkEndpoint selectEndpoint(UserInfo userInfo, List<SparkEndpoint> endpoints);
  }

  public class RandomSelectStrategy implements EndpointSelectStrategy {
    private Random rand;

    public RandomSelectStrategy() {
      rand = new Random();
    }

    @Override
    public SparkEndpoint selectEndpoint(UserInfo userInfo, List<SparkEndpoint> endpoints) {
      int size = endpoints.size();
      if (size == 0) {
        return null;
      }
      if (size == 1) {
        return endpoints.get(0);
      }
      Collections.sort(endpoints);

      SparkEndpoint endpoint = endpoints.get(Math.abs(userInfo.username.hashCode()) % size);
      if (checkEndpoint(endpoint)) {
        return endpoint;
      }
      int maxSelectTime = size * 3;
      int select = 0;
      while (select < maxSelectTime) {
        endpoint = endpoints.get(rand.nextInt(size));
        if (checkEndpoint(endpoint)) {
          return endpoint;
        }
        select++;
      }
      return null;
    }
  }

  public class TagBasedSelectStrategy implements EndpointSelectStrategy {
    EndpointSelectStrategy backSelectionStrategy;

    public TagBasedSelectStrategy() {
      backSelectionStrategy = new RandomSelectStrategy();
    }

    @Override
    public SparkEndpoint selectEndpoint(UserInfo userInfo, List<SparkEndpoint> endpoints) {
      int size = endpoints.size();
      if (size == 0) {
        return null;
      }
      if (size == 1) {
        return endpoints.get(0);
      }

      List<String> sessionTags = userInfo.getTags();
      // for no tag session, first select endpoint which has no tags
      // if all endpoints has tag, the select one randomly
      if (sessionTags.isEmpty()) {
        List<SparkEndpoint> noTagEndpoints =
            endpoints.stream()
                .filter(endpoint -> !endpoint.getServerInfo().hasTags())
                .collect(Collectors.toList());
        if (!noTagEndpoints.isEmpty()) {
          return backSelectionStrategy.selectEndpoint(userInfo, noTagEndpoints);
        } else {
          return backSelectionStrategy.selectEndpoint(userInfo, endpoints);
        }
      }

      int maxTagMatchCnt = 0;
      List<SparkEndpoint> bestMatchEndPoints = new ArrayList<>();
      for (SparkEndpoint endpoint : endpoints) {
        int matchTagCnt = endpoint.getServerInfo().matchTags(sessionTags);
        if (matchTagCnt > maxTagMatchCnt) {
          maxTagMatchCnt = matchTagCnt;
          bestMatchEndPoints = new ArrayList<>();
          bestMatchEndPoints.add(endpoint);
        }
        if (matchTagCnt == maxTagMatchCnt) {
          bestMatchEndPoints.add(endpoint);
        }
      }
      return backSelectionStrategy.selectEndpoint(userInfo, bestMatchEndPoints);
    }
  }

  private boolean checkEndpoint(SparkEndpoint endpoint) {
    String url = endpoint.getServerUrl();
    ServerFailure failure = failServers.get(url);
    if (failure == null) {
      return true;
    }
    long currTime = System.currentTimeMillis();
    // recheck the fail server every 5 minutes
    if ((currTime - failure.lastCheckFailureTime) > 5 * 60 * 1000) {
      return true;
    }
    return false;
  }
}
