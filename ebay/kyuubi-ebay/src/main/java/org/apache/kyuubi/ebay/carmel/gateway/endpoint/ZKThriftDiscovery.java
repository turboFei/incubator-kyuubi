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

import static org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent.Type.CHILD_ADDED;
import static org.apache.kyuubi.ebay.carmel.gateway.config.CarmelConfig.ConfVars.CARMEL_COMMON_ZK_QUORUM;
import static org.apache.kyuubi.ebay.carmel.gateway.config.CarmelConfig.ConfVars.CARMEL_GATEWAY_THRIFT_ZOOKEEPER_NAMESPACE;

import com.google.common.cache.*;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.ZKPaths;
import org.apache.kyuubi.config.KyuubiConf;
import org.apache.kyuubi.ebay.carmel.gateway.config.CarmelConfig;
import org.apache.kyuubi.ebay.carmel.gateway.tools.JsonUtils;
import org.apache.kyuubi.ha.HighAvailabilityConf;
import org.apache.kyuubi.ha.client.zookeeper.ZookeeperClientProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZKThriftDiscovery implements ThriftDiscovery {
  private static final Logger LOG = LoggerFactory.getLogger(ZKThriftDiscovery.class);

  private final LoadingCache<String, Set<ThriftServerInfo>> cache;
  private final CuratorFramework zooKeeperClient;
  private final String zkBasePath;
  private final ConcurrentMap<String, PathChildrenCache> watches = new ConcurrentHashMap<>();

  public ZKThriftDiscovery(CarmelConfig config) throws Exception {
    String carmelZookeeperQuorum = config.getVar(CARMEL_COMMON_ZK_QUORUM);
    if (StringUtils.isBlank(carmelZookeeperQuorum)) {
      throw new CarmelRuntimeException("The carmel zookeeper quorum is not specified.");
    }
    // using carmel zk quorum and reuse Kyuubi common code to get zk client
    KyuubiConf kyuubiConf = new KyuubiConf(true);
    kyuubiConf.set(HighAvailabilityConf.HA_ADDRESSES(), carmelZookeeperQuorum);

    this.zooKeeperClient = ZookeeperClientProvider.buildZookeeperClient(kyuubiConf);
    zooKeeperClient.start();
    this.zkBasePath = config.getVar(CARMEL_GATEWAY_THRIFT_ZOOKEEPER_NAMESPACE);
    this.cache =
        CacheBuilder.newBuilder()
            .expireAfterWrite(2, TimeUnit.HOURS)
            .removalListener(
                new RemovalListener<String, Set<ThriftServerInfo>>() {
                  @Override
                  public void onRemoval(
                      RemovalNotification<String, Set<ThriftServerInfo>> notification) {
                    PathChildrenCache watch = watches.remove(notification.getKey());
                    if (watch != null) {
                      try {
                        watch.close();
                      } catch (Exception e) {
                        LOG.error("error when close watch:" + notification.getKey(), e);
                      }
                    }
                  }
                })
            .build(
                new CacheLoader<String, Set<ThriftServerInfo>>() {
                  @Override
                  public Set<ThriftServerInfo> load(String key) throws Exception {
                    return getZNodeData(key);
                  }
                });
  }

  private Set<ThriftServerInfo> getZNodeData(String queue) throws Exception {
    String zkPath = ZKPaths.makePath(zkBasePath, queue);
    addWatchForQueue(queue, zkPath);
    List<String> nodes = zooKeeperClient.getChildren().forPath(zkPath);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Get server url from path " + zkPath + " and server urls: " + nodes);
    }
    Set<ThriftServerInfo> result = Sets.newHashSet();
    for (String node : nodes) {
      result.add(parseServerInfo(ZKPaths.makePath(zkPath, node)));
    }
    return result;
  }

  private ThriftServerInfo parseServerInfo(String zkNodeName) throws Exception {
    String serverUrl = zkNodeName.substring(zkNodeName.indexOf(":") + 1);
    ThriftServerInfo serverInfo = new ThriftServerInfo(serverUrl);
    try {
      byte[] serverDataBytes = zooKeeperClient.getData().forPath(zkNodeName);
      if (serverDataBytes != null && serverDataBytes.length > 0) {
        ThriftServerInfo.ServerData serverData =
            JsonUtils.readValue(serverDataBytes, ThriftServerInfo.ServerData.class);
        if (serverData != null) {
          serverInfo.setServerData(serverData);
        }
      }
    } catch (Exception e) {
      LOG.error("error when get thrift server data from zk node:" + zkNodeName, e);
    }
    return serverInfo;
  }

  private String parseServerUrl(String zkNodeName) {
    return zkNodeName.substring(zkNodeName.indexOf(":") + 1);
  }

  private void addWatchForQueue(String queue, String zkPath) {
    PathChildrenCache childrenCache = watches.get(queue);
    if (childrenCache == null) {
      childrenCache = new PathChildrenCache(zooKeeperClient, zkPath, false);
      PathChildrenCache prev = watches.putIfAbsent(queue, childrenCache);
      if (prev != null) {
        return;
      }
      childrenCache
          .getListenable()
          .addListener(
              new PathChildrenCacheListener() {
                @Override
                public void childEvent(CuratorFramework client, PathChildrenCacheEvent event)
                    throws Exception {
                  if (event.getType() == CHILD_ADDED) {
                    try {
                      String addNodeName = event.getData().getPath();
                      LOG.info("zk discover new node added:{}", addNodeName);
                      cache.get(queue).add(parseServerInfo(addNodeName));
                    } catch (Exception e) {
                      LOG.error("error when add new thrift server", e);
                    }
                  }
                }
              });
      try {
        childrenCache.start();
      } catch (Exception e) {
        LOG.error("error when start watch", e);
      }
    }
  }

  @Override
  public List<ThriftServerInfo> getServers(String queue) throws Exception {
    Set<ThriftServerInfo> cachedServers = cache.get(queue);
    if (cachedServers == null) {
      return Collections.emptyList();
    }
    return Lists.newArrayList(cachedServers);
  }

  @Override
  public void invalidate(String key) {
    this.cache.invalidate(key);
  }

  @Override
  public void close() throws Exception {
    if (zooKeeperClient != null) {
      zooKeeperClient.close();
    }
  }
}
