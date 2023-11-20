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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.*;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.util.*;
import java.util.concurrent.*;
import javax.net.ssl.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.kyuubi.util.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BdpAccessManager implements AccessManager {

  private static final Logger LOG = LoggerFactory.getLogger(BdpAccessManager.class);
  private static final String BTD_CERT = "bdp.vip.ebay.com.cer";

  private final ObjectMapper mapper = new ObjectMapper();
  private final CloseableHttpClient client;
  private final String url;
  private final String clusterName;

  private final LoadingCache<String, AccountMappingCache> batchAccountCache;
  private final LoadingCache<String, GroupUsersCache> groupUsersCache;
  private int cacheUpdateTime;

  private final Map<String, AllUserQueuesData> allUserQueuesCache;
  private final ScheduledExecutorService allUserQueuesCacheScheduler;

  private final ExecutorService asyncUserQueueFetcher;

  public BdpAccessManager(String url, String clusterName) {
    this.client = initHttpClient();
    this.url = url;
    this.clusterName = clusterName;
    this.cacheUpdateTime = 5 * 60 * 1000; // cache time 5 minutes

    this.batchAccountCache =
        CacheBuilder.newBuilder()
            .maximumSize(5000)
            .removalListener(
                new RemovalListener<String, AccountMappingCache>() {
                  @Override
                  public void onRemoval(
                      RemovalNotification<String, AccountMappingCache> notification) {
                    LOG.info(
                        "remove the batch account mapping from cache for account:{}, cause:{}",
                        notification.getKey(),
                        notification.getCause());
                  }
                })
            .build(
                new CacheLoader<String, AccountMappingCache>() {
                  @Override
                  public AccountMappingCache load(String key) throws Exception {
                    return getBatchAccountCache(key);
                  }
                });

    this.groupUsersCache =
        CacheBuilder.newBuilder()
            .maximumSize(5000)
            .removalListener(
                new RemovalListener<String, GroupUsersCache>() {
                  @Override
                  public void onRemoval(RemovalNotification<String, GroupUsersCache> notification) {
                    LOG.info(
                        "remove the group users from cache for account:{}, cause:{}",
                        notification.getKey(),
                        notification.getCause());
                  }
                })
            .build(
                new CacheLoader<String, GroupUsersCache>() {
                  @Override
                  public GroupUsersCache load(String key) throws Exception {
                    return getGroupUsersCache(key);
                  }
                });

    this.allUserQueuesCache = new ConcurrentHashMap<String, AllUserQueuesData>();

    allUserQueuesCacheScheduler =
        ThreadUtils.newDaemonSingleThreadScheduledExecutor(
            "BdpAccessManagerAllUserQueuesCache-%d", true);
    ThreadUtils.scheduleTolerableRunnableWithFixedDelay(
        allUserQueuesCacheScheduler,
        new Runnable() {
          @Override
          public void run() {
            try {
              updateAllUserQueuesCache(clusterName, doGetAllUserQueues(clusterName));
            } catch (Exception e) {
              LOG.error("Failed to update all user queues cache for " + clusterName, e);
            }
          }
        },
        5,
        300,
        TimeUnit.SECONDS);

    asyncUserQueueFetcher =
        ThreadUtils.newDaemonQueuedThreadPool(
            8, 100, 60000, "BdpAccessManagerAsyncUserQueueFetcher-%d");
  }

  private CloseableHttpClient initHttpClient() {
    SSLContext sslContext;
    try {
      SSLContextBuilder builder = new SSLContextBuilder();
      builder.loadTrustMaterial((TrustStrategy) (x509Certificates, s) -> true);
      sslContext = builder.build();
    } catch (Exception e) {
      throw new CarmelRuntimeException("error when load trust material");
    }

    SSLConnectionSocketFactory sslSocketFactory = new SSLConnectionSocketFactory(sslContext);
    return HttpClients.custom().setSSLSocketFactory(sslSocketFactory).build();
  }

  private X509TrustManager trustManagerForCertificates(InputStream in)
      throws GeneralSecurityException {
    CertificateFactory certificateFactory = CertificateFactory.getInstance("X.509");
    Collection<? extends Certificate> certificates = certificateFactory.generateCertificates(in);
    if (certificates.isEmpty()) {
      throw new IllegalArgumentException("expected non-empty set of trusted certificates");
    }

    // Put the certificates a key store.
    char[] password = "password".toCharArray(); // Any password will work.
    KeyStore keyStore = newEmptyKeyStore(password);
    int index = 0;
    for (Certificate certificate : certificates) {
      String certificateAlias = Integer.toString(index++);
      keyStore.setCertificateEntry(certificateAlias, certificate);
    }

    // Use it to build an X509 trust manager.
    KeyManagerFactory keyManagerFactory =
        KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    keyManagerFactory.init(keyStore, password);
    TrustManagerFactory trustManagerFactory =
        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    trustManagerFactory.init(keyStore);
    TrustManager[] trustManagers = trustManagerFactory.getTrustManagers();
    if (trustManagers.length != 1 || !(trustManagers[0] instanceof X509TrustManager)) {
      throw new IllegalStateException(
          "Unexpected default trust managers:" + Arrays.toString(trustManagers));
    }
    return (X509TrustManager) trustManagers[0];
  }

  private KeyStore newEmptyKeyStore(char[] password) throws GeneralSecurityException {
    try {
      KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
      InputStream in = null; // By convention, 'null' creates an empty key store.
      keyStore.load(in, password);
      return keyStore;
    } catch (IOException e) {
      throw new GeneralSecurityException("Instance new empty keystore error", e);
    }
  }

  @Override
  public List<QueueInfo> getUserQueues(String user) throws Exception {
    AllUserQueuesData allUserQueuesData = allUserQueuesCache.get(clusterName);

    if (isAllUserQueuesCacheUpToDate()) {
      if (allUserQueuesData != null && allUserQueuesData.userQueuesMapping.containsKey(user)) {
        return allUserQueuesData.userQueuesMapping.get(user).queues;
      }
    }

    if (allUserQueuesData == null || !allUserQueuesData.userQueuesMapping.containsKey(user)) {
      List<QueueInfo> res = doGetUserQueues(user);
      updateAllUserQueuesCache(user, res);
      return res;
    } else {
      UserQueuesCache cached = allUserQueuesData.userQueuesMapping.get(user);

      if (!isUserQueuesCacheUpToDate(cached)) {
        doGetUserQueuesAsync(user);
      }

      return cached.queues;
    }
  }

  private Boolean isUserQueuesCacheUpToDate(UserQueuesCache cache) {
    return (System.currentTimeMillis() - cache.cacheTime) < 3600 * 1000;
  }

  private Boolean isAllUserQueuesCacheUpToDate() {
    AllUserQueuesData allUserQueuesData = allUserQueuesCache.get(clusterName);
    return allUserQueuesData != null
        && (System.currentTimeMillis() - allUserQueuesData.cacheTime) < 3600 * 1000;
  }

  private synchronized void updateAllUserQueuesCache(String user, List<QueueInfo> queueInfos) {
    UserQueuesCache userQueuesCache = new UserQueuesCache();
    userQueuesCache.cacheTime = System.currentTimeMillis();
    userQueuesCache.queues = queueInfos;

    if (allUserQueuesCache.get(clusterName) == null) {
      AllUserQueuesData res = new AllUserQueuesData();
      res.userQueuesMapping = new ConcurrentHashMap<>();
      res.cacheTime = -1L;

      allUserQueuesCache.put(clusterName, res);
    }
    allUserQueuesCache.get(clusterName).userQueuesMapping.put(user, userQueuesCache);
  }

  private synchronized void updateAllUserQueuesCache(
      String clusterName, Map<String, UserQueuesCache> userQueuesMapping) {
    AllUserQueuesData res = new AllUserQueuesData();
    res.userQueuesMapping = userQueuesMapping;
    res.cacheTime = System.currentTimeMillis();

    if (res.userQueuesMapping != null && !res.userQueuesMapping.isEmpty()) {
      allUserQueuesCache.put(clusterName, res);
      LOG.info(
          "Update all user queues cache for {} with {} users",
          clusterName,
          res.userQueuesMapping.size());
    }
  }

  private List<QueueInfo> doGetUserQueues(String user) throws Exception {
    LOG.info("Get user queues for {}", user);
    HttpGet httpget = new HttpGet(this.url + "/product/queue/user/" + clusterName + "/" + user);
    httpget.setConfig(getDefaultRequestConfig());
    try (CloseableHttpResponse response = this.client.execute(httpget)) {
      if (response.getStatusLine().getStatusCode() == 200) {
        return parseQueueResult(response.getEntity().getContent());
      }
    }
    throw new Exception("The status of server BDP is invalid");
  }

  private synchronized void doGetUserQueuesAsync(String user) {
    try {
      asyncUserQueueFetcher.submit(
          () -> {
            try {
              updateAllUserQueuesCache(user, doGetUserQueues(user));
            } catch (Throwable t) {
              LOG.error("Failed to get user queues from remote for user " + user, t);
            }
          });
    } catch (RejectedExecutionException e) {
      LOG.warn(
          "asyncUserQueueFetcherQueue is full, ignore the user queues request of user " + user);
    }
  }

  protected List<QueueInfo> parseQueueResult(InputStream inputStream) throws IOException {
    JsonNode node = mapper.readTree(inputStream);
    LOG.info("queue result from BDP: " + node.toString());
    JsonNode successNode = node.get("success");
    boolean success = successNode.asBoolean();
    if (!success) {
      throw new CarmelRuntimeException("BDP return fail result");
    }
    List<QueueInfo> queueList = new ArrayList<>();
    Iterator<JsonNode> iter = node.get("result").iterator();
    while (iter.hasNext()) {
      JsonNode queueNode = iter.next();
      boolean accessible = queueNode.get("accessible").asBoolean();
      if (accessible && queueNode.get("subQueues").isNull()) {
        String queueName = queueNode.get("queueName").asText();
        boolean defaultQueue = queueNode.get("defaultQueue").asBoolean();
        queueList.add(new QueueInfo(queueName, defaultQueue));
      }
    }
    return queueList;
  }

  private Map<String, UserQueuesCache> doGetAllUserQueues(String clusterName) throws Exception {
    HttpGet httpget = new HttpGet(this.url + "/product/userHermes/allUsersQueues");
    int connectTimeout = 60 * 1000;
    int readTimeout = 60 * 1000;
    int connectionRequestTimeout = 180 * 1000;
    httpget.setConfig(getRequestConfig(connectTimeout, readTimeout, connectionRequestTimeout));
    try (CloseableHttpResponse response = this.client.execute(httpget)) {
      if (response.getStatusLine().getStatusCode() == 200) {
        return parseAllQueueResult(response.getEntity().getContent());
      }
    }
    throw new Exception("The status of server BDP is invalid");
  }

  protected Map<String, UserQueuesCache> parseAllQueueResult(InputStream inputStream)
      throws IOException {
    // {"success":true,"result":[{"name":"sohdas","queues":["hdmi-default"]},{"name":"jjestribek","queues":["hdmi-default","hdmi-gmo"]},{"name":"ruirli","queues":["hdmi-gcx","hdmi-default"]}],"message":null}
    JsonNode node = mapper.readTree(inputStream);
    LOG.info("queue result from BDP: " + StringUtils.substring(node.toString(), 0, 100));
    JsonNode successNode = node.get("success");
    boolean success = successNode.asBoolean();
    if (!success) {
      throw new CarmelRuntimeException("BDP return fail result");
    }

    Long current = System.currentTimeMillis();
    Map<String, UserQueuesCache> result = new HashMap<>();
    Iterator<JsonNode> iter = node.get("result").iterator();
    while (iter.hasNext()) {
      JsonNode item = iter.next();
      String username = item.get("name").asText();
      if (!item.get("queues").isNull()) {
        List<QueueInfo> queueList = new ArrayList<>();
        for (JsonNode queue : item.get("queues")) {
          // Set all defaultQueue to false
          queueList.add(new QueueInfo(queue.asText(), false));
        }

        if (!queueList.isEmpty()) {
          UserQueuesCache userQueuesCache = new UserQueuesCache();
          userQueuesCache.cacheTime = current;
          userQueuesCache.queues = queueList;
          result.put(username, userQueuesCache);
        }
      }
    }
    return result;
  }

  @Override
  public List<String> getBatchAccountsForServiceAccount(String serviceAccount) throws Exception {
    AccountMappingCache batchAccountMapping = batchAccountCache.get(serviceAccount);
    if (System.currentTimeMillis() - batchAccountMapping.cacheTime <= cacheUpdateTime) {
      return batchAccountMapping.mapping;
    }
    try {
      batchAccountMapping = getBatchAccountCache(serviceAccount);
      batchAccountCache.put(serviceAccount, batchAccountMapping);
    } catch (Exception e) {
      LOG.error("Fail to update batch account cache for account:" + serviceAccount, e);
    }
    return batchAccountMapping.mapping;
  }

  private AccountMappingCache getBatchAccountCache(String serviceAccount) throws Exception {
    AccountMappingCache mappingCache = new AccountMappingCache();
    mappingCache.mapping = doGetBatchAccountMapping(serviceAccount);
    mappingCache.cacheTime = System.currentTimeMillis();
    return mappingCache;
  }

  private List<String> doGetBatchAccountMapping(String serviceAccount) throws Exception {
    HttpGet httpget =
        new HttpGet(this.url + "/product/batch/" + serviceAccount + "/service/mapping");
    httpget.setConfig(getDefaultRequestConfig());

    String errMsg;
    try (CloseableHttpResponse response = this.client.execute(httpget)) {
      if (response.getStatusLine().getStatusCode() == 200) {
        return parseAccountMappingResult(serviceAccount, response.getEntity().getContent());
      } else {
        errMsg = response.getStatusLine().getReasonPhrase();
      }
    }
    throw new Exception("The status of server BDP is invalid: " + errMsg);
  }

  protected List<String> parseAccountMappingResult(String account, InputStream inputStream)
      throws IOException {
    JsonNode node = mapper.readTree(inputStream);
    LOG.info("Account " + account + " mapping result from BDP: " + node.toString());

    JsonNode successNode = node.get("success");
    boolean success = successNode.asBoolean();
    if (!success) {
      throw new CarmelRuntimeException("BDP return fail result");
    }
    List<String> mapping = new ArrayList<>();

    JsonNode mappingResult = node.get("result");
    if (mappingResult != null && mappingResult.isArray()) {
      for (final JsonNode n : mappingResult) {
        mapping.add(n.asText());
      }
    } else {
      throw new CarmelRuntimeException("mapping result is null or not array");
    }
    return mapping;
  }

  @Override
  public Set<String> getGroupUsers(String groupName) throws Exception {
    GroupUsersCache usersCache = groupUsersCache.get(groupName);
    if (System.currentTimeMillis() - usersCache.cacheTime <= cacheUpdateTime) {
      return usersCache.users;
    }
    try {
      usersCache = getGroupUsersCache(groupName);
      groupUsersCache.put(groupName, usersCache);
    } catch (Exception e) {
      LOG.error("Fail to update group users cache for group:" + groupName, e);
    }
    return usersCache.users;
  }

  private GroupUsersCache getGroupUsersCache(String groupName) throws Exception {
    GroupUsersCache groupUsersCache = new GroupUsersCache();
    groupUsersCache.users = new HashSet<>(doGetGroupUsers(groupName));
    groupUsersCache.cacheTime = System.currentTimeMillis();
    return groupUsersCache;
  }

  private List<String> doGetGroupUsers(String groupName) throws Exception {
    HttpGet httpget = new HttpGet(this.url + "/product/group/show/" + groupName);
    httpget.setConfig(getDefaultRequestConfig());

    String errMsg;
    try (CloseableHttpResponse response = this.client.execute(httpget)) {
      if (response.getStatusLine().getStatusCode() == 200) {
        return parseGroupUsersResult(groupName, response.getEntity().getContent());
      } else {
        errMsg = response.getStatusLine().getReasonPhrase();
      }
    }
    throw new Exception("The status of server BDP is invalid: " + errMsg);
  }

  protected List<String> parseGroupUsersResult(String groupName, InputStream inputStream)
      throws IOException {
    JsonNode node = mapper.readTree(inputStream);
    LOG.info("Group " + groupName + " users result from BDP: " + node.toString());

    JsonNode successNode = node.get("success");
    boolean success = successNode.asBoolean();
    if (!success) {
      throw new CarmelRuntimeException("BDP return fail result for group:" + groupName);
    }
    List<String> users = new ArrayList<>();

    JsonNode result = node.get("result");
    if (result == null || !result.isArray()) {
      throw new CarmelRuntimeException("group users result is null or not an array");
    }
    if (result.size() < 1) {
      return users;
    }
    JsonNode members = result.get(0).get("members");
    if (members == null || !members.isArray()) {
      return users;
    }
    for (final JsonNode n : members) {
      users.add(n.asText());
    }

    return users;
  }

  private RequestConfig getDefaultRequestConfig() {
    int connectTimeout = 60 * 1000;
    int readTimeout = 60 * 1000;
    int connectionRequestTimeout = 60 * 1000;
    return getRequestConfig(connectTimeout, readTimeout, connectionRequestTimeout);
  }

  private RequestConfig getRequestConfig(
      int connectTimeout, int readTimeout, int connectionRequestTimeout) {
    RequestConfig config =
        RequestConfig.custom()
            .setConnectTimeout(connectTimeout)
            .setSocketTimeout(readTimeout)
            .setConnectionRequestTimeout(connectionRequestTimeout)
            .build();
    return config;
  }

  @Override
  public void invalidate(String key) {}

  public static class UserQueuesCache {
    public long cacheTime;
    public List<QueueInfo> queues;
  }

  private static class AllUserQueuesData {
    public long cacheTime;
    public Map<String, UserQueuesCache> userQueuesMapping;
  }

  private static class AccountMappingCache {
    public long cacheTime;
    public List<String> mapping;
  }

  private static class GroupUsersCache {
    public long cacheTime;
    public Set<String> users;
  }
}
