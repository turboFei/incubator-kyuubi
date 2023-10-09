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

package org.apache.kyuubi.client;

import java.util.*;
import org.apache.kyuubi.client.api.v1.dto.Engine;
import org.apache.kyuubi.client.api.v1.dto.OperationData;
import org.apache.kyuubi.client.api.v1.dto.ServerData;
import org.apache.kyuubi.client.api.v1.dto.SessionData;

public class AdminRestApi {
  private KyuubiRestClient client;

  private static final String API_BASE_PATH = "admin";

  private AdminRestApi() {}

  public AdminRestApi(KyuubiRestClient client) {
    this.client = client;
  }

  public String refreshHadoopConf() {
    String path = String.format("%s/%s", API_BASE_PATH, "refresh/hadoop_conf");
    return this.getClient().post(path, null, client.getAuthHeader());
  }

  public String refreshUserDefaultsConf() {
    String path = String.format("%s/%s", API_BASE_PATH, "refresh/user_defaults_conf");
    return this.getClient().post(path, null, client.getAuthHeader());
  }

  public String refreshKubernetesConf() {
    String path = String.format("%s/%s", API_BASE_PATH, "refresh/kubernetes_conf");
    return this.getClient().post(path, null, client.getAuthHeader());
  }

  public String refreshUnlimitedUsers() {
    String path = String.format("%s/%s", API_BASE_PATH, "refresh/unlimited_users");
    return this.getClient().post(path, null, client.getAuthHeader());
  }

  public String refreshDenyUsers() {
    String path = String.format("%s/%s", API_BASE_PATH, "refresh/deny_users");
    return this.getClient().post(path, null, client.getAuthHeader());
  }

  public String deleteEngine(
      String engineType, String shareLevel, String subdomain, String hs2ProxyUser, String cluster) {
    Map<String, Object> params = new HashMap<>();
    params.put("type", engineType);
    params.put("sharelevel", shareLevel);
    params.put("subdomain", subdomain);
    params.put("hive.server2.proxy.user", hs2ProxyUser);
    params.put("cluster", cluster);
    return this.getClient().delete(API_BASE_PATH + "/engine", params, client.getAuthHeader());
  }

  public List<Engine> listEngines(
      String engineType,
      String shareLevel,
      String subdomain,
      String hs2ProxyUser,
      String all,
      String cluster) {
    Map<String, Object> params = new HashMap<>();
    params.put("type", engineType);
    params.put("sharelevel", shareLevel);
    params.put("subdomain", subdomain);
    params.put("hive.server2.proxy.user", hs2ProxyUser);
    params.put("all", all);
    params.put("cluster", cluster);
    Engine[] result =
        this.getClient()
            .get(API_BASE_PATH + "/engine", params, Engine[].class, client.getAuthHeader());
    return Arrays.asList(result);
  }

  public List<SessionData> listSessions() {
    return listSessions(Collections.emptyList());
  }

  public List<SessionData> listSessions(List<String> users) {
    Map<String, Object> params = new HashMap<>();
    if (users != null && !users.isEmpty()) {
      params.put("users", String.join(",", users));
    }
    SessionData[] result =
        this.getClient()
            .get(API_BASE_PATH + "/sessions", params, SessionData[].class, client.getAuthHeader());
    return Arrays.asList(result);
  }

  public String closeSession(String sessionHandleStr) {
    String url = String.format("%s/sessions/%s", API_BASE_PATH, sessionHandleStr);
    return this.getClient().delete(url, null, client.getAuthHeader());
  }

  public List<OperationData> listOperations() {
    OperationData[] result =
        this.getClient()
            .get(
                API_BASE_PATH + "/operations", null, OperationData[].class, client.getAuthHeader());
    return Arrays.asList(result);
  }

  public String closeOperation(String operationHandleStr) {
    String url = String.format("%s/operations/%s", API_BASE_PATH, operationHandleStr);
    return this.getClient().delete(url, null, client.getAuthHeader());
  }

  public List<ServerData> listServers() {
    ServerData[] result =
        this.getClient()
            .get(API_BASE_PATH + "/server", null, ServerData[].class, client.getAuthHeader());
    return Arrays.asList(result);
  }

  private IRestClient getClient() {
    return this.client.getHttpClient();
  }
}
