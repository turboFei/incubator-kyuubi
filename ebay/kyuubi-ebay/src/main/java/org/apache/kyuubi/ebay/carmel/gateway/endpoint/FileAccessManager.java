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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionType;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FileAccessManager implements AccessManager {

  private final Map<String, QueueInfo> userQueue = new HashMap<>();
  private final ObjectMapper mapper = new ObjectMapper();

  public FileAccessManager(String path) {
    List<UserInfo> list = getUserInfo(path);
    for (FileAccessManager.UserInfo userInfo : list) {
      if (Strings.isNullOrEmpty(userInfo.username) || Strings.isNullOrEmpty(userInfo.queue)) {
        continue;
      }
      this.userQueue.put(userInfo.username, new QueueInfo(userInfo.queue));
    }
  }

  @Override
  public List<QueueInfo> getUserQueues(String user) {
    return Lists.newArrayList(userQueue.get(user));
  }

  @Override
  public List<String> getBatchAccountsForServiceAccount(String serviceAccount) throws Exception {
    throw new Exception("Method not implement in FileAccessManager");
  }

  @Override
  public Set<String> getGroupUsers(String groupName) throws Exception {
    throw new UnsupportedOperationException("Method not implement in FileAccessManager");
  }

  private List<FileAccessManager.UserInfo> getUserInfo(String path) {
    try {
      CollectionType javaType =
          mapper
              .getTypeFactory()
              .constructCollectionType(List.class, FileAccessManager.UserInfo.class);
      if (!path.contains("/")) {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader == null) {
          classLoader = FileAccessManager.class.getClassLoader();
        }
        return mapper.readValue(classLoader.getResourceAsStream(path), javaType);
      }
      return mapper.readValue(Files.readAllBytes(Paths.get(path)), javaType);
    } catch (Exception e) {
      throw new CarmelRuntimeException("error when get user info", e);
    }
  }

  @Override
  public void invalidate(String key) {}

  private static class UserInfo {

    private String username;
    private String queue;

    public UserInfo(String username, String queue) {
      this.username = username;
      this.queue = queue;
    }

    public UserInfo() {}

    public String getUsername() {
      return username;
    }

    public void setUsername(String username) {
      this.username = username;
    }

    public String getQueue() {
      return queue;
    }

    public void setQueue(String queue) {
      this.queue = queue;
    }
  }
}
