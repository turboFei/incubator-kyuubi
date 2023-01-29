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
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FileThriftDiscovery implements ThriftDiscovery {

  private final Map<String, String> serverUrl = new HashMap<>();
  private final ObjectMapper mapper = new ObjectMapper();

  public FileThriftDiscovery(String path) throws IOException {
    List<ServerUrl> list = getAllServerUrls(path);
    for (ServerUrl url : list) {
      if (Strings.isNullOrEmpty(url.grouppath) || Strings.isNullOrEmpty(url.url)) {
        continue;
      }
      this.serverUrl.put(url.grouppath, url.url);
    }
  }

  private List<ServerUrl> getAllServerUrls(String path) throws IOException {
    CollectionType javaType =
        mapper.getTypeFactory().constructCollectionType(List.class, ServerUrl.class);
    if (!path.contains("/")) {
      ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
      if (classLoader == null) {
        classLoader = FileThriftDiscovery.class.getClassLoader();
      }
      return mapper.readValue(classLoader.getResourceAsStream(path), javaType);
    }
    return mapper.readValue(Files.readAllBytes(Paths.get(path)), javaType);
  }

  @Override
  public List<String> getServerUrls(String queue) throws Exception {
    return Lists.newArrayList(this.serverUrl.values());
  }

  @Override
  public void invalidate(String key) {}

  private static class ServerUrl {

    private String grouppath;
    private String url;

    public ServerUrl() {}

    public ServerUrl(String grouppath, String url) {
      this.grouppath = grouppath;
      this.url = url;
    }

    public String getGrouppath() {
      return grouppath;
    }

    public void setGrouppath(String grouppath) {
      this.grouppath = grouppath;
    }

    public String getUrl() {
      return url;
    }

    public void setUrl(String url) {
      this.url = url;
    }
  }
}
