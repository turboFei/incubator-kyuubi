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

import java.net.Socket;
import org.apache.kyuubi.client.KyuubiSyncThriftClient;
import org.apache.kyuubi.ebay.carmel.gateway.config.CarmelConfig;
import org.apache.kyuubi.shaded.thrift.protocol.TProtocol;
import org.apache.kyuubi.shaded.thrift.transport.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkEndpoint implements Comparable<SparkEndpoint> {
  private static final Logger LOG = LoggerFactory.getLogger(SparkEndpoint.class);

  private QueueInfo queue;
  private ThriftServerInfo serverInfo;
  private UserInfo userInfo;
  private KyuubiSyncThriftClient syncThriftClient;
  private TTransport serviceTransport;
  private int connectTimeoutInMs;

  private String id;
  private String name;
  private String url;

  public SparkEndpoint(
      CarmelConfig config, QueueInfo queue, ThriftServerInfo serverInfo, UserInfo userInfo) {
    this.queue = queue;
    this.serverInfo = serverInfo;
    this.userInfo = userInfo;
    this.connectTimeoutInMs = config.getIntVar(CARMEL_GATEWAY_TO_SERVER_TIMEOUT_SEC) * 1000;
  }

  public void close() {
    if (serviceTransport != null) {
      LOG.info("close transport: " + getTransportInfo(serviceTransport));
      serviceTransport.close();
    }
  }

  public KyuubiSyncThriftClient getClient() {
    if (syncThriftClient == null) {
      try {
        String[] hostAndPortArr = serverInfo.getServerUrl().split(":");
        if (hostAndPortArr.length != 2) {
          throw new IllegalStateException("The value of server url is not valid.");
        }
        TProtocol tProtocol =
            KyuubiSyncThriftClient.createTProtocol(
                userInfo.username,
                userInfo.password,
                hostAndPortArr[0],
                Integer.parseInt(hostAndPortArr[1]),
                connectTimeoutInMs,
                connectTimeoutInMs);
        this.serviceTransport = tProtocol.getTransport();
        this.syncThriftClient =
            KyuubiSyncThriftClient.createClient(
                hostAndPortArr[0], Integer.parseInt(hostAndPortArr[1]), tProtocol);
      } catch (Exception e) {
        throw new CarmelRuntimeException(
            "error when open transport:" + serverInfo.getServerUrl(), e);
      }
    }
    return syncThriftClient;
  }

  public QueueInfo getQueue() {
    return queue;
  }

  public String getServerUrl() {
    return serverInfo.getServerUrl();
  }

  public ThriftServerInfo getServerInfo() {
    return serverInfo;
  }

  private String getTransportInfo(TTransport transport) {
    try {
      if (transport instanceof TSaslClientTransport) {
        TSaslClientTransport saslTransport = (TSaslClientTransport) transport;
        TSocket underlyTransport = (TSocket) saslTransport.getUnderlyingTransport();
        Socket socket = underlyTransport.getSocket();
        return "from " + socket.getLocalSocketAddress() + " to " + socket.getRemoteSocketAddress();
      }
    } catch (Exception e) {
      LOG.error("exception when get transport info", e);
    }
    return serverInfo.getServerUrl();
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getId() {
    return id;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setUrl(String url) {
    this.url = url;
  }

  @Override
  public String toString() {
    return "SparkEndpoint{"
        + "queue='"
        + queue
        + '\''
        + ", serverUrl='"
        + serverInfo.getServerUrl()
        + '\''
        + ", id='"
        + id
        + '\''
        + ", name='"
        + name
        + '\''
        + ", url='"
        + url
        + '\''
        + '}';
  }

  @Override
  public int compareTo(SparkEndpoint o) {
    if (!queue.getName().equals(o.queue.getName())) {
      return queue.getName().compareTo(o.queue.getName());
    }
    return this.serverInfo.getServerUrl().compareTo(o.getServerUrl());
  }
}
