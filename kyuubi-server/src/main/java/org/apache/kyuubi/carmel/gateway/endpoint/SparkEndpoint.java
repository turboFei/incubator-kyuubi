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

package org.apache.kyuubi.carmel.gateway.endpoint;

import static org.apache.kyuubi.ebay.carmel.gateway.config.CarmelConfig.ConfVars.*;

import java.net.Socket;
import org.apache.kyuubi.client.KyuubiSyncThriftClient;
import org.apache.kyuubi.ebay.carmel.gateway.config.CarmelConfig;
import org.apache.kyuubi.ebay.carmel.gateway.endpoint.CarmelRuntimeException;
import org.apache.kyuubi.ebay.carmel.gateway.endpoint.QueueInfo;
import org.apache.kyuubi.ebay.carmel.gateway.endpoint.UserInfo;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkEndpoint implements Comparable<SparkEndpoint> {
  private static final Logger LOG = LoggerFactory.getLogger(SparkEndpoint.class);

  private QueueInfo queue;
  private String serverUrl;
  private UserInfo userInfo;
  private KyuubiSyncThriftClient syncThriftClient;
  private TTransport serviceTransport;
  private int connectTimeoutInMs;
  private String id;

  public SparkEndpoint(CarmelConfig config, QueueInfo queue, String serverUrl, UserInfo userInfo) {
    this.queue = queue;
    this.serverUrl = serverUrl;
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
        String[] hostAndPortArr = serverUrl.split(":");
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
        this.syncThriftClient = KyuubiSyncThriftClient.createClient(tProtocol);
      } catch (Exception e) {
        throw new CarmelRuntimeException("error when open transport:" + serverUrl, e);
      }
    }
    return syncThriftClient;
  }

  public QueueInfo getQueue() {
    return queue;
  }

  public String getServerUrl() {
    return serverUrl;
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
    return serverUrl;
  }

  public void setId(String id) {
    this.id = id;
  }

  @Override
  public String toString() {
    return "SparkEndpoint{"
        + "queue='"
        + queue
        + '\''
        + ", serverUrl='"
        + serverUrl
        + '\''
        + ", id='"
        + id
        + '\''
        + '}';
  }

  @Override
  public int compareTo(SparkEndpoint o) {
    if (!queue.getName().equals(o.queue.getName())) {
      return queue.getName().compareTo(o.queue.getName());
    }
    return this.serverUrl.compareTo(o.serverUrl);
  }
}
