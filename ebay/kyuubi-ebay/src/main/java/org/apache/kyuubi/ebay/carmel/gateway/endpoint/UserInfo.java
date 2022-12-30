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

import com.google.common.base.MoreObjects;
import java.util.Objects;

public class UserInfo {
  /** the real user used to connect to backend */
  public final String username;

  public final String password;
  private String assignedQueue;

  private int sessionIdleTimeout = 0;
  /** the original user when using impersonation */
  private final String originUser;

  public UserInfo(String username, String password) {
    this(username, username, password);
  }

  public UserInfo(String originUser, String username, String password) {
    this.username = username;
    this.password = password;
    this.originUser = originUser;
  }

  public String getUsername() {
    return this.username;
  }

  public String getPassword() {
    return this.password;
  }

  public String getAssignedQueue() {
    return assignedQueue;
  }

  public void setAssignedQueue(String assignedQueue) {
    this.assignedQueue = assignedQueue;
  }

  public int getSessionIdleTimeout() {
    return sessionIdleTimeout;
  }

  public void setSessionIdleTimeout(int sessionIdleTimeout) {
    this.sessionIdleTimeout = sessionIdleTimeout;
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.username, this.password);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }

    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }

    UserInfo that = (UserInfo) obj;
    return Objects.equals(this.username, that.username)
        && Objects.equals(this.password, that.password);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("username", this.username)
        .add("assignQueue", this.assignedQueue)
        .add("sessionIdleTimeout", this.sessionIdleTimeout)
        .toString();
  }
}
