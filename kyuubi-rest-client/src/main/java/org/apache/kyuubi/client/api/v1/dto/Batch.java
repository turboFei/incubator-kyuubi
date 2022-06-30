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

package org.apache.kyuubi.client.api.v1.dto;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

public class Batch {
  private String id;
  private String user;
  private String batchType;
  private String name;
  private Map<String, String> batchInfo;
  private String kyuubiInstance;
  private String state;
  private long createTime;
  private long endTime;

  public Batch() {}

  public Batch(
      String id,
      String user,
      String batchType,
      String name,
      Map<String, String> batchInfo,
      String kyuubiInstance,
      String state,
      long createTime,
      long endTime) {
    this.id = id;
    this.user = user;
    this.batchType = batchType;
    this.name = name;
    this.batchInfo = batchInfo;
    this.kyuubiInstance = kyuubiInstance;
    this.state = state;
    this.createTime = createTime;
    this.endTime = endTime;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getUser() {
    return user;
  }

  public void setUser(String user) {
    this.user = user;
  }

  public String getBatchType() {
    return batchType;
  }

  public void setBatchType(String batchType) {
    this.batchType = batchType;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public Map<String, String> getBatchInfo() {
    if (null == batchInfo) {
      return Collections.emptyMap();
    }
    return batchInfo;
  }

  public void setBatchInfo(Map<String, String> batchInfo) {
    this.batchInfo = batchInfo;
  }

  public String getKyuubiInstance() {
    return kyuubiInstance;
  }

  public void setKyuubiInstance(String kyuubiInstance) {
    this.kyuubiInstance = kyuubiInstance;
  }

  public String getState() {
    return state;
  }

  public void setState(String state) {
    this.state = state;
  }

  public long getCreateTime() {
    return createTime;
  }

  public void setCreateTime(long createTime) {
    this.createTime = createTime;
  }

  public long getEndTime() {
    return endTime;
  }

  public void setEndTime(long endTime) {
    this.endTime = endTime;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Batch batch = (Batch) o;
    return Objects.equals(getId(), batch.getId());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getId());
  }
}
