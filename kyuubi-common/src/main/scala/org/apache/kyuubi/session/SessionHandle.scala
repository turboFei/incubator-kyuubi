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

package org.apache.kyuubi.session

import java.util.{Objects, UUID}

import org.apache.hive.service.rpc.thrift.{TProtocolVersion, TSessionHandle}

import org.apache.kyuubi.cli.Handle

case class SessionHandle(identifier: UUID, protocol: TProtocolVersion) extends Handle {

  def toTSessionHandle: TSessionHandle = {
    val tSessionHandle = new TSessionHandle
    tSessionHandle.setSessionId(Handle.toTHandleIdentifier(identifier))
    tSessionHandle
  }

  override def hashCode(): Int = identifier.hashCode() + 31

  override def equals(obj: Any): Boolean = obj match {
    case SessionHandle(id, _) => Objects.equals(this.identifier, id)
    case _ => false
  }

  override def toString: String = s"SessionHandle [$identifier]"
}

object SessionHandle {
  def apply(tHandle: TSessionHandle, protocol: TProtocolVersion): SessionHandle = {
    apply(Handle.fromTHandleIdentifier(tHandle.getSessionId), protocol)
  }

  def apply(tHandle: TSessionHandle): SessionHandle = {
    apply(tHandle, TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V1)
  }

  def apply(protocol: TProtocolVersion): SessionHandle = {
    new SessionHandle(UUID.randomUUID(), protocol)
  }

  def fromUUID(uuid: String): SessionHandle = {
    val id = UUID.fromString(uuid)
    new SessionHandle(id, TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V1)
  }
}
