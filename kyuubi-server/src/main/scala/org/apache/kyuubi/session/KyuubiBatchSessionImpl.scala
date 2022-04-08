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

import com.codahale.metrics.MetricRegistry
import org.apache.hive.service.rpc.thrift._

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.events.{EventBus, KyuubiEvent, KyuubiSessionEvent}
import org.apache.kyuubi.metrics.MetricsConstants._
import org.apache.kyuubi.metrics.MetricsSystem

class KyuubiBatchSessionImpl(
    protocol: TProtocolVersion,
    user: String,
    password: String,
    ipAddress: String,
    conf: Map[String, String],
    sessionManager: KyuubiSessionManager,
    sessionConf: KyuubiConf)
  extends AbstractSession(protocol, user, password, ipAddress, conf, sessionManager) {

  override val normalizedConf: Map[String, String] = sessionManager.validateBatchConfig(conf)

  // TODO: Leverage the session conf advisor

  private[kyuubi] val optimizedConf: Map[String, String] = {
    normalizedConf ++ sessionConf.getBatchConf
  }

  private val sessionEvent = KyuubiSessionEvent(this)
  EventBus.post(sessionEvent)

  override def getSessionEvent: Option[KyuubiEvent] = {
    Option(sessionEvent)
  }


  override def open(): Unit = {
    MetricsSystem.tracing { ms =>
      ms.incCount(CONN_TOTAL)
      ms.incCount(MetricRegistry.name(CONN_OPEN, user))
    }

    // we should call super.open before running launch engine operation
    super.open()

    runOperation()
  }


  override def close(): Unit = {
    super.close()
    sessionEvent.endTime = System.currentTimeMillis()
    EventBus.post(sessionEvent)
    MetricsSystem.tracing(_.decCount(MetricRegistry.name(CONN_OPEN, user)))
  }
}
