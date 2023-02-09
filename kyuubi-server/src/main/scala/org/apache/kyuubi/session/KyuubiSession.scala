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
import org.apache.hive.service.rpc.thrift.TProtocolVersion

import org.apache.kyuubi.carmel.gateway.session.CarmelSessionImpl
import org.apache.kyuubi.config.KyuubiConf.SESSION_NAME
import org.apache.kyuubi.config.KyuubiReservedKeys.{KYUUBI_SESSION_CONNECTION_URL_KEY, KYUUBI_SESSION_REAL_USER_KEY}
import org.apache.kyuubi.events.KyuubiSessionEvent
import org.apache.kyuubi.metrics.MetricsConstants.{CONN_OPEN, CONN_TOTAL}
import org.apache.kyuubi.metrics.MetricsSystem
import org.apache.kyuubi.session.SessionType.SessionType

abstract class KyuubiSession(
    protocol: TProtocolVersion,
    user: String,
    password: String,
    ipAddress: String,
    conf: Map[String, String],
    sessionManager: KyuubiSessionManager)
  extends AbstractSession(protocol, user, password, ipAddress, conf, sessionManager) {

  val sessionType: SessionType

  val connectionUrl = conf.get(KYUUBI_SESSION_CONNECTION_URL_KEY).getOrElse("")

  val realUser = conf.get(KYUUBI_SESSION_REAL_USER_KEY).getOrElse(user)

  val sessionCluster: Option[String]
  def sessionQueue: Option[String] = None
  val sessionTag: Option[String]

  def defaultName: Option[String] = sessionCluster.map(c => s"$c ${getClass.getSimpleName}")
  override lazy val name: Option[String] = normalizedConf.get(SESSION_NAME.key).orElse(defaultName)

  def getSessionEvent: Option[KyuubiSessionEvent]

  def checkSessionAccessPathURIs(): Unit

  private[kyuubi] def handleSessionException(f: => Unit): Unit = {
    try {
      f
    } catch {
      case t: Throwable =>
        getSessionEvent.foreach(_.exception = Some(t))
        throw t
    }
  }

  protected def traceMetricsOnOpen(): Unit = MetricsSystem.tracing { ms =>
    ms.incCount(CONN_TOTAL)
    ms.incCount(MetricRegistry.name(CONN_TOTAL, sessionType.toString))
    ms.incCount(MetricRegistry.name(CONN_OPEN, user))
    ms.incCount(MetricRegistry.name(CONN_OPEN, sessionType.toString))

    sessionCluster.foreach { cluster =>
      ms.incCount(MetricRegistry.name(CONN_TOTAL, cluster))
      ms.incCount(MetricRegistry.name(CONN_TOTAL, cluster, user))
      ms.incCount(MetricRegistry.name(CONN_OPEN, cluster))
      ms.incCount(MetricRegistry.name(CONN_OPEN, cluster, user))
      if (isInstanceOf[CarmelSessionImpl]) {
        asInstanceOf[CarmelSessionImpl].sessionQueue.foreach { queue =>
          ms.incCount(MetricRegistry.name(CONN_OPEN, cluster, queue))
        }
      }
    }
  }

  protected def traceMetricsOnClose(): Unit = MetricsSystem.tracing { ms =>
    ms.decCount(MetricRegistry.name(CONN_OPEN, user))
    ms.decCount(MetricRegistry.name(CONN_OPEN, sessionType.toString))

    sessionCluster.foreach { cluster =>
      ms.decCount(MetricRegistry.name(CONN_OPEN, cluster))
      ms.decCount(MetricRegistry.name(CONN_OPEN, cluster, user))
      if (isInstanceOf[CarmelSessionImpl]) {
        asInstanceOf[CarmelSessionImpl].sessionQueue.foreach { queue =>
          ms.decCount(MetricRegistry.name(CONN_OPEN, cluster, queue))
        }
      }
    }
  }
}
