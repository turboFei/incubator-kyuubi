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

package org.apache.kyuubi.ebay.carmel.gateway.session

import java.util.concurrent.{Callable, Future, LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}

import scala.collection.mutable.ListBuffer

import org.apache.hive.service.rpc.thrift.TGetInfoType

import org.apache.kyuubi.Logging
import org.apache.kyuubi.session.{SessionHandle, SessionManager}

class CarmelSessionStateChecker(sessionMgr: SessionManager) extends Runnable with Logging {
  private val executorService = new ThreadPoolExecutor(
    0,
    20,
    10L,
    TimeUnit.SECONDS,
    new LinkedBlockingQueue[Runnable]())

  override def run(): Unit = {
    try {
      val futures = ListBuffer[Future[(SessionHandle, Boolean)]]()
      sessionMgr.allSessions().foreach {
        case carmelSession: CarmelSessionImpl
            if carmelSession.getSessionEvent.flatMap(_.exception).isEmpty =>
          val future = executorService.submit(new Callable[(SessionHandle, Boolean)] {
            override def call(): (SessionHandle, Boolean) = {
              val sessionHandle = carmelSession.handle
              var attempt = 0
              while (attempt < 3) {
                try {
                  Option(carmelSession.sparkEndpoint).map { endpoint =>
                    endpoint.getClient.getInfo(TGetInfoType.CLI_DBMS_NAME)
                  }
                  return sessionHandle -> true
                } catch {
                  case e: Throwable =>
                    attempt += 1
                    error(
                      s"Failed to check carmel session status $sessionHandle, retry $attempt",
                      e)

                }
              }
              sessionHandle -> false
            }
          })
          futures += future

        case _ =>
      }

      futures.foreach { future =>
        try {
          val (handle, alive) = future.get(60, TimeUnit.SECONDS)
          if (!alive) {
            warn(s"Session $handle becomes inactive")
            val carmelSession = sessionMgr.getSession(handle).asInstanceOf[CarmelSessionImpl]
            carmelSession.setBackendSessionStatus(CarmelSessionStatus.INACTIVE)
            // TODO remove carmel session from session manager if inactive
            // carmelSession.close()
          }
        } catch {
          case e: Throwable => error("Failed to check carmel session status", e)
        }
      }
    } catch {
      case e: Throwable =>
        error("Failed to check backend session status, will retry later", e)
    }
  }
}
