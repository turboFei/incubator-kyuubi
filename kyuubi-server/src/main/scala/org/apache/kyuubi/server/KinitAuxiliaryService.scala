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

package org.apache.kyuubi.server

import java.util.concurrent.TimeUnit

import org.apache.hadoop.security.UserGroupInformation

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.service.AbstractService
import org.apache.kyuubi.util.{KyuubiHadoopUtils, ThreadUtils}

class KinitAuxiliaryService() extends AbstractService("KinitAuxiliaryService") {

  private val executor = ThreadUtils.newDaemonSingleThreadScheduledExecutor(getName)

  private var kinitInterval: Long = _
  private var kinitMaxAttempts: Int = _
  @volatile private var kinitAttempts: Int = _

  private var kinitTask: Runnable = _

  override def initialize(conf: KyuubiConf): Unit = {
    if (UserGroupInformation.isSecurityEnabled) {
      val keytab = conf.get(KyuubiConf.SERVER_KEYTAB)
      val principal = conf.get(KyuubiConf.SERVER_PRINCIPAL)
        .map(KyuubiHadoopUtils.getServerPrincipal)
      kinitInterval = conf.get(KyuubiConf.KINIT_INTERVAL)
      kinitMaxAttempts = conf.get(KyuubiConf.KINIT_MAX_ATTEMPTS)

      require(keytab.nonEmpty && principal.nonEmpty, "principal or keytab is missing")
      UserGroupInformation.loginUserFromKeytab(principal.get, keytab.get)
      val krb5Conf = Option(System.getProperty("java.security.krb5.conf"))
        .orElse(Option(System.getenv("KRB5_CONFIG")))
      val commands = Seq("kinit", "-kt", keytab.get, principal.get)
      val kinitProc = new ProcessBuilder(commands: _*).inheritIO()
      krb5Conf.foreach(kinitProc.environment().put("KRB5_CONFIG", _))
      kinitTask = new Runnable {
        override def run(): Unit = {
          val process = kinitProc.start()
          if (process.waitFor() == 0) {
            info(s"Successfully ${commands.mkString(" ")}")
            kinitAttempts = 0
            executor.schedule(this, kinitInterval, TimeUnit.MILLISECONDS)
          } else {
            if (kinitAttempts >= kinitMaxAttempts) {
              error(s"Failed to kinit with $kinitAttempts attempts, will exit...")
              System.exit(-1)
            }
            kinitAttempts += 1
            executor.submit(this)
          }
        }
      }
    }
    super.initialize(conf)
  }

  override def start(): Unit = {
    super.start()
    if (UserGroupInformation.isSecurityEnabled) {
      executor.submit(kinitTask)
    }
  }

  override def stop(): Unit = {
    super.stop()
    executor.shutdown()
    try {
      executor.awaitTermination(10, TimeUnit.SECONDS)
    } catch {
      case _: InterruptedException =>
    }
  }
}
