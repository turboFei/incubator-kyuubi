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
package org.apache.kyuubi.engine.deploy.yarn

import java.io.File

import org.apache.hadoop.fs.Path
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import org.apache.kyuubi.{KyuubiFunSuite, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.deploy.yarn.EngineYarnModeSubmitter.KYUUBI_ENGINE_DEPLOY_YARN_MODE_JARS_KEY

class EngineYarnModeSubmitterSuite extends KyuubiFunSuite with Matchers {

  val kyuubiHome: String = Utils.getCodeSourceLocation(getClass).split("kyuubi-common").head

  test("Classpath should contain engine jars dir and conf dir") {
    val kyuubiConf = new KyuubiConf()
      .set(KYUUBI_ENGINE_DEPLOY_YARN_MODE_JARS_KEY, "mock.jar")

    val env = MockEngineYarnModeSubmitter.setupLaunchEnv(kyuubiConf)
    assert(env.contains(Environment.HADOOP_CONF_DIR.name()))

    val cp = env("CLASSPATH").split(":|;|<CPS>")

    assert(cp.length == 2)
    cp should contain("{{PWD}}/__kyuubi_engine_conf__")
    cp should contain("{{PWD}}/__kyuubi_engine_conf__/__hadoop_conf__")
  }

  test("container env should contain engine env") {
    val kyuubiConf = new KyuubiConf()
      .set(s"${KyuubiConf.KYUUBI_ENGINE_YARN_MODE_ENV_PREFIX}.KYUUBI_HOME", kyuubiHome)

    val env = MockEngineYarnModeSubmitter.setupLaunchEnv(kyuubiConf)
    assert(env.nonEmpty)
    assert(env.contains("KYUUBI_HOME"))
    assert(env("KYUUBI_HOME") == kyuubiHome)
  }

  test("distinct archive files") {
    val targetJars: String = s"${Utils.getCodeSourceLocation(getClass)}"
    // double the jars to make sure the distinct works
    val archives = s"$targetJars,$targetJars"
    val files = MockEngineYarnModeSubmitter.listDistinctFiles(archives)
    val targetFiles = Utils.listFilesRecursively(new File(targetJars))
    assert(targetFiles != null)
    assert(targetFiles.length == files.length)
  }

}

object MockEngineYarnModeSubmitter extends EngineYarnModeSubmitter {
  override var engineType: String = "mock"

  stagingDirPath = new Path("target/test-staging-dir")

  override def engineMainClass(): String = "org.apache.kyuubi.engine.deploy.Mock"
}
