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
package org.apache.kyuubi.ctl.cmd.submit

import org.apache.kyuubi.client.api.v1.dto.Batch
import org.apache.kyuubi.client.util.JsonUtil
import org.apache.kyuubi.ctl.{BatchOpts, CliConfig, ControlCliException, LogOpts}
import org.apache.kyuubi.ctl.cmd.Command
import org.apache.kyuubi.ctl.cmd.create.CreateBatchCommand
import org.apache.kyuubi.ctl.cmd.log.LogBatchCommand
import org.apache.kyuubi.ctl.util.{BatchUtil, CtlUtils, Render, Validator}

class SubmitBatchCommand(cliConfig: CliConfig) extends Command[Batch](cliConfig) {

  def validate(): Unit = {
    Validator.validateFilename(normalizedCliConfig)
  }

  def doRun(): Batch = {
    val map = CtlUtils.loadYamlAsMap(normalizedCliConfig)

    val createBatchCommand = new CreateBatchCommand(normalizedCliConfig)
    val batchId = createBatchCommand.doRun().getId

    val logBatchCommand = new LogBatchCommand(
      normalizedCliConfig.copy(
        batchOpts = BatchOpts(batchId = batchId),
        logOpts = LogOpts(forward = true)),
      map)
    val batch = logBatchCommand.doRun()

    if (BatchUtil.isTerminalState(batch.getState) && !BatchUtil.isFinishedState(batch.getState)) {
      error(s"Batch ${batch.getId} failed: ${JsonUtil.toJson(batch)}")
      throw ControlCliException(1)
    }

    batch
  }

  def render(batch: Batch): Unit = {
    info(Render.renderBatchInfo(batch))
  }
}
