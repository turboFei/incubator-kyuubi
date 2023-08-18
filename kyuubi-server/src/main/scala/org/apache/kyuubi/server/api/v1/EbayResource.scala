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

package org.apache.kyuubi.server.api.v1

import javax.ws.rs.{GET, NotAllowedException, Path, Produces, QueryParam}
import javax.ws.rs.core.MediaType

import scala.collection.JavaConverters._

import io.swagger.v3.oas.annotations.media.{ArraySchema, Content, Schema}
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.tags.Tag
import org.apache.hadoop.security.UserGroupInformation

import org.apache.kyuubi.{Logging, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.server.api.ApiRequestContext

@Tag(name = "Ebay")
@Produces(Array(MediaType.APPLICATION_JSON))
private[v1] class EbayResource extends ApiRequestContext with Logging {
  private lazy val administrators = fe.getConf.get(KyuubiConf.SERVER_ADMINISTRATORS).toSet +
    Utils.currentUser

  private def isAdministrator(userName: String): Boolean = {
    administrators.contains(userName)
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      array = new ArraySchema(schema = new Schema(implementation = classOf[String])))),
    description = "get the groups")
  @GET
  @Path("groups")
  def groups(@QueryParam("user") user: String): Seq[String] = {
    val userName = fe.getSessionUser(Map.empty[String, String])
    val ipAddress = fe.getIpAddress
    info(s"Received listing all live sessions request from $userName/$ipAddress")
    if (!isAdministrator(userName)) {
      throw new NotAllowedException(
        s"$userName is not allowed to list all live sessions")
    }
    UserGroupInformation.createRemoteUser(user).getGroups.asScala
  }
}
