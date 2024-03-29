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

import javax.ws.rs.{DELETE, GET, NotAllowedException, NotFoundException, Path, Produces, QueryParam}
import javax.ws.rs.core.{MediaType, Response}

import scala.collection.JavaConverters._

import io.swagger.v3.oas.annotations.media.{ArraySchema, Content, Schema}
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.tags.Tag
import org.apache.hadoop.security.UserGroupInformation

import org.apache.kyuubi.{KYUUBI_VERSION, Logging, Utils}
import org.apache.kyuubi.config.{KyuubiConf, KyuubiEbayConf}
import org.apache.kyuubi.engine.{EngineType, ShareLevel}
import org.apache.kyuubi.ha.HighAvailabilityConf
import org.apache.kyuubi.ha.client.DiscoveryClient
import org.apache.kyuubi.ha.client.DiscoveryClientProvider.withDiscoveryClient
import org.apache.kyuubi.server.api.ApiRequestContext
import org.apache.kyuubi.session.KyuubiSessionManager

@Tag(name = "Ebay")
@Produces(Array(MediaType.APPLICATION_JSON))
private[v1] class EbayResource extends ApiRequestContext with Logging {
  private lazy val administrators = fe.getConf.get(KyuubiConf.SERVER_ADMINISTRATORS).toSet +
    Utils.currentUser

  private def checkAdministrator(): Unit = {
    val userName = fe.getSessionUser(Map.empty[String, String])
    if (!administrators.contains(userName)) {
      throw new NotAllowedException(
        s"$userName is not allowed to list all live sessions")
    }
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
    checkAdministrator()
    UserGroupInformation.createRemoteUser(user).getGroups.asScala
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      array = new ArraySchema(schema = new Schema(implementation = classOf[String])))),
    description = "get the users")
  @GET
  @Path("users")
  def sparkUsers(@QueryParam("cluster") cluster: String): Seq[(String, Seq[String])] = {
    checkAdministrator()

    val clusterOptList = Option(cluster).map(c => Seq(Option(c))).getOrElse {
      KyuubiEbayConf.getNonCarmelClusterOptList(fe.getConf)
    }

    clusterOptList.map { clusterOpt =>
      val clusterConf = getClusterConf(clusterOpt)
      val users = withDiscoveryClient(clusterConf) { discoveryClient =>
        getChildren(discoveryClient, sparkSQLUserEngineSpace(clusterConf))
      }
      s"${clusterOpt.orNull}" -> users
    }
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(mediaType = MediaType.APPLICATION_JSON)),
    description = "delete the user spark engines with specified subdomain")
  @DELETE
  @Path("engines")
  def deleteUsersSparkEngine(
      @QueryParam("cluster") cluster: String,
      @QueryParam("users") users: Seq[String],
      @QueryParam("subdomain") subdomain: String = "default"): Response = {
    checkAdministrator()
    val clusterOptList = Option(cluster).map(c => Seq(Option(c))).getOrElse {
      KyuubiEbayConf.getNonCarmelClusterOptList(fe.getConf)
    }

    val responseMessageBuilder = new StringBuilder()
    clusterOptList.foreach { clusterOpt =>
      val clusterConf = getClusterConf(clusterOpt)
      val engineSpace = sparkSQLUserEngineSpace(clusterConf)
      withDiscoveryClient(clusterConf) { discoveryClient =>
        users.foreach { user =>
          val userEngineSpace = s"$engineSpace/$user/$subdomain"
          val engineNodes = getChildren(discoveryClient, userEngineSpace)
          if (engineNodes.isEmpty) {
            responseMessageBuilder.append(s"No engine found in $userEngineSpace\n")
          } else {
            engineNodes.foreach { engine =>
              try {
                discoveryClient.delete(s"$userEngineSpace/$engine")
                responseMessageBuilder.append(s"Deleted $engine from $userEngineSpace\n")
              } catch {
                case e: Throwable =>
                  responseMessageBuilder.append(
                    s"Failed to delete $engine from $userEngineSpace: " +
                      s"${e.getMessage}\n")
              }
            }
          }
        }
      }
    }

    Response.ok(responseMessageBuilder.toString()).build()
  }

  private def getClusterConf(clusterOpt: Option[String]): KyuubiConf = {
    try {
      fe.be.sessionManager.asInstanceOf[KyuubiSessionManager].getClusterConf(
        clusterOpt.map(c => Map(KyuubiEbayConf.SESSION_CLUSTER.key -> c)).getOrElse(Map.empty))
    } catch {
      case _: Throwable =>
        throw new NotFoundException(s"Please specify the correct cluster to access.")
    }
  }

  private def sparkSQLUserEngineSpace(conf: KyuubiConf): String = {
    val serverSpace = conf.get(HighAvailabilityConf.HA_NAMESPACE)
    s"${serverSpace}_${KYUUBI_VERSION}_${ShareLevel.USER}_${EngineType.SPARK_SQL}"
  }

  private def getChildren(discoveryClient: DiscoveryClient, path: String): Seq[String] = {
    try {
      discoveryClient.getChildren(path)
    } catch {
      case _: Throwable => Seq.empty
    }
  }
}
