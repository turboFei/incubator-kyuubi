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

package org.apache.kyuubi.server.trino.api

import java.net.URI
import java.security.SecureRandom
import java.util.Objects.requireNonNull
import java.util.UUID
import java.util.concurrent.atomic.AtomicLong
import javax.ws.rs.WebApplicationException
import javax.ws.rs.core.{Response, UriInfo}

import Slug.Context.{EXECUTING_QUERY, QUEUED_QUERY}
import com.google.common.hash.Hashing
import io.trino.client.QueryResults
import org.apache.hive.service.rpc.thrift.TProtocolVersion

import org.apache.kyuubi.operation.{FetchOrientation, OperationHandle}
import org.apache.kyuubi.operation.OperationState.{FINISHED, INITIALIZED, OperationState, PENDING}
import org.apache.kyuubi.service.BackendService
import org.apache.kyuubi.session.SessionHandle

case class Query(
    queryId: QueryId,
    context: TrinoContext,
    be: BackendService) {

  private val QUEUED_QUERY_PATH = "/v1/statement/queued/"
  private val EXECUTING_QUERY_PATH = "/v1/statement/executing"

  private val slug: Slug = Slug.createNewWithUUID(queryId.getQueryId)
  private val lastToken = new AtomicLong

  private val defaultMaxRows = 1000
  private val defaultFetchOrientation = FetchOrientation.withName("FETCH_NEXT")

  def getQueryResults(token: Long, uriInfo: UriInfo, maxWait: Long = 0): QueryResults = {
    val status =
      be.getOperationStatus(queryId.operationHandle, Some(maxWait))
    val nextUri = if (status.exception.isEmpty) {
      getNextUri(token + 1, uriInfo, toSlugContext(status.state))
    } else null
    val queryHtmlUri = uriInfo.getRequestUriBuilder
      .replacePath("ui/query.html").replaceQuery(queryId.getQueryId).build()

    status.state match {
      case FINISHED =>
        val metaData = be.getResultSetMetadata(queryId.operationHandle)
        val resultSet = be.fetchResults(
          queryId.operationHandle,
          defaultFetchOrientation,
          defaultMaxRows,
          false)
        TrinoContext.createQueryResults(
          queryId.getQueryId,
          nextUri,
          queryHtmlUri,
          status,
          Option(metaData),
          Option(resultSet))
      case _ =>
        TrinoContext.createQueryResults(
          queryId.getQueryId,
          nextUri,
          queryHtmlUri,
          status)
    }
  }

  def getLastToken: Long = this.lastToken.get()

  def getSlug: Slug = this.slug

  def cancel: Unit = clear

  private def clear = {
    be.closeOperation(queryId.operationHandle)
    context.session.get("sessionId").foreach { id =>
      be.closeSession(SessionHandle.fromUUID(id))
    }
  }

  private def setToken(token: Long): Unit = {
    val lastToken = this.lastToken.get
    if (token != lastToken && token != lastToken + 1) {
      throw new WebApplicationException(Response.Status.GONE)
    }
    this.lastToken.compareAndSet(lastToken, token)
  }

  private def getNextUri(token: Long, uriInfo: UriInfo, slugContext: Slug.Context.Context): URI = {
    val path = slugContext match {
      case QUEUED_QUERY => QUEUED_QUERY_PATH
      case EXECUTING_QUERY => EXECUTING_QUERY_PATH
    }

    uriInfo.getBaseUriBuilder.replacePath(path)
      .path(queryId.getQueryId)
      .path(slug.makeSlug(slugContext, token))
      .path(String.valueOf(token))
      .replaceQuery("")
      .build()
  }

  private def toSlugContext(state: OperationState): Slug.Context.Context = {
    state match {
      case INITIALIZED | PENDING => Slug.Context.QUEUED_QUERY
      case _ => Slug.Context.EXECUTING_QUERY
    }
  }

}

object Query {

  def apply(
      statement: String,
      context: TrinoContext,
      translator: KyuubiTrinoOperationTranslator,
      backendService: BackendService,
      queryTimeout: Long = 0): Query = {

    val sessionHandle = createSession(context, backendService)
    val operationHandle = translator.transform(
      statement,
      sessionHandle,
      context.session,
      true,
      queryTimeout)
    val newSessionProperties =
      context.session + ("sessionId" -> sessionHandle.identifier.toString)
    val updatedContext = context.copy(session = newSessionProperties)
    Query(QueryId(operationHandle), updatedContext, backendService)
  }

  def apply(id: String, context: TrinoContext, backendService: BackendService): Query = {
    Query(QueryId(id), context, backendService)
  }

  private def createSession(
      context: TrinoContext,
      backendService: BackendService): SessionHandle = {
    backendService.openSession(
      TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V11,
      context.user,
      "",
      context.remoteUserAddress.getOrElse(""),
      context.session)
  }

}

case class QueryId(operationHandle: OperationHandle) {
  def getQueryId: String = operationHandle.identifier.toString
}

object QueryId {
  def apply(id: String): QueryId = QueryId(OperationHandle(id))
}

object Slug {

  object Context extends Enumeration {
    type Context = Value
    val QUEUED_QUERY, EXECUTING_QUERY = Value
  }

  private val RANDOM = new SecureRandom

  def createNew: Slug = {
    val randomBytes = new Array[Byte](16)
    RANDOM.nextBytes(randomBytes)
    new Slug(randomBytes)
  }

  def createNewWithUUID(uuid: String): Slug = {
    val uuidBytes = UUID.fromString(uuid).toString.getBytes("UTF-8")
    new Slug(uuidBytes)
  }
}

case class Slug(slugKey: Array[Byte]) {
  val hmac = Hashing.hmacSha1(requireNonNull(slugKey, "slugKey is null"))

  def makeSlug(context: Slug.Context.Context, token: Long): String = {
    "y" + hmac.newHasher.putInt(context.id).putLong(token).hash.toString
  }

  def isValid(context: Slug.Context.Context, slug: String, token: Long): Boolean =
    makeSlug(context, token) == slug
}
