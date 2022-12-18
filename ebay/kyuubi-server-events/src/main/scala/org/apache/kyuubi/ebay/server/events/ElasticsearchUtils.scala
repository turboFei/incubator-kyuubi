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

package org.apache.kyuubi.ebay.server.events

import java.util.{Map => JMap}

import org.apache.http.HttpHost
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.get.GetRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.client.{RequestOptions, RestClient, RestHighLevelClient}
import org.elasticsearch.client.indices.{CreateIndexRequest, GetIndexRequest}
import org.elasticsearch.common.xcontent.XContentType

import org.apache.kyuubi.{Logging, Utils}
import org.apache.kyuubi.config.{KyuubiConf, KyuubiEbayConf}

object ElasticsearchUtils extends Logging {
  @volatile private var CLIENT: RestHighLevelClient = _

  def getClient(): RestHighLevelClient = {
    if (CLIENT == null) {
      this.synchronized {
        if (CLIENT == null) {
          val conf = KyuubiConf()
          val credentialsProvider =
            ElasticsearchCredentialsProvider.getElasticCredentialsProvider(conf)
          val host = conf.get(KyuubiEbayConf.ELASTIC_SEARCH_HOST)
          val port = conf.get(KyuubiEbayConf.ELASTIC_SEARCH_PORT)
          val schema = conf.get(KyuubiEbayConf.ELASTIC_SEARCH_SCHEMA)
          CLIENT = new RestHighLevelClient(RestClient.builder(
            new HttpHost(host, port, schema)).setHttpClientConfigCallback(httpAsyncClientBuilder =>
            httpAsyncClientBuilder.setDefaultCredentialsProvider(
              credentialsProvider).setSSLHostnameVerifier((_, _) => true)))
        }
      }
    }
    CLIENT
  }

  def indexExists(index: String): Boolean = {
    try {
      val request = new GetIndexRequest(index)
      getClient().indices().exists(request, RequestOptions.DEFAULT)
    } catch {
      case e: Throwable =>
        error(s"Error checking index[$index] exists", e)
        throw e
    }
  }

  def createIndex(index: String, source: String): Unit = {
    try {
      val request = new CreateIndexRequest(index)
      request.source(source, XContentType.JSON)
      getClient().indices().create(request, RequestOptions.DEFAULT)
    } catch {
      case e: Throwable =>
        error(s"Error creating index[$index] exists", e)
        throw e
    }
  }

  def createDoc(index: String, id: String, doc: String): Unit = {
    try {
      val request = new IndexRequest(index).id(id)
      request.source(doc, XContentType.JSON)
      getClient().index(request, RequestOptions.DEFAULT)
    } catch {
      case e: Throwable =>
        error(s"Error creating doc [$index/$id]: $doc", e)
        throw e
    }
  }

  // visible for testing
  def getDoc(index: String, id: String): JMap[String, AnyRef] = {
    try {
      val request = new GetRequest().index(index).id(id)
      getClient().get(request, RequestOptions.DEFAULT).getSource
    } catch {
      case e: Throwable =>
        error(s"Error getting doc [$index/$id]", e)
        throw e
    }
  }

  def updateDoc(index: String, id: String, doc: String): Unit = {
    try {
      val request = new UpdateRequest(index, id)
      request.doc(doc)
      getClient().update(request, RequestOptions.DEFAULT)
    } catch {
      case e: Throwable =>
        error(s"Error updating doc [$index/$id]: $doc", e)
        throw e
    }
  }

  def bulkUpdate(docsToUpdate: List[(String, String, String)]): Unit = {
    try {
      val request = new BulkRequest()
      docsToUpdate.foreach { case (index, id, doc) =>
        request.add(new IndexRequest(index).id(id).source(doc, XContentType.JSON))
      }
      getClient().bulk(request, RequestOptions.DEFAULT)
    } catch {
      case e: Throwable =>
        error(s"Error making bulk update: ${docsToUpdate.mkString(",")}", e)
        throw e
    }
  }

  def shutdown(): Unit = {
    Option(CLIENT).foreach(_ => Utils.tryLogNonFatalError(CLIENT.close()))
  }
}
