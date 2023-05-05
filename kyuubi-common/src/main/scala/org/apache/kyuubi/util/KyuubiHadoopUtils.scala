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

package org.apache.kyuubi.util

import java.io._
import java.util.{Base64, Map => JMap}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier
import org.apache.hadoop.io.Text
import org.apache.hadoop.security.{Credentials, SecurityUtil, UserGroupInformation}
import org.apache.hadoop.security.token.{Token, TokenIdentifier}
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier
import org.apache.hadoop.yarn.api.records.ApplicationId
import org.apache.hadoop.yarn.conf.YarnConfiguration

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.{KyuubiConf, KyuubiEbayConf}

object KyuubiHadoopUtils extends Logging {
  val HADOOP_MASTER_CONF_DIR = "HADOOP_MASTER_CONF_DIR"
  val HADOOP_CONF_DIR = "HADOOP_CONF_DIR"

  private val subjectField =
    classOf[UserGroupInformation].getDeclaredField("subject")
  subjectField.setAccessible(true)

  private val tokenMapField =
    classOf[Credentials].getDeclaredField("tokenMap")
  tokenMapField.setAccessible(true)

  /**
   * TODO: enhance the usage for EventLoggingService and UserGroupInformation
   * @param conf kyuubi conf
   * @param clusterOpt the cluster name, for cluster mode, does not load defaults
   * @return
   */
  def newHadoopConf(
      conf: KyuubiConf,
      loadDefaults: Boolean = true,
      clusterOpt: Option[String] = None): Configuration = {
    clusterOpt.map { _ =>
      val clusterConf = KyuubiEbayConf.loadClusterConf(conf, clusterOpt)
      val clusterEnvs = clusterConf.getEnvs
      val hadoopConf = new Configuration(false)
      clusterEnvs.get(HADOOP_MASTER_CONF_DIR).map(new File(_)).filter(_.isDirectory)
        .orElse(clusterEnvs.get(HADOOP_CONF_DIR).map(new File(_)).filter(_.isDirectory)).foreach {
          confDir =>
            confDir.listFiles().filter(_.getName.endsWith(".xml")).foreach { xmlFile =>
              hadoopConf.addResource(xmlFile.toURI.toURL)
            }
        }
      clusterConf.getAll.foreach { case (k, v) => hadoopConf.set(k, v) }
      hadoopConf
    }.getOrElse {
      val hadoopConf = new Configuration(loadDefaults)
      conf.getAll
        .foreach { case (k, v) => hadoopConf.set(k, v) }
      hadoopConf
    }
  }

  def newYarnConfiguration(
      conf: KyuubiConf,
      clusterOpt: Option[String] = None): YarnConfiguration = {
    new YarnConfiguration(newHadoopConf(conf, clusterOpt = clusterOpt))
  }

  def getServerPrincipal(principal: String): String = {
    SecurityUtil.getServerPrincipal(principal, "0.0.0.0")
  }

  def encodeCredentials(creds: Credentials): String = {
    val byteStream = new ByteArrayOutputStream
    creds.writeTokenStorageToStream(new DataOutputStream(byteStream))

    Base64.getMimeEncoder.encodeToString(byteStream.toByteArray)
  }

  def decodeCredentials(newValue: String): Credentials = {
    val decoded = Base64.getMimeDecoder.decode(newValue)

    val byteStream = new ByteArrayInputStream(decoded)
    val creds = new Credentials()
    creds.readTokenStorageStream(new DataInputStream(byteStream))
    creds
  }

  /**
   * Get [[Credentials#tokenMap]] by reflection as [[Credentials#getTokenMap]] is not present before
   * Hadoop 3.2.1.
   */
  def getTokenMap(credentials: Credentials): Map[Text, Token[_ <: TokenIdentifier]] = {
    tokenMapField.get(credentials)
      .asInstanceOf[JMap[Text, Token[_ <: TokenIdentifier]]]
      .asScala
      .toMap
  }

  def getTokenIssueDate(token: Token[_ <: TokenIdentifier]): Option[Long] = {
    token.decodeIdentifier() match {
      case tokenIdent: AbstractDelegationTokenIdentifier =>
        Some(tokenIdent.getIssueDate)
      case null =>
        // TokenIdentifiers not found in ServiceLoader
        val tokenIdentifier = new DelegationTokenIdentifier
        val buf = new ByteArrayInputStream(token.getIdentifier)
        val in = new DataInputStream(buf)
        Try(tokenIdentifier.readFields(in)) match {
          case Success(_) =>
            Some(tokenIdentifier.getIssueDate)
          case Failure(e) =>
            warn(s"Can not decode identifier of token $token", e)
            None
        }
      case tokenIdent =>
        debug(s"Unsupported TokenIdentifier kind: ${tokenIdent.getKind}")
        None
    }
  }

  /**
   * Copy from Application.fromString, it is involved from Hadoop-2.8
   * However, ebay hadoop dependency version is hadoop-2.7.3
   */
  def getApplicationIdFromString(appIdStr: String): ApplicationId = {
    val appIdStrPrefix = "application"
    val APPLICATION_ID_PREFIX = appIdStrPrefix + '_'
    if (!appIdStr.startsWith(APPLICATION_ID_PREFIX)) {
      throw new IllegalArgumentException("Invalid ApplicationId prefix: "
        + appIdStr + ". The valid ApplicationId should start with prefix "
        + appIdStrPrefix);
    }

    try {
      val pos1 = APPLICATION_ID_PREFIX.length() - 1
      val pos2 = appIdStr.indexOf('_', pos1 + 1)
      if (pos2 < 0) {
        throw new IllegalArgumentException("Invalid ApplicationId: "
          + appIdStr)
      }
      val rmId = appIdStr.substring(pos1 + 1, pos2).toLong
      val appId = appIdStr.substring(pos2 + 1).toInt
      ApplicationId.newInstance(rmId, appId)
    } catch {
      case n: NumberFormatException =>
        throw new IllegalArgumentException("Invalid ApplicationId: " + appIdStr, n)
    }
  }
}
