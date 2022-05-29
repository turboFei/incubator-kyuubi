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

package org.apache.kyuubi.ctl

import scala.collection.mutable.ListBuffer

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf.{ENGINE_SHARE_LEVEL, ENGINE_SHARE_LEVEL_SUBDOMAIN, ENGINE_TYPE}
import org.apache.kyuubi.ha.HighAvailabilityConf._
import org.apache.kyuubi.ha.client.{DiscoveryClientProvider, ServiceNodeInfo}
import org.apache.kyuubi.ha.client.DiscoveryClient
import org.apache.kyuubi.ha.client.DiscoveryPaths

private[ctl] object ServiceControlAction extends Enumeration {
  type ServiceControlAction = Value
  val CREATE, GET, DELETE, LIST = Value
}

private[ctl] object ServiceControlObject extends Enumeration {
  type ServiceControlObject = Value
  val SERVER, ENGINE = Value
}

/**
 * Main gateway of launching a Kyuubi Ctl action.
 */
private[kyuubi] class ServiceControlCli extends Logging {
  import DiscoveryClientProvider._
  import ServiceControlCli._

  private var verbose: Boolean = false

  def doAction(args: Array[String]): Unit = {
    // Initialize logging if it hasn't been done yet.
    // Set log level ERROR
    initializeLoggerIfNecessary(true)

    val ctlArgs = parseArguments(args)

    // when parse failed, exit
    if (ctlArgs.cliArgs == null) {
      sys.exit(1)
    }

    verbose = ctlArgs.cliArgs.verbose
    if (verbose) {
      super.info(ctlArgs.toString)
    }
    ctlArgs.cliArgs.action match {
      case ServiceControlAction.CREATE => create(ctlArgs)
      case ServiceControlAction.LIST => list(ctlArgs, filterHostPort = false)
      case ServiceControlAction.GET => list(ctlArgs, filterHostPort = true)
      case ServiceControlAction.DELETE => delete(ctlArgs)
    }
  }

  protected def parseArguments(args: Array[String]): ServiceControlCliArguments = {
    new ServiceControlCliArguments(args)
  }

  /**
   * Expose Kyuubi server instance to another domain.
   */
  private def create(args: ServiceControlCliArguments): Unit = {
    val kyuubiConf = args.conf

    kyuubiConf.setIfMissing(HA_ADDRESSES, args.cliArgs.zkQuorum)
    withDiscoveryClient(kyuubiConf) { discoveryClient =>
      val fromNamespace =
        DiscoveryPaths.makePath(null, kyuubiConf.get(HA_NAMESPACE))
      val toNamespace = getZkNamespace(args)

      val currentServerNodes = discoveryClient.getServiceNodesInfo(fromNamespace)
      val exposedServiceNodes = ListBuffer[ServiceNodeInfo]()

      if (currentServerNodes.nonEmpty) {
        def doCreate(zc: DiscoveryClient): Unit = {
          currentServerNodes.foreach { sn =>
            info(s"Exposing server instance:${sn.instance} with version:${sn.version}" +
              s" from $fromNamespace to $toNamespace")
            val newNodePath = zc.createAndGetServiceNode(
              kyuubiConf,
              args.cliArgs.namespace,
              sn.instance,
              sn.version,
              true)
            exposedServiceNodes += sn.copy(
              namespace = toNamespace,
              nodeName = newNodePath.split("/").last)
          }
        }

        if (kyuubiConf.get(HA_ADDRESSES) == args.cliArgs.zkQuorum) {
          doCreate(discoveryClient)
        } else {
          kyuubiConf.set(HA_ADDRESSES, args.cliArgs.zkQuorum)
          withDiscoveryClient(kyuubiConf)(doCreate)
        }
      }

      val title = "Created zookeeper service nodes"
      info(renderServiceNodesInfo(title, exposedServiceNodes, verbose))
    }
  }

  /**
   * List Kyuubi server nodes info.
   */
  private def list(args: ServiceControlCliArguments, filterHostPort: Boolean): Unit = {
    withDiscoveryClient(args.conf) { discoveryClient =>
      val znodeRoot = getZkNamespace(args)
      val hostPortOpt =
        if (filterHostPort) {
          Some((args.cliArgs.host, args.cliArgs.port.toInt))
        } else None
      val nodes = getServiceNodes(discoveryClient, znodeRoot, hostPortOpt)

      val title = "Zookeeper service nodes"
      info(renderServiceNodesInfo(title, nodes, verbose))
    }
  }

  private def getServiceNodes(
      discoveryClient: DiscoveryClient,
      znodeRoot: String,
      hostPortOpt: Option[(String, Int)]): Seq[ServiceNodeInfo] = {
    val serviceNodes = discoveryClient.getServiceNodesInfo(znodeRoot)
    hostPortOpt match {
      case Some((host, port)) => serviceNodes.filter { sn =>
          sn.host == host && sn.port == port
        }
      case _ => serviceNodes
    }
  }

  /**
   * Delete zookeeper service node with specified host port.
   */
  private def delete(args: ServiceControlCliArguments): Unit = {
    withDiscoveryClient(args.conf) { discoveryClient =>
      val znodeRoot = getZkNamespace(args)
      val hostPortOpt = Some((args.cliArgs.host, args.cliArgs.port.toInt))
      val nodesToDelete = getServiceNodes(discoveryClient, znodeRoot, hostPortOpt)

      val deletedNodes = ListBuffer[ServiceNodeInfo]()
      nodesToDelete.foreach { node =>
        val nodePath = s"$znodeRoot/${node.nodeName}"
        info(s"Deleting zookeeper service node:$nodePath")
        try {
          discoveryClient.delete(nodePath)
          deletedNodes += node
        } catch {
          case e: Exception =>
            error(s"Failed to delete zookeeper service node:$nodePath", e)
        }
      }

      val title = "Deleted zookeeper service nodes"
      info(renderServiceNodesInfo(title, deletedNodes, verbose))
    }
  }
}

object ServiceControlCli extends CommandLineUtils with Logging {
  override def main(args: Array[String]): Unit = {
    val ctl = new ServiceControlCli() {
      self =>
      override protected def parseArguments(args: Array[String]): ServiceControlCliArguments = {
        new ServiceControlCliArguments(args) {
          override def info(msg: => Any): Unit = self.info(msg)

          override def warn(msg: => Any): Unit = self.warn(msg)

          override def error(msg: => Any): Unit = self.error(msg)

          override private[kyuubi] lazy val effectSetup = new KyuubiOEffectSetup {
            override def displayToOut(msg: String): Unit = self.info(msg)

            override def displayToErr(msg: String): Unit = self.info(msg)

            override def reportError(msg: String): Unit = self.info(msg)

            override def reportWarning(msg: String): Unit = self.warn(msg)
          }
        }
      }

      override def info(msg: => Any): Unit = printMessage(msg)

      override def warn(msg: => Any): Unit = printMessage(s"Warning: $msg")

      override def error(msg: => Any): Unit = printMessage(s"Error: $msg")

      override def doAction(args: Array[String]): Unit = {
        try {
          super.doAction(args)
          exitFn(0)
        } catch {
          case e: ServiceControlCliException =>
            exitFn(e.exitCode)
        }
      }
    }

    ctl.doAction(args)
  }

  private[ctl] def getZkNamespace(args: ServiceControlCliArguments): String = {
    args.cliArgs.service match {
      case ServiceControlObject.SERVER =>
        DiscoveryPaths.makePath(null, args.cliArgs.namespace)
      case ServiceControlObject.ENGINE =>
        val engineType = Some(args.cliArgs.engineType)
          .filter(_ != null).filter(_.nonEmpty)
          .getOrElse(args.conf.get(ENGINE_TYPE))
        val engineSubdomain = Some(args.cliArgs.engineSubdomain)
          .filter(_ != null).filter(_.nonEmpty)
          .getOrElse(args.conf.get(ENGINE_SHARE_LEVEL_SUBDOMAIN).getOrElse("default"))
        val engineShareLevel = Some(args.cliArgs.engineShareLevel)
          .filter(_ != null).filter(_.nonEmpty)
          .getOrElse(args.conf.get(ENGINE_SHARE_LEVEL))
        // The path of the engine defined in zookeeper comes from
        // org.apache.kyuubi.engine.EngineRef#engineSpace
        DiscoveryPaths.makePath(
          s"${args.cliArgs.namespace}_${args.cliArgs.version}_${engineShareLevel}_${engineType}",
          args.cliArgs.user,
          Array(engineSubdomain))
    }
  }

  private[ctl] def renderServiceNodesInfo(
      title: String,
      serviceNodeInfo: Seq[ServiceNodeInfo],
      verbose: Boolean): String = {
    val header = Seq("Namespace", "Host", "Port", "Version")
    val rows = serviceNodeInfo.sortBy(_.nodeName).map { sn =>
      Seq(sn.namespace, sn.host, sn.port.toString, sn.version.getOrElse(""))
    }
    Tabulator.format(title, header, rows, verbose)
  }
}
