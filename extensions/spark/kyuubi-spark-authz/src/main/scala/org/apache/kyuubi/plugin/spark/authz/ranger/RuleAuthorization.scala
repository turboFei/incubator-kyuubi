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

package org.apache.kyuubi.plugin.spark.authz.ranger

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag

import org.apache.kyuubi.plugin.spark.authz.{AccessControlException, ObjectType, _}
import org.apache.kyuubi.plugin.spark.authz.ObjectType._
import org.apache.kyuubi.plugin.spark.authz.ranger.RuleAuthorization.KYUUBI_AUTHZ_TAG
import org.apache.kyuubi.plugin.spark.authz.util.AuthZUtils._

class RuleAuthorization(spark: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan match {
    case p if !plan.getTagValue(KYUUBI_AUTHZ_TAG).contains(true) =>
      RuleAuthorization.checkPrivileges(spark, p)
      p.setTagValue(KYUUBI_AUTHZ_TAG, true)
      p
    case p => p // do nothing if checked privileges already.
  }
}

object RuleAuthorization {

  val KYUUBI_AUTHZ_TAG = TreeNodeTag[Boolean]("__KYUUBI_AUTHZ_TAG")

  def checkPrivileges(spark: SparkSession, plan: LogicalPlan): Unit = {
    val auditHandler = new SparkRangerAuditHandler
    val ugi = getAuthzUgi(spark.sparkContext)
    val opType = OperationType(plan.nodeName)
    val (inputs, outputs) = PrivilegesBuilder.build(plan, spark)
    val requests = new ArrayBuffer[AccessRequest]()
    if (inputs.isEmpty && opType == OperationType.SHOWDATABASES) {
      val resource = AccessResource(DATABASE, null)
      requests += AccessRequest(resource, ugi, opType, AccessType.USE)
    }

    def addAccessRequest(objects: Seq[PrivilegeObject], isInput: Boolean): Unit = {
      objects.foreach { obj =>
        val resource = AccessResource(obj, opType)
        val accessType = ranger.AccessType(obj, opType, isInput)
        if (accessType != AccessType.NONE && !requests.exists(o =>
            o.accessType == accessType && o.getResource == resource)) {
          requests += AccessRequest(resource, ugi, opType, accessType)
        }
      }
    }

    addAccessRequest(inputs, isInput = true)
    addAccessRequest(outputs, isInput = false)

    requests.foreach { request =>
      val resource = request.getResource.asInstanceOf[AccessResource]
      resource.objectType match {
        case ObjectType.COLUMN if resource.getColumns.nonEmpty =>
          resource.getColumns.foreach { col =>
            val cr = AccessResource(COLUMN, resource.getDatabase, resource.getTable, col)
            val req = AccessRequest(cr, ugi, opType, request.accessType)
            verify(req, auditHandler)
          }
        case _ => verify(request, auditHandler)
      }
    }
  }

  private def verify(req: AccessRequest, auditHandler: SparkRangerAuditHandler): Unit = {
    val ret = SparkRangerAdminPlugin.isAccessAllowed(req, auditHandler)
    if (ret != null && !ret.getIsAllowed) {
      throw new AccessControlException(
        s"Permission denied: user [${req.getUser}] does not have [${req.getAccessType}] privilege" +
          s" on [${req.getResource.getAsString}]")
    }
  }
}
