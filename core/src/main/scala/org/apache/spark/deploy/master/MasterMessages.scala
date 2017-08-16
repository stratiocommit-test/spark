/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.deploy.master

sealed trait MasterMessages extends Serializable

/** Contains messages seen only by the Master and its associated entities. */
private[master] object MasterMessages {

  // LeaderElectionAgent to Master

  case object ElectedLeader

  case object RevokedLeadership

  // Master to itself

  case object CheckForWorkerTimeOut

  case class BeginRecovery(storedApps: Seq[ApplicationInfo], storedWorkers: Seq[WorkerInfo])

  case object CompleteRecovery

  case object BoundPortsRequest

  case class BoundPortsResponse(rpcEndpointPort: Int, webUIPort: Int, restPort: Option[Int])
}
