/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.deploy

/**
 * Used to send state on-the-wire about Executors from Worker to Master.
 * This state is sufficient for the Master to reconstruct its internal data structures during
 * failover.
 */
private[deploy] class ExecutorDescription(
    val appId: String,
    val execId: Int,
    val cores: Int,
    val state: ExecutorState.Value)
  extends Serializable {

  override def toString: String =
    "ExecutorState(appId=%s, execId=%d, cores=%d, state=%s)".format(appId, execId, cores, state)
}
