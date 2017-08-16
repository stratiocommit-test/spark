/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.streaming.scheduler

import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.scheduler.{ExecutorCacheTaskLocation, TaskLocation}
import org.apache.spark.streaming.scheduler.ReceiverState._

private[streaming] case class ReceiverErrorInfo(
    lastErrorMessage: String = "", lastError: String = "", lastErrorTime: Long = -1L)

/**
 * Class having information about a receiver.
 *
 * @param receiverId the unique receiver id
 * @param state the current Receiver state
 * @param scheduledLocations the scheduled locations provided by ReceiverSchedulingPolicy
 * @param runningExecutor the running executor if the receiver is active
 * @param name the receiver name
 * @param endpoint the receiver endpoint. It can be used to send messages to the receiver
 * @param errorInfo the receiver error information if it fails
 */
private[streaming] case class ReceiverTrackingInfo(
    receiverId: Int,
    state: ReceiverState,
    scheduledLocations: Option[Seq[TaskLocation]],
    runningExecutor: Option[ExecutorCacheTaskLocation],
    name: Option[String] = None,
    endpoint: Option[RpcEndpointRef] = None,
    errorInfo: Option[ReceiverErrorInfo] = None) {

  def toReceiverInfo: ReceiverInfo = ReceiverInfo(
    receiverId,
    name.getOrElse(""),
    state == ReceiverState.ACTIVE,
    location = runningExecutor.map(_.host).getOrElse(""),
    executorId = runningExecutor.map(_.executorId).getOrElse(""),
    lastErrorMessage = errorInfo.map(_.lastErrorMessage).getOrElse(""),
    lastError = errorInfo.map(_.lastError).getOrElse(""),
    lastErrorTime = errorInfo.map(_.lastErrorTime).getOrElse(-1L)
  )
}
