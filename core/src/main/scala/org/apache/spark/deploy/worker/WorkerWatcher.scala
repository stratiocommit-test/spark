/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.deploy.worker

import org.apache.spark.internal.Logging
import org.apache.spark.rpc._

/**
 * Endpoint which connects to a worker process and terminates the JVM if the
 * connection is severed.
 * Provides fate sharing between a worker and its associated child processes.
 */
private[spark] class WorkerWatcher(
    override val rpcEnv: RpcEnv, workerUrl: String, isTesting: Boolean = false)
  extends RpcEndpoint with Logging {

  logInfo(s"Connecting to worker $workerUrl")
  if (!isTesting) {
    rpcEnv.asyncSetupEndpointRefByURI(workerUrl)
  }

  // Used to avoid shutting down JVM during tests
  // In the normal case, exitNonZero will call `System.exit(-1)` to shutdown the JVM. In the unit
  // test, the user should call `setTesting(true)` so that `exitNonZero` will set `isShutDown` to
  // true rather than calling `System.exit`. The user can check `isShutDown` to know if
  // `exitNonZero` is called.
  private[deploy] var isShutDown = false

  // Lets filter events only from the worker's rpc system
  private val expectedAddress = RpcAddress.fromURIString(workerUrl)
  private def isWorker(address: RpcAddress) = expectedAddress == address

  private def exitNonZero() = if (isTesting) isShutDown = true else System.exit(-1)

  override def receive: PartialFunction[Any, Unit] = {
    case e => logWarning(s"Received unexpected message: $e")
  }

  override def onConnected(remoteAddress: RpcAddress): Unit = {
    if (isWorker(remoteAddress)) {
      logInfo(s"Successfully connected to $workerUrl")
    }
  }

  override def onDisconnected(remoteAddress: RpcAddress): Unit = {
    if (isWorker(remoteAddress)) {
      // This log message will never be seen
      logError(s"Lost connection to worker rpc endpoint $workerUrl. Exiting.")
      exitNonZero()
    }
  }

  override def onNetworkError(cause: Throwable, remoteAddress: RpcAddress): Unit = {
    if (isWorker(remoteAddress)) {
      // These logs may not be seen if the worker (and associated pipe) has died
      logError(s"Could not initialize connection to worker $workerUrl. Exiting.")
      logError(s"Error was: $cause")
      exitNonZero()
    }
  }
}
