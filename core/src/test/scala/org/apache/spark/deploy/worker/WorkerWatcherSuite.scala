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

import org.apache.spark.{SecurityManager, SparkConf, SparkFunSuite}
import org.apache.spark.rpc.{RpcAddress, RpcEndpointAddress, RpcEnv}

class WorkerWatcherSuite extends SparkFunSuite {
  test("WorkerWatcher shuts down on valid disassociation") {
    val conf = new SparkConf()
    val rpcEnv = RpcEnv.create("test", "localhost", 12345, conf, new SecurityManager(conf))
    val targetWorkerUrl = RpcEndpointAddress(RpcAddress("1.2.3.4", 1234), "Worker").toString
    val workerWatcher = new WorkerWatcher(rpcEnv, targetWorkerUrl, isTesting = true)
    rpcEnv.setupEndpoint("worker-watcher", workerWatcher)
    workerWatcher.onDisconnected(RpcAddress("1.2.3.4", 1234))
    assert(workerWatcher.isShutDown)
    rpcEnv.shutdown()
  }

  test("WorkerWatcher stays alive on invalid disassociation") {
    val conf = new SparkConf()
    val rpcEnv = RpcEnv.create("test", "localhost", 12345, conf, new SecurityManager(conf))
    val targetWorkerUrl = RpcEndpointAddress(RpcAddress("1.2.3.4", 1234), "Worker").toString
    val otherRpcAddress = RpcAddress("4.3.2.1", 1234)
    val workerWatcher = new WorkerWatcher(rpcEnv, targetWorkerUrl, isTesting = true)
    rpcEnv.setupEndpoint("worker-watcher", workerWatcher)
    workerWatcher.onDisconnected(otherRpcAddress)
    assert(!workerWatcher.isShutDown)
    rpcEnv.shutdown()
  }
}
