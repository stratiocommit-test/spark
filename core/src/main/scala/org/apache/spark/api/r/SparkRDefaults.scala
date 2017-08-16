/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.api.r

private[spark] object SparkRDefaults {

  // Default value for spark.r.backendConnectionTimeout config
  val DEFAULT_CONNECTION_TIMEOUT: Int = 6000

  // Default value for spark.r.heartBeatInterval config
  val DEFAULT_HEARTBEAT_INTERVAL: Int = 100

  // Default value for spark.r.numRBackendThreads config
  val DEFAULT_NUM_RBACKEND_THREADS = 2
}
