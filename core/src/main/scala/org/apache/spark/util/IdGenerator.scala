/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.util

import java.util.concurrent.atomic.AtomicInteger

/**
 * A util used to get a unique generation ID. This is a wrapper around Java's
 * AtomicInteger. An example usage is in BlockManager, where each BlockManager
 * instance would start an RpcEndpoint and we use this utility to assign the RpcEndpoints'
 * unique names.
 */
private[spark] class IdGenerator {
  private val id = new AtomicInteger
  def next: Int = id.incrementAndGet
}
