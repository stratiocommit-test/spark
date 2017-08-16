/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.util.collection

import scala.reflect.ClassTag

/**
 * An append-only buffer that keeps track of its estimated size in bytes.
 */
private[spark] class SizeTrackingVector[T: ClassTag]
  extends PrimitiveVector[T]
  with SizeTracker {

  override def +=(value: T): Unit = {
    super.+=(value)
    super.afterUpdate()
  }

  override def resize(newLength: Int): PrimitiveVector[T] = {
    super.resize(newLength)
    resetSamples()
    this
  }
}
