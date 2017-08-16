/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.partial

/**
 * A Double value with error bars and associated confidence.
 */
class BoundedDouble(val mean: Double, val confidence: Double, val low: Double, val high: Double) {

  override def toString(): String = "[%.3f, %.3f]".format(low, high)

  override def hashCode: Int =
    this.mean.hashCode ^ this.confidence.hashCode ^ this.low.hashCode ^ this.high.hashCode

  /**
   * @note Consistent with Double, any NaN value will make equality false
   */
  override def equals(that: Any): Boolean =
    that match {
      case that: BoundedDouble =>
        this.mean == that.mean &&
        this.confidence == that.confidence &&
        this.low == that.low &&
        this.high == that.high
      case _ => false
    }
}
