/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.expressions

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

/**
 * An aggregator that uses a single associative and commutative reduce function. This reduce
 * function can be used to go through all input values and reduces them to a single value.
 * If there is no input, a null value is returned.
 *
 * This class currently assumes there is at least one input row.
 */
private[sql] class ReduceAggregator[T: Encoder](func: (T, T) => T)
  extends Aggregator[T, (Boolean, T), T] {

  private val encoder = implicitly[Encoder[T]]

  override def zero: (Boolean, T) = (false, null.asInstanceOf[T])

  override def bufferEncoder: Encoder[(Boolean, T)] =
    ExpressionEncoder.tuple(
      ExpressionEncoder[Boolean](),
      encoder.asInstanceOf[ExpressionEncoder[T]])

  override def outputEncoder: Encoder[T] = encoder

  override def reduce(b: (Boolean, T), a: T): (Boolean, T) = {
    if (b._1) {
      (true, func(b._2, a))
    } else {
      (true, a)
    }
  }

  override def merge(b1: (Boolean, T), b2: (Boolean, T)): (Boolean, T) = {
    if (!b1._1) {
      b2
    } else if (!b2._1) {
      b1
    } else {
      (true, func(b1._2, b2._2))
    }
  }

  override def finish(reduction: (Boolean, T)): T = {
    if (!reduction._1) {
      throw new IllegalStateException("ReduceAggregator requires at least one input row")
    }
    reduction._2
  }
}
