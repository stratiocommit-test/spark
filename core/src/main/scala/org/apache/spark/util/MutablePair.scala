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

import org.apache.spark.annotation.DeveloperApi

/**
 * :: DeveloperApi ::
 * A tuple of 2 elements. This can be used as an alternative to Scala's Tuple2 when we want to
 * minimize object allocation.
 *
 * @param  _1   Element 1 of this MutablePair
 * @param  _2   Element 2 of this MutablePair
 */
@DeveloperApi
case class MutablePair[@specialized(Int, Long, Double, Char, Boolean/* , AnyRef */) T1,
                       @specialized(Int, Long, Double, Char, Boolean/* , AnyRef */) T2]
  (var _1: T1, var _2: T2)
  extends Product2[T1, T2]
{
  /** No-arg constructor for serialization */
  def this() = this(null.asInstanceOf[T1], null.asInstanceOf[T2])

  /** Updates this pair with new values and returns itself */
  def update(n1: T1, n2: T2): MutablePair[T1, T2] = {
    _1 = n1
    _2 = n2
    this
  }

  override def toString: String = "(" + _1 + "," + _2 + ")"

  override def canEqual(that: Any): Boolean = that.isInstanceOf[MutablePair[_, _]]
}
