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

import org.apache.spark.annotation.{Experimental, InterfaceStability}
import org.apache.spark.sql.{Dataset, Encoder, TypedColumn}
import org.apache.spark.sql.catalyst.encoders.encoderFor
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Complete}
import org.apache.spark.sql.execution.aggregate.TypedAggregateExpression

/**
 * :: Experimental ::
 * A base class for user-defined aggregations, which can be used in `Dataset` operations to take
 * all of the elements of a group and reduce them to a single value.
 *
 * For example, the following aggregator extracts an `int` from a specific class and adds them up:
 * {{{
 *   case class Data(i: Int)
 *
 *   val customSummer =  new Aggregator[Data, Int, Int] {
 *     def zero: Int = 0
 *     def reduce(b: Int, a: Data): Int = b + a.i
 *     def merge(b1: Int, b2: Int): Int = b1 + b2
 *     def finish(r: Int): Int = r
 *   }.toColumn()
 *
 *   val ds: Dataset[Data] = ...
 *   val aggregated = ds.select(customSummer)
 * }}}
 *
 * Based loosely on Aggregator from Algebird: https://github.com/twitter/algebird
 *
 * @tparam IN The input type for the aggregation.
 * @tparam BUF The type of the intermediate value of the reduction.
 * @tparam OUT The type of the final output result.
 * @since 1.6.0
 */
@Experimental
@InterfaceStability.Evolving
abstract class Aggregator[-IN, BUF, OUT] extends Serializable {

  /**
   * A zero value for this aggregation. Should satisfy the property that any b + zero = b.
   * @since 1.6.0
   */
  def zero: BUF

  /**
   * Combine two values to produce a new value.  For performance, the function may modify `b` and
   * return it instead of constructing new object for b.
   * @since 1.6.0
   */
  def reduce(b: BUF, a: IN): BUF

  /**
   * Merge two intermediate values.
   * @since 1.6.0
   */
  def merge(b1: BUF, b2: BUF): BUF

  /**
   * Transform the output of the reduction.
   * @since 1.6.0
   */
  def finish(reduction: BUF): OUT

  /**
   * Specifies the `Encoder` for the intermediate value type.
   * @since 2.0.0
   */
  def bufferEncoder: Encoder[BUF]

  /**
   * Specifies the `Encoder` for the final ouput value type.
   * @since 2.0.0
   */
  def outputEncoder: Encoder[OUT]

  /**
   * Returns this `Aggregator` as a `TypedColumn` that can be used in `Dataset`.
   * operations.
   * @since 1.6.0
   */
  def toColumn: TypedColumn[IN, OUT] = {
    implicit val bEncoder = bufferEncoder
    implicit val cEncoder = outputEncoder

    val expr =
      AggregateExpression(
        TypedAggregateExpression(this),
        Complete,
        isDistinct = false)

    new TypedColumn[IN, OUT](expr, encoderFor[OUT])
  }
}
