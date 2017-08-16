/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.expressions.scalalang

import org.apache.spark.annotation.{Experimental, InterfaceStability}
import org.apache.spark.sql._
import org.apache.spark.sql.execution.aggregate._

/**
 * :: Experimental ::
 * Type-safe functions available for `Dataset` operations in Scala.
 *
 * Java users should use [[org.apache.spark.sql.expressions.javalang.typed]].
 *
 * @since 2.0.0
 */
@Experimental
@InterfaceStability.Evolving
// scalastyle:off
object typed {
  // scalastyle:on

  // Note: whenever we update this file, we should update the corresponding Java version too.
  // The reason we have separate files for Java and Scala is because in the Scala version, we can
  // use tighter types (primitive types) for return types, whereas in the Java version we can only
  // use boxed primitive types.
  // For example, avg in the Scala version returns Scala primitive Double, whose bytecode
  // signature is just a java.lang.Object; avg in the Java version returns java.lang.Double.

  // TODO: This is pretty hacky. Maybe we should have an object for implicit encoders.
  private val implicits = new SQLImplicits {
    override protected def _sqlContext: SQLContext = null
  }

  import implicits._

  /**
   * Average aggregate function.
   *
   * @since 2.0.0
   */
  def avg[IN](f: IN => Double): TypedColumn[IN, Double] = new TypedAverage(f).toColumn

  /**
   * Count aggregate function.
   *
   * @since 2.0.0
   */
  def count[IN](f: IN => Any): TypedColumn[IN, Long] = new TypedCount(f).toColumn

  /**
   * Sum aggregate function for floating point (double) type.
   *
   * @since 2.0.0
   */
  def sum[IN](f: IN => Double): TypedColumn[IN, Double] = new TypedSumDouble[IN](f).toColumn

  /**
   * Sum aggregate function for integral (long, i.e. 64 bit integer) type.
   *
   * @since 2.0.0
   */
  def sumLong[IN](f: IN => Long): TypedColumn[IN, Long] = new TypedSumLong[IN](f).toColumn

  // TODO:
  // stddevOf: Double
  // varianceOf: Double
  // approxCountDistinct: Long

  // minOf: T
  // maxOf: T

  // firstOf: T
  // lastOf: T
}
