/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.expressions.javalang;

import org.apache.spark.annotation.Experimental;
import org.apache.spark.annotation.InterfaceStability;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.TypedColumn;
import org.apache.spark.sql.execution.aggregate.TypedAverage;
import org.apache.spark.sql.execution.aggregate.TypedCount;
import org.apache.spark.sql.execution.aggregate.TypedSumDouble;
import org.apache.spark.sql.execution.aggregate.TypedSumLong;

/**
 * :: Experimental ::
 * Type-safe functions available for {@link org.apache.spark.sql.Dataset} operations in Java.
 *
 * Scala users should use {@link org.apache.spark.sql.expressions.scalalang.typed}.
 *
 * @since 2.0.0
 */
@Experimental
@InterfaceStability.Evolving
public class typed {
  // Note: make sure to keep in sync with typed.scala

  /**
   * Average aggregate function.
   *
   * @since 2.0.0
   */
  public static <T> TypedColumn<T, Double> avg(MapFunction<T, Double> f) {
    return new TypedAverage<T>(f).toColumnJava();
  }

  /**
   * Count aggregate function.
   *
   * @since 2.0.0
   */
  public static <T> TypedColumn<T, Long> count(MapFunction<T, Object> f) {
    return new TypedCount<T>(f).toColumnJava();
  }

  /**
   * Sum aggregate function for floating point (double) type.
   *
   * @since 2.0.0
   */
  public static <T> TypedColumn<T, Double> sum(MapFunction<T, Double> f) {
    return new TypedSumDouble<T>(f).toColumnJava();
  }

  /**
   * Sum aggregate function for integral (long, i.e. 64 bit integer) type.
   *
   * @since 2.0.0
   */
  public static <T> TypedColumn<T, Long> sumLong(MapFunction<T, Long> f) {
    return new TypedSumLong<T>(f).toColumnJava();
  }
}
