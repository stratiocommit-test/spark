/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.types.AbstractDataType

/**
 * A trait that gets mixin to define the expected input types of an expression.
 *
 * This trait is typically used by operator expressions (e.g. [[Add]], [[Subtract]]) to define
 * expected input types without any implicit casting.
 *
 * Most function expressions (e.g. [[Substring]] should extends [[ImplicitCastInputTypes]]) instead.
 */
trait ExpectsInputTypes extends Expression {

  /**
   * Expected input types from child expressions. The i-th position in the returned seq indicates
   * the type requirement for the i-th child.
   *
   * The possible values at each position are:
   * 1. a specific data type, e.g. LongType, StringType.
   * 2. a non-leaf abstract data type, e.g. NumericType, IntegralType, FractionalType.
   */
  def inputTypes: Seq[AbstractDataType]

  override def checkInputDataTypes(): TypeCheckResult = {
    val mismatches = children.zip(inputTypes).zipWithIndex.collect {
      case ((child, expected), idx) if !expected.acceptsType(child.dataType) =>
        s"argument ${idx + 1} requires ${expected.simpleString} type, " +
          s"however, '${child.sql}' is of ${child.dataType.simpleString} type."
    }

    if (mismatches.isEmpty) {
      TypeCheckResult.TypeCheckSuccess
    } else {
      TypeCheckResult.TypeCheckFailure(mismatches.mkString(" "))
    }
  }
}


/**
 * A mixin for the analyzer to perform implicit type casting using
 * [[org.apache.spark.sql.catalyst.analysis.TypeCoercion.ImplicitTypeCasts]].
 */
trait ImplicitCastInputTypes extends ExpectsInputTypes {
  // No other methods
}
