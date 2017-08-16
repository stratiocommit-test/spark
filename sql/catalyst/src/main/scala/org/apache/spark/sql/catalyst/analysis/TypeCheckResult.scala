/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.catalyst.analysis

/**
 * Represents the result of `Expression.checkInputDataTypes`.
 * We will throw `AnalysisException` in `CheckAnalysis` if `isFailure` is true.
 */
trait TypeCheckResult {
  def isFailure: Boolean = !isSuccess
  def isSuccess: Boolean
}

object TypeCheckResult {

  /**
   * Represents the successful result of `Expression.checkInputDataTypes`.
   */
  object TypeCheckSuccess extends TypeCheckResult {
    def isSuccess: Boolean = true
  }

  /**
   * Represents the failing result of `Expression.checkInputDataTypes`,
   * with an error message to show the reason of failure.
   */
  case class TypeCheckFailure(message: String) extends TypeCheckResult {
    def isSuccess: Boolean = false
  }
}
