/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.catalyst.expressions.aggregate

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, JoinedRow}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateSafeProjection

/**
 * Evaluator for a [[DeclarativeAggregate]].
 */
case class DeclarativeAggregateEvaluator(function: DeclarativeAggregate, input: Seq[Attribute]) {

  lazy val initializer = GenerateSafeProjection.generate(function.initialValues)

  lazy val updater = GenerateSafeProjection.generate(
    function.updateExpressions,
    function.aggBufferAttributes ++ input)

  lazy val merger = GenerateSafeProjection.generate(
    function.mergeExpressions,
    function.aggBufferAttributes ++ function.inputAggBufferAttributes)

  lazy val evaluator = GenerateSafeProjection.generate(
    function.evaluateExpression :: Nil,
    function.aggBufferAttributes)

  def initialize(): InternalRow = initializer.apply(InternalRow.empty).copy()

  def update(values: InternalRow*): InternalRow = {
    val joiner = new JoinedRow
    val buffer = values.foldLeft(initialize()) { (buffer, input) =>
      updater(joiner(buffer, input))
    }
    buffer.copy()
  }

  def merge(buffers: InternalRow*): InternalRow = {
    val joiner = new JoinedRow
    val buffer = buffers.foldLeft(initialize()) { (left, right) =>
      merger(joiner(left, right))
    }
    buffer.copy()
  }

  def eval(buffer: InternalRow): InternalRow = evaluator(buffer).copy()
}
