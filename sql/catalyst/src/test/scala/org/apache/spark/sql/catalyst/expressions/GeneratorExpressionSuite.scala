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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._

class GeneratorExpressionSuite extends SparkFunSuite with ExpressionEvalHelper {
  private def checkTuple(actual: Expression, expected: Seq[InternalRow]): Unit = {
    assert(actual.eval(null).asInstanceOf[TraversableOnce[InternalRow]].toSeq === expected)
  }

  private final val empty_array = CreateArray(Seq.empty)
  private final val int_array = CreateArray(Seq(1, 2, 3).map(Literal(_)))
  private final val str_array = CreateArray(Seq("a", "b", "c").map(Literal(_)))

  test("explode") {
    val int_correct_answer = Seq(create_row(1), create_row(2), create_row(3))
    val str_correct_answer = Seq(create_row("a"), create_row("b"), create_row("c"))

    checkTuple(Explode(empty_array), Seq.empty)
    checkTuple(Explode(int_array), int_correct_answer)
    checkTuple(Explode(str_array), str_correct_answer)
  }

  test("posexplode") {
    val int_correct_answer = Seq(create_row(0, 1), create_row(1, 2), create_row(2, 3))
    val str_correct_answer = Seq(create_row(0, "a"), create_row(1, "b"), create_row(2, "c"))

    checkTuple(PosExplode(CreateArray(Seq.empty)), Seq.empty)
    checkTuple(PosExplode(int_array), int_correct_answer)
    checkTuple(PosExplode(str_array), str_correct_answer)
  }

  test("inline") {
    val correct_answer = Seq(create_row(0, "a"), create_row(1, "b"), create_row(2, "c"))

    checkTuple(
      Inline(Literal.create(Array(), ArrayType(new StructType().add("id", LongType)))),
      Seq.empty)

    checkTuple(
      Inline(CreateArray(Seq(
        CreateStruct(Seq(Literal(0), Literal("a"))),
        CreateStruct(Seq(Literal(1), Literal("b"))),
        CreateStruct(Seq(Literal(2), Literal("c")))
      ))),
      correct_answer)
  }

  test("stack") {
    checkTuple(Stack(Seq(1, 1).map(Literal(_))), Seq(create_row(1)))
    checkTuple(Stack(Seq(1, 1, 2).map(Literal(_))), Seq(create_row(1, 2)))
    checkTuple(Stack(Seq(2, 1, 2).map(Literal(_))), Seq(create_row(1), create_row(2)))
    checkTuple(Stack(Seq(2, 1, 2, 3).map(Literal(_))), Seq(create_row(1, 2), create_row(3, null)))
    checkTuple(Stack(Seq(3, 1, 2, 3).map(Literal(_))), Seq(1, 2, 3).map(create_row(_)))
    checkTuple(Stack(Seq(4, 1, 2, 3).map(Literal(_))), Seq(1, 2, 3, null).map(create_row(_)))

    checkTuple(
      Stack(Seq(3, 1, 1.0, "a", 2, 2.0, "b", 3, 3.0, "c").map(Literal(_))),
      Seq(create_row(1, 1.0, "a"), create_row(2, 2.0, "b"), create_row(3, 3.0, "c")))

    assert(Stack(Seq(Literal(1))).checkInputDataTypes().isFailure)
    assert(Stack(Seq(Literal(1.0))).checkInputDataTypes().isFailure)
    assert(Stack(Seq(Literal(1), Literal(1), Literal(1.0))).checkInputDataTypes().isSuccess)
    assert(Stack(Seq(Literal(2), Literal(1), Literal(1.0))).checkInputDataTypes().isFailure)
  }
}
