/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.catalyst.encoders

import scala.reflect.ClassTag

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.Encoders

class NonEncodable(i: Int)

case class ComplexNonEncodable1(name1: NonEncodable)

case class ComplexNonEncodable2(name2: ComplexNonEncodable1)

case class ComplexNonEncodable3(name3: Option[NonEncodable])

case class ComplexNonEncodable4(name4: Array[NonEncodable])

case class ComplexNonEncodable5(name5: Option[Array[NonEncodable]])

class EncoderErrorMessageSuite extends SparkFunSuite {

  // Note: we also test error messages for encoders for private classes in JavaDatasetSuite.
  // That is done in Java because Scala cannot create truly private classes.

  test("primitive types in encoders using Kryo serialization") {
    intercept[UnsupportedOperationException] { Encoders.kryo[Int] }
    intercept[UnsupportedOperationException] { Encoders.kryo[Long] }
    intercept[UnsupportedOperationException] { Encoders.kryo[Char] }
  }

  test("primitive types in encoders using Java serialization") {
    intercept[UnsupportedOperationException] { Encoders.javaSerialization[Int] }
    intercept[UnsupportedOperationException] { Encoders.javaSerialization[Long] }
    intercept[UnsupportedOperationException] { Encoders.javaSerialization[Char] }
  }

  test("nice error message for missing encoder") {
    val errorMsg1 =
      intercept[UnsupportedOperationException](ExpressionEncoder[ComplexNonEncodable1]).getMessage
    assert(errorMsg1.contains(
      s"""root class: "${clsName[ComplexNonEncodable1]}""""))
    assert(errorMsg1.contains(
      s"""field (class: "${clsName[NonEncodable]}", name: "name1")"""))

    val errorMsg2 =
      intercept[UnsupportedOperationException](ExpressionEncoder[ComplexNonEncodable2]).getMessage
    assert(errorMsg2.contains(
      s"""root class: "${clsName[ComplexNonEncodable2]}""""))
    assert(errorMsg2.contains(
      s"""field (class: "${clsName[ComplexNonEncodable1]}", name: "name2")"""))
    assert(errorMsg1.contains(
      s"""field (class: "${clsName[NonEncodable]}", name: "name1")"""))

    val errorMsg3 =
      intercept[UnsupportedOperationException](ExpressionEncoder[ComplexNonEncodable3]).getMessage
    assert(errorMsg3.contains(
      s"""root class: "${clsName[ComplexNonEncodable3]}""""))
    assert(errorMsg3.contains(
      s"""field (class: "scala.Option", name: "name3")"""))
    assert(errorMsg3.contains(
      s"""option value class: "${clsName[NonEncodable]}""""))

    val errorMsg4 =
      intercept[UnsupportedOperationException](ExpressionEncoder[ComplexNonEncodable4]).getMessage
    assert(errorMsg4.contains(
      s"""root class: "${clsName[ComplexNonEncodable4]}""""))
    assert(errorMsg4.contains(
      s"""field (class: "scala.Array", name: "name4")"""))
    assert(errorMsg4.contains(
      s"""array element class: "${clsName[NonEncodable]}""""))

    val errorMsg5 =
      intercept[UnsupportedOperationException](ExpressionEncoder[ComplexNonEncodable5]).getMessage
    assert(errorMsg5.contains(
      s"""root class: "${clsName[ComplexNonEncodable5]}""""))
    assert(errorMsg5.contains(
      s"""field (class: "scala.Option", name: "name5")"""))
    assert(errorMsg5.contains(
      s"""option value class: "scala.Array""""))
    assert(errorMsg5.contains(
      s"""array element class: "${clsName[NonEncodable]}""""))
  }

  private def clsName[T : ClassTag]: String = implicitly[ClassTag[T]].runtimeClass.getName
}
