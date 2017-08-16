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

import scala.math._

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{RandomDataGenerator, Row}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{GenerateOrdering, LazilyGeneratedOrdering}
import org.apache.spark.sql.types._

class OrderingSuite extends SparkFunSuite with ExpressionEvalHelper {

  def compareArrays(a: Seq[Any], b: Seq[Any], expected: Int): Unit = {
    test(s"compare two arrays: a = $a, b = $b") {
      val dataType = ArrayType(IntegerType)
      val rowType = StructType(StructField("array", dataType, nullable = true) :: Nil)
      val toCatalyst = CatalystTypeConverters.createToCatalystConverter(rowType)
      val rowA = toCatalyst(Row(a)).asInstanceOf[InternalRow]
      val rowB = toCatalyst(Row(b)).asInstanceOf[InternalRow]
      Seq(Ascending, Descending).foreach { direction =>
        val sortOrder = direction match {
          case Ascending => BoundReference(0, dataType, nullable = true).asc
          case Descending => BoundReference(0, dataType, nullable = true).desc
        }
        val expectedCompareResult = direction match {
          case Ascending => signum(expected)
          case Descending => -1 * signum(expected)
        }

        val kryo = new KryoSerializer(new SparkConf).newInstance()
        val intOrdering = new InterpretedOrdering(sortOrder :: Nil)
        val genOrdering = new LazilyGeneratedOrdering(sortOrder :: Nil)
        val kryoIntOrdering = kryo.deserialize[InterpretedOrdering](kryo.serialize(intOrdering))
        val kryoGenOrdering = kryo.deserialize[LazilyGeneratedOrdering](kryo.serialize(genOrdering))

        Seq(intOrdering, genOrdering, kryoIntOrdering, kryoGenOrdering).foreach { ordering =>
          assert(ordering.compare(rowA, rowA) === 0)
          assert(ordering.compare(rowB, rowB) === 0)
          assert(signum(ordering.compare(rowA, rowB)) === expectedCompareResult)
          assert(signum(ordering.compare(rowB, rowA)) === -1 * expectedCompareResult)
        }
      }
    }
  }

  // Two arrays have the same size.
  compareArrays(Seq[Any](), Seq[Any](), 0)
  compareArrays(Seq[Any](1), Seq[Any](1), 0)
  compareArrays(Seq[Any](1, 2), Seq[Any](1, 2), 0)
  compareArrays(Seq[Any](1, 2, 2), Seq[Any](1, 2, 3), -1)

  // Two arrays have different sizes.
  compareArrays(Seq[Any](), Seq[Any](1), -1)
  compareArrays(Seq[Any](1, 2, 3), Seq[Any](1, 2, 3, 4), -1)
  compareArrays(Seq[Any](1, 2, 3), Seq[Any](1, 2, 3, 2), -1)
  compareArrays(Seq[Any](1, 2, 3), Seq[Any](1, 2, 2, 2), 1)

  // Arrays having nulls.
  compareArrays(Seq[Any](1, 2, 3), Seq[Any](1, 2, 3, null), -1)
  compareArrays(Seq[Any](), Seq[Any](null), -1)
  compareArrays(Seq[Any](null), Seq[Any](null), 0)
  compareArrays(Seq[Any](null, null), Seq[Any](null, null), 0)
  compareArrays(Seq[Any](null), Seq[Any](null, null), -1)
  compareArrays(Seq[Any](null), Seq[Any](1), -1)
  compareArrays(Seq[Any](null), Seq[Any](null, 1), -1)
  compareArrays(Seq[Any](null, 1), Seq[Any](1, 1), -1)
  compareArrays(Seq[Any](1, null, 1), Seq[Any](1, null, 1), 0)
  compareArrays(Seq[Any](1, null, 1), Seq[Any](1, null, 2), -1)

  // Test GenerateOrdering for all common types. For each type, we construct random input rows that
  // contain two columns of that type, then for pairs of randomly-generated rows we check that
  // GenerateOrdering agrees with RowOrdering.
  {
    val structType =
      new StructType()
        .add("f1", FloatType, nullable = true)
        .add("f2", ArrayType(BooleanType, containsNull = true), nullable = true)
    val arrayOfStructType = ArrayType(structType)
    val complexTypes = ArrayType(IntegerType) :: structType :: arrayOfStructType :: Nil
    (DataTypeTestUtils.atomicTypes ++ complexTypes ++ Set(NullType)).foreach { dataType =>
      test(s"GenerateOrdering with $dataType") {
        val rowOrdering = InterpretedOrdering.forSchema(Seq(dataType, dataType))
        val genOrdering = GenerateOrdering.generate(
          BoundReference(0, dataType, nullable = true).asc ::
            BoundReference(1, dataType, nullable = true).asc :: Nil)
        val rowType = StructType(
          StructField("a", dataType, nullable = true) ::
            StructField("b", dataType, nullable = true) :: Nil)
        val maybeDataGenerator = RandomDataGenerator.forType(rowType, nullable = false)
        assume(maybeDataGenerator.isDefined)
        val randGenerator = maybeDataGenerator.get
        val toCatalyst = CatalystTypeConverters.createToCatalystConverter(rowType)
        for (_ <- 1 to 50) {
          val a = toCatalyst(randGenerator()).asInstanceOf[InternalRow]
          val b = toCatalyst(randGenerator()).asInstanceOf[InternalRow]
          withClue(s"a = $a, b = $b") {
            assert(genOrdering.compare(a, a) === 0)
            assert(genOrdering.compare(b, b) === 0)
            assert(rowOrdering.compare(a, a) === 0)
            assert(rowOrdering.compare(b, b) === 0)
            assert(signum(genOrdering.compare(a, b)) === -1 * signum(genOrdering.compare(b, a)))
            assert(signum(rowOrdering.compare(a, b)) === -1 * signum(rowOrdering.compare(b, a)))
            assert(
              signum(rowOrdering.compare(a, b)) === signum(genOrdering.compare(a, b)),
              "Generated and non-generated orderings should agree")
          }
        }
      }
    }
  }
}
