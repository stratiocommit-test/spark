/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql

import scala.util.Random

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.types._

/**
 * Tests of [[RandomDataGenerator]].
 */
class RandomDataGeneratorSuite extends SparkFunSuite {

  /**
   * Tests random data generation for the given type by using it to generate random values then
   * converting those values into their Catalyst equivalents using CatalystTypeConverters.
   */
  def testRandomDataGeneration(dataType: DataType, nullable: Boolean = true): Unit = {
    val toCatalyst = CatalystTypeConverters.createToCatalystConverter(dataType)
    val generator = RandomDataGenerator.forType(dataType, nullable, new Random(33)).getOrElse {
      fail(s"Random data generator was not defined for $dataType")
    }
    if (nullable) {
      assert(Iterator.fill(100)(generator()).contains(null))
    } else {
      assert(!Iterator.fill(100)(generator()).contains(null))
    }
    for (_ <- 1 to 10) {
      val generatedValue = generator()
      toCatalyst(generatedValue)
    }
  }

  // Basic types:
  for (
    dataType <- DataTypeTestUtils.atomicTypes;
    nullable <- Seq(true, false)
    if !dataType.isInstanceOf[DecimalType]) {
    test(s"$dataType (nullable=$nullable)") {
      testRandomDataGeneration(dataType)
    }
  }

  for (
    arrayType <- DataTypeTestUtils.atomicArrayTypes
    if RandomDataGenerator.forType(arrayType.elementType, arrayType.containsNull).isDefined
  ) {
    test(s"$arrayType") {
      testRandomDataGeneration(arrayType)
    }
  }

  val atomicTypesWithDataGenerators =
    DataTypeTestUtils.atomicTypes.filter(RandomDataGenerator.forType(_).isDefined)

  // Complex types:
  for (
    keyType <- atomicTypesWithDataGenerators;
    valueType <- atomicTypesWithDataGenerators
    // Scala's BigDecimal.hashCode can lead to OutOfMemoryError on Scala 2.10 (see SI-6173) and
    // Spark can hit NumberFormatException errors when converting certain BigDecimals (SPARK-8802).
    // For these reasons, we don't support generation of maps with decimal keys.
    if !keyType.isInstanceOf[DecimalType]
  ) {
    val mapType = MapType(keyType, valueType)
    test(s"$mapType") {
      testRandomDataGeneration(mapType)
    }
  }

  for (
    colOneType <- atomicTypesWithDataGenerators;
    colTwoType <- atomicTypesWithDataGenerators
  ) {
    val structType = StructType(StructField("a", colOneType) :: StructField("b", colTwoType) :: Nil)
    test(s"$structType") {
      testRandomDataGeneration(structType)
    }
  }

  test("check size of generated map") {
    val mapType = MapType(IntegerType, IntegerType)
    for (seed <- 1 to 1000) {
      val generator = RandomDataGenerator.forType(
        mapType, nullable = false, rand = new Random(seed)).get
      val maps = Seq.fill(100)(generator().asInstanceOf[Map[Int, Int]])
      val expectedTotalElements = 100 / 2 * RandomDataGenerator.MAX_MAP_SIZE
      val deviation = math.abs(maps.map(_.size).sum - expectedTotalElements)
      assert(deviation.toDouble / expectedTotalElements < 2e-1)
    }
  }
}
