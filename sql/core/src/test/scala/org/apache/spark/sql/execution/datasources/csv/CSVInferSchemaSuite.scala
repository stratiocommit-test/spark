/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.execution.datasources.csv

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.types._

class CSVInferSchemaSuite extends SparkFunSuite {

  test("String fields types are inferred correctly from null types") {
    val options = new CSVOptions(Map.empty[String, String])
    assert(CSVInferSchema.inferField(NullType, "", options) == NullType)
    assert(CSVInferSchema.inferField(NullType, null, options) == NullType)
    assert(CSVInferSchema.inferField(NullType, "100000000000", options) == LongType)
    assert(CSVInferSchema.inferField(NullType, "60", options) == IntegerType)
    assert(CSVInferSchema.inferField(NullType, "3.5", options) == DoubleType)
    assert(CSVInferSchema.inferField(NullType, "test", options) == StringType)
    assert(CSVInferSchema.inferField(NullType, "2015-08-20 15:57:00", options) == TimestampType)
    assert(CSVInferSchema.inferField(NullType, "True", options) == BooleanType)
    assert(CSVInferSchema.inferField(NullType, "FAlSE", options) == BooleanType)

    val textValueOne = Long.MaxValue.toString + "0"
    val decimalValueOne = new java.math.BigDecimal(textValueOne)
    val expectedTypeOne = DecimalType(decimalValueOne.precision, decimalValueOne.scale)
    assert(CSVInferSchema.inferField(NullType, textValueOne, options) == expectedTypeOne)
  }

  test("String fields types are inferred correctly from other types") {
    val options = new CSVOptions(Map.empty[String, String])
    assert(CSVInferSchema.inferField(LongType, "1.0", options) == DoubleType)
    assert(CSVInferSchema.inferField(LongType, "test", options) == StringType)
    assert(CSVInferSchema.inferField(IntegerType, "1.0", options) == DoubleType)
    assert(CSVInferSchema.inferField(DoubleType, null, options) == DoubleType)
    assert(CSVInferSchema.inferField(DoubleType, "test", options) == StringType)
    assert(CSVInferSchema.inferField(LongType, "2015-08-20 14:57:00", options) == TimestampType)
    assert(CSVInferSchema.inferField(DoubleType, "2015-08-20 15:57:00", options) == TimestampType)
    assert(CSVInferSchema.inferField(LongType, "True", options) == BooleanType)
    assert(CSVInferSchema.inferField(IntegerType, "FALSE", options) == BooleanType)
    assert(CSVInferSchema.inferField(TimestampType, "FALSE", options) == BooleanType)

    val textValueOne = Long.MaxValue.toString + "0"
    val decimalValueOne = new java.math.BigDecimal(textValueOne)
    val expectedTypeOne = DecimalType(decimalValueOne.precision, decimalValueOne.scale)
    assert(CSVInferSchema.inferField(IntegerType, textValueOne, options) == expectedTypeOne)
  }

  test("Timestamp field types are inferred correctly via custom data format") {
    var options = new CSVOptions(Map("timestampFormat" -> "yyyy-mm"))
    assert(CSVInferSchema.inferField(TimestampType, "2015-08", options) == TimestampType)
    options = new CSVOptions(Map("timestampFormat" -> "yyyy"))
    assert(CSVInferSchema.inferField(TimestampType, "2015", options) == TimestampType)
  }

  test("Timestamp field types are inferred correctly from other types") {
    val options = new CSVOptions(Map.empty[String, String])
    assert(CSVInferSchema.inferField(IntegerType, "2015-08-20 14", options) == StringType)
    assert(CSVInferSchema.inferField(DoubleType, "2015-08-20 14:10", options) == StringType)
    assert(CSVInferSchema.inferField(LongType, "2015-08 14:49:00", options) == StringType)
  }

  test("Boolean fields types are inferred correctly from other types") {
    val options = new CSVOptions(Map.empty[String, String])
    assert(CSVInferSchema.inferField(LongType, "Fale", options) == StringType)
    assert(CSVInferSchema.inferField(DoubleType, "TRUEe", options) == StringType)
  }

  test("Type arrays are merged to highest common type") {
    assert(
      CSVInferSchema.mergeRowTypes(Array(StringType),
        Array(DoubleType)).deep == Array(StringType).deep)
    assert(
      CSVInferSchema.mergeRowTypes(Array(IntegerType),
        Array(LongType)).deep == Array(LongType).deep)
    assert(
      CSVInferSchema.mergeRowTypes(Array(DoubleType),
        Array(LongType)).deep == Array(DoubleType).deep)
  }

  test("Null fields are handled properly when a nullValue is specified") {
    var options = new CSVOptions(Map("nullValue" -> "null"))
    assert(CSVInferSchema.inferField(NullType, "null", options) == NullType)
    assert(CSVInferSchema.inferField(StringType, "null", options) == StringType)
    assert(CSVInferSchema.inferField(LongType, "null", options) == LongType)

    options = new CSVOptions(Map("nullValue" -> "\\N"))
    assert(CSVInferSchema.inferField(IntegerType, "\\N", options) == IntegerType)
    assert(CSVInferSchema.inferField(DoubleType, "\\N", options) == DoubleType)
    assert(CSVInferSchema.inferField(TimestampType, "\\N", options) == TimestampType)
    assert(CSVInferSchema.inferField(BooleanType, "\\N", options) == BooleanType)
    assert(CSVInferSchema.inferField(DecimalType(1, 1), "\\N", options) == DecimalType(1, 1))
  }

  test("Merging Nulltypes should yield Nulltype.") {
    val mergedNullTypes = CSVInferSchema.mergeRowTypes(Array(NullType), Array(NullType))
    assert(mergedNullTypes.deep == Array(NullType).deep)
  }

  test("SPARK-18433: Improve DataSource option keys to be more case-insensitive") {
    val options = new CSVOptions(Map("TiMeStampFormat" -> "yyyy-mm"))
    assert(CSVInferSchema.inferField(TimestampType, "2015-08", options) == TimestampType)
  }
}
