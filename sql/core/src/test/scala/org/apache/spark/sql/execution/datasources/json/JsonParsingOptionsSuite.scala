/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.execution.datasources.json

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.json.JSONOptions
import org.apache.spark.sql.test.SharedSQLContext

/**
 * Test cases for various [[JSONOptions]].
 */
class JsonParsingOptionsSuite extends QueryTest with SharedSQLContext {

  test("allowComments off") {
    val str = """{'name': /* hello */ 'Reynold Xin'}"""
    val rdd = spark.sparkContext.parallelize(Seq(str))
    val df = spark.read.json(rdd)

    assert(df.schema.head.name == "_corrupt_record")
  }

  test("allowComments on") {
    val str = """{'name': /* hello */ 'Reynold Xin'}"""
    val rdd = spark.sparkContext.parallelize(Seq(str))
    val df = spark.read.option("allowComments", "true").json(rdd)

    assert(df.schema.head.name == "name")
    assert(df.first().getString(0) == "Reynold Xin")
  }

  test("allowSingleQuotes off") {
    val str = """{'name': 'Reynold Xin'}"""
    val rdd = spark.sparkContext.parallelize(Seq(str))
    val df = spark.read.option("allowSingleQuotes", "false").json(rdd)

    assert(df.schema.head.name == "_corrupt_record")
  }

  test("allowSingleQuotes on") {
    val str = """{'name': 'Reynold Xin'}"""
    val rdd = spark.sparkContext.parallelize(Seq(str))
    val df = spark.read.json(rdd)

    assert(df.schema.head.name == "name")
    assert(df.first().getString(0) == "Reynold Xin")
  }

  test("allowUnquotedFieldNames off") {
    val str = """{name: 'Reynold Xin'}"""
    val rdd = spark.sparkContext.parallelize(Seq(str))
    val df = spark.read.json(rdd)

    assert(df.schema.head.name == "_corrupt_record")
  }

  test("allowUnquotedFieldNames on") {
    val str = """{name: 'Reynold Xin'}"""
    val rdd = spark.sparkContext.parallelize(Seq(str))
    val df = spark.read.option("allowUnquotedFieldNames", "true").json(rdd)

    assert(df.schema.head.name == "name")
    assert(df.first().getString(0) == "Reynold Xin")
  }

  test("allowNumericLeadingZeros off") {
    val str = """{"age": 0018}"""
    val rdd = spark.sparkContext.parallelize(Seq(str))
    val df = spark.read.json(rdd)

    assert(df.schema.head.name == "_corrupt_record")
  }

  test("allowNumericLeadingZeros on") {
    val str = """{"age": 0018}"""
    val rdd = spark.sparkContext.parallelize(Seq(str))
    val df = spark.read.option("allowNumericLeadingZeros", "true").json(rdd)

    assert(df.schema.head.name == "age")
    assert(df.first().getLong(0) == 18)
  }

  // The following two tests are not really working - need to look into Jackson's
  // JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS.
  ignore("allowNonNumericNumbers off") {
    val str = """{"age": NaN}"""
    val rdd = spark.sparkContext.parallelize(Seq(str))
    val df = spark.read.json(rdd)

    assert(df.schema.head.name == "_corrupt_record")
  }

  ignore("allowNonNumericNumbers on") {
    val str = """{"age": NaN}"""
    val rdd = spark.sparkContext.parallelize(Seq(str))
    val df = spark.read.option("allowNonNumericNumbers", "true").json(rdd)

    assert(df.schema.head.name == "age")
    assert(df.first().getDouble(0).isNaN)
  }

  test("allowBackslashEscapingAnyCharacter off") {
    val str = """{"name": "Cazen Lee", "price": "\$10"}"""
    val rdd = spark.sparkContext.parallelize(Seq(str))
    val df = spark.read.option("allowBackslashEscapingAnyCharacter", "false").json(rdd)

    assert(df.schema.head.name == "_corrupt_record")
  }

  test("allowBackslashEscapingAnyCharacter on") {
    val str = """{"name": "Cazen Lee", "price": "\$10"}"""
    val rdd = spark.sparkContext.parallelize(Seq(str))
    val df = spark.read.option("allowBackslashEscapingAnyCharacter", "true").json(rdd)

    assert(df.schema.head.name == "name")
    assert(df.schema.last.name == "price")
    assert(df.first().getString(0) == "Cazen Lee")
    assert(df.first().getString(1) == "$10")
  }
}
