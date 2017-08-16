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

import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._

/**
 * An end-to-end test suite specifically for testing Tungsten (Unsafe/CodeGen) mode.
 *
 * This is here for now so I can make sure Tungsten project is tested without refactoring existing
 * end-to-end test infra. In the long run this should just go away.
 */
class DataFrameTungstenSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  test("test simple types") {
    val df = sparkContext.parallelize(Seq((1, 2))).toDF("a", "b")
    assert(df.select(struct("a", "b")).first().getStruct(0) === Row(1, 2))
  }

  test("test struct type") {
    val struct = Row(1, 2L, 3.0F, 3.0)
    val data = sparkContext.parallelize(Seq(Row(1, struct)))

    val schema = new StructType()
      .add("a", IntegerType)
      .add("b",
        new StructType()
          .add("b1", IntegerType)
          .add("b2", LongType)
          .add("b3", FloatType)
          .add("b4", DoubleType))

    val df = spark.createDataFrame(data, schema)
    assert(df.select("b").first() === Row(struct))
  }

  test("test nested struct type") {
    val innerStruct = Row(1, "abcd")
    val outerStruct = Row(1, 2L, 3.0F, 3.0, innerStruct, "efg")
    val data = sparkContext.parallelize(Seq(Row(1, outerStruct)))

    val schema = new StructType()
      .add("a", IntegerType)
      .add("b",
        new StructType()
          .add("b1", IntegerType)
          .add("b2", LongType)
          .add("b3", FloatType)
          .add("b4", DoubleType)
          .add("b5", new StructType()
          .add("b5a", IntegerType)
          .add("b5b", StringType))
          .add("b6", StringType))

    val df = spark.createDataFrame(data, schema)
    assert(df.select("b").first() === Row(outerStruct))
  }
}
