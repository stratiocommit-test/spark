/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.ml.feature

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.DefaultReadWriteTest
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.types.{LongType, StructField, StructType}

class SQLTransformerSuite
  extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  import testImplicits._

  test("params") {
    ParamsSuite.checkParams(new SQLTransformer())
  }

  test("transform numeric data") {
    val original = Seq((0, 1.0, 3.0), (2, 2.0, 5.0)).toDF("id", "v1", "v2")
    val sqlTrans = new SQLTransformer().setStatement(
      "SELECT *, (v1 + v2) AS v3, (v1 * v2) AS v4 FROM __THIS__")
    val result = sqlTrans.transform(original)
    val resultSchema = sqlTrans.transformSchema(original.schema)
    val expected = Seq((0, 1.0, 3.0, 4.0, 3.0), (2, 2.0, 5.0, 7.0, 10.0))
      .toDF("id", "v1", "v2", "v3", "v4")
    assert(result.schema.toString == resultSchema.toString)
    assert(resultSchema == expected.schema)
    assert(result.collect().toSeq == expected.collect().toSeq)
    assert(original.sparkSession.catalog.listTables().count() == 0)
  }

  test("read/write") {
    val t = new SQLTransformer()
      .setStatement("select * from __THIS__")
    testDefaultReadWrite(t)
  }

  test("transformSchema") {
    val df = spark.range(10)
    val outputSchema = new SQLTransformer()
      .setStatement("SELECT id + 1 AS id1 FROM __THIS__")
      .transformSchema(df.schema)
    val expected = StructType(Seq(StructField("id1", LongType, nullable = false)))
    assert(outputSchema === expected)
  }
}
