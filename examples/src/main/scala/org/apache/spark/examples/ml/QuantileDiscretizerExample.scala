/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.examples.ml

// $example on$
import org.apache.spark.ml.feature.QuantileDiscretizer
// $example off$
import org.apache.spark.sql.SparkSession

object QuantileDiscretizerExample {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("QuantileDiscretizerExample")
      .getOrCreate()

    // $example on$
    val data = Array((0, 18.0), (1, 19.0), (2, 8.0), (3, 5.0), (4, 2.2))
    val df = spark.createDataFrame(data).toDF("id", "hour")
    // $example off$
    // Output of QuantileDiscretizer for such small datasets can depend on the number of
    // partitions. Here we force a single partition to ensure consistent results.
    // Note this is not necessary for normal use cases
        .repartition(1)

    // $example on$
    val discretizer = new QuantileDiscretizer()
      .setInputCol("hour")
      .setOutputCol("result")
      .setNumBuckets(3)

    val result = discretizer.fit(df).transform(df)
    result.show()
    // $example off$

    spark.stop()
  }
}
