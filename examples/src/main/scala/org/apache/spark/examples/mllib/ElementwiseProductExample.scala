/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
// scalastyle:off println
package org.apache.spark.examples.mllib

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
// $example on$
import org.apache.spark.mllib.feature.ElementwiseProduct
import org.apache.spark.mllib.linalg.Vectors
// $example off$

object ElementwiseProductExample {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("ElementwiseProductExample")
    val sc = new SparkContext(conf)

    // $example on$
    // Create some vector data; also works for sparse vectors
    val data = sc.parallelize(Array(Vectors.dense(1.0, 2.0, 3.0), Vectors.dense(4.0, 5.0, 6.0)))

    val transformingVector = Vectors.dense(0.0, 1.0, 2.0)
    val transformer = new ElementwiseProduct(transformingVector)

    // Batch transform and per-row transform give the same results:
    val transformedData = transformer.transform(data)
    val transformedData2 = data.map(x => transformer.transform(x))
    // $example off$

    println("transformedData: ")
    transformedData.foreach(x => println(x))

    println("transformedData2: ")
    transformedData2.foreach(x => println(x))

    sc.stop()
  }
}
// scalastyle:on println
