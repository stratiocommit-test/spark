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

import org.apache.spark.{SparkConf, SparkContext}

object StratifiedSamplingExample {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("StratifiedSamplingExample")
    val sc = new SparkContext(conf)

    // $example on$
    // an RDD[(K, V)] of any key value pairs
    val data = sc.parallelize(
      Seq((1, 'a'), (1, 'b'), (2, 'c'), (2, 'd'), (2, 'e'), (3, 'f')))

    // specify the exact fraction desired from each key
    val fractions = Map(1 -> 0.1, 2 -> 0.6, 3 -> 0.3)

    // Get an approximate sample from each stratum
    val approxSample = data.sampleByKey(withReplacement = false, fractions = fractions)
    // Get an exact sample from each stratum
    val exactSample = data.sampleByKeyExact(withReplacement = false, fractions = fractions)
    // $example off$

    println("approxSample size is " + approxSample.collect().size.toString)
    approxSample.collect().foreach(println)

    println("exactSample its size is " + exactSample.collect().size.toString)
    exactSample.collect().foreach(println)

    sc.stop()
  }
}
// scalastyle:on println
