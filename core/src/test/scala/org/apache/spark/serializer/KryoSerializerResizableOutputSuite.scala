/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.serializer

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.LocalSparkContext
import org.apache.spark.SparkContext
import org.apache.spark.SparkException

class KryoSerializerResizableOutputSuite extends SparkFunSuite {

  // trial and error showed this will not serialize with 1mb buffer
  val x = (1 to 400000).toArray

  test("kryo without resizable output buffer should fail on large array") {
    val conf = new SparkConf(false)
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryoserializer.buffer", "1m")
    conf.set("spark.kryoserializer.buffer.max", "1m")
    val sc = new SparkContext("local", "test", conf)
    intercept[SparkException](sc.parallelize(x).collect())
    LocalSparkContext.stop(sc)
  }

  test("kryo with resizable output buffer should succeed on large array") {
    val conf = new SparkConf(false)
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryoserializer.buffer", "1m")
    conf.set("spark.kryoserializer.buffer.max", "2m")
    val sc = new SparkContext("local", "test", conf)
    assert(sc.parallelize(x).collect() === x)
    LocalSparkContext.stop(sc)
  }
}
