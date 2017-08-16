/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.api.python

import org.apache.spark.{SharedSparkContext, SparkFunSuite}

class SerDeUtilSuite extends SparkFunSuite with SharedSparkContext {

  test("Converting an empty pair RDD to python does not throw an exception (SPARK-5441)") {
    val emptyRdd = sc.makeRDD(Seq[(Any, Any)]())
    SerDeUtil.pairRDDToPython(emptyRdd, 10)
  }

  test("Converting an empty python RDD to pair RDD does not throw an exception (SPARK-5441)") {
    val emptyRdd = sc.makeRDD(Seq[(Any, Any)]())
    val javaRdd = emptyRdd.toJavaRDD()
    val pythonRdd = SerDeUtil.javaToPython(javaRdd)
    SerDeUtil.pythonToPairRDD(pythonRdd, false)
  }
}

