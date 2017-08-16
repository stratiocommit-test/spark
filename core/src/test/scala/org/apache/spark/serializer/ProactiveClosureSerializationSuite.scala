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

import org.apache.spark.{SharedSparkContext, SparkException, SparkFunSuite}
import org.apache.spark.rdd.RDD

/* A trivial (but unserializable) container for trivial functions */
class UnserializableClass {
  def op[T](x: T): String = x.toString

  def pred[T](x: T): Boolean = x.toString.length % 2 == 0
}

class ProactiveClosureSerializationSuite extends SparkFunSuite with SharedSparkContext {

  def fixture: (RDD[String], UnserializableClass) = {
    (sc.parallelize(0 until 1000).map(_.toString), new UnserializableClass)
  }

  test("throws expected serialization exceptions on actions") {
    val (data, uc) = fixture
    val ex = intercept[SparkException] {
      data.map(uc.op(_)).count()
    }
    assert(ex.getMessage.contains("Task not serializable"))
  }

  // There is probably a cleaner way to eliminate boilerplate here, but we're
  // iterating over a map from transformation names to functions that perform that
  // transformation on a given RDD, creating one test case for each

  for (transformation <-
      Map("map" -> xmap _,
          "flatMap" -> xflatMap _,
          "filter" -> xfilter _,
          "mapPartitions" -> xmapPartitions _,
          "mapPartitionsWithIndex" -> xmapPartitionsWithIndex _)) {
    val (name, xf) = transformation

    test(s"$name transformations throw proactive serialization exceptions") {
      val (data, uc) = fixture
      val ex = intercept[SparkException] {
        xf(data, uc)
      }
      assert(ex.getMessage.contains("Task not serializable"),
        s"RDD.$name doesn't proactively throw NotSerializableException")
    }
  }

  private def xmap(x: RDD[String], uc: UnserializableClass): RDD[String] =
    x.map(y => uc.op(y))

  private def xflatMap(x: RDD[String], uc: UnserializableClass): RDD[String] =
    x.flatMap(y => Seq(uc.op(y)))

  private def xfilter(x: RDD[String], uc: UnserializableClass): RDD[String] =
    x.filter(y => uc.pred(y))

  private def xmapPartitions(x: RDD[String], uc: UnserializableClass): RDD[String] =
    x.mapPartitions(_.map(y => uc.op(y)))

  private def xmapPartitionsWithIndex(x: RDD[String], uc: UnserializableClass): RDD[String] =
    x.mapPartitionsWithIndex((_, it) => it.map(y => uc.op(y)))

}
