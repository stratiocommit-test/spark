/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.shuffle

import org.apache.spark._

case class KeyClass()

case class ValueClass()

case class CombinerClass()

class ShuffleDependencySuite extends SparkFunSuite with LocalSparkContext {

  val conf = new SparkConf(loadDefaults = false)

  test("key, value, and combiner classes correct in shuffle dependency without aggregation") {
    sc = new SparkContext("local", "test", conf.clone())
    val rdd = sc.parallelize(1 to 5, 4)
      .map(key => (KeyClass(), ValueClass()))
      .groupByKey()
    val dep = rdd.dependencies.head.asInstanceOf[ShuffleDependency[_, _, _]]
    assert(!dep.mapSideCombine, "Test requires that no map-side aggregator is defined")
    assert(dep.keyClassName == classOf[KeyClass].getName)
    assert(dep.valueClassName == classOf[ValueClass].getName)
  }

  test("key, value, and combiner classes available in shuffle dependency with aggregation") {
    sc = new SparkContext("local", "test", conf.clone())
    val rdd = sc.parallelize(1 to 5, 4)
      .map(key => (KeyClass(), ValueClass()))
      .aggregateByKey(CombinerClass())({ case (a, b) => a }, { case (a, b) => a })
    val dep = rdd.dependencies.head.asInstanceOf[ShuffleDependency[_, _, _]]
    assert(dep.mapSideCombine && dep.aggregator.isDefined, "Test requires map-side aggregation")
    assert(dep.keyClassName == classOf[KeyClass].getName)
    assert(dep.valueClassName == classOf[ValueClass].getName)
    assert(dep.combinerClassName == Some(classOf[CombinerClass].getName))
  }

  test("combineByKey null combiner class tag handled correctly") {
    sc = new SparkContext("local", "test", conf.clone())
    val rdd = sc.parallelize(1 to 5, 4)
      .map(key => (KeyClass(), ValueClass()))
      .combineByKey((v: ValueClass) => v,
        (c: AnyRef, v: ValueClass) => c,
        (c1: AnyRef, c2: AnyRef) => c1)
    val dep = rdd.dependencies.head.asInstanceOf[ShuffleDependency[_, _, _]]
    assert(dep.keyClassName == classOf[KeyClass].getName)
    assert(dep.valueClassName == classOf[ValueClass].getName)
    assert(dep.combinerClassName == None)
  }

}
