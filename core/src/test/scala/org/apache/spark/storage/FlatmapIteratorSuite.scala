/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.storage

import org.apache.spark._


class FlatmapIteratorSuite extends SparkFunSuite with LocalSparkContext {
  /* Tests the ability of Spark to deal with user provided iterators from flatMap
   * calls, that may generate more data then available memory. In any
   * memory based persistance Spark will unroll the iterator into an ArrayBuffer
   * for caching, however in the case that the use defines DISK_ONLY persistance,
   * the iterator will be fed directly to the serializer and written to disk.
   *
   * This also tests the ObjectOutputStream reset rate. When serializing using the
   * Java serialization system, the serializer caches objects to prevent writing redundant
   * data, however that stops GC of those objects. By calling 'reset' you flush that
   * info from the serializer, and allow old objects to be GC'd
   */
  test("Flatmap Iterator to Disk") {
    val sconf = new SparkConf().setMaster("local").setAppName("iterator_to_disk_test")
    sc = new SparkContext(sconf)
    val expand_size = 100
    val data = sc.parallelize((1 to 5).toSeq).
      flatMap( x => Stream.range(0, expand_size))
    var persisted = data.persist(StorageLevel.DISK_ONLY)
    assert(persisted.count()===500)
    assert(persisted.filter(_==1).count()===5)
  }

  test("Flatmap Iterator to Memory") {
    val sconf = new SparkConf().setMaster("local").setAppName("iterator_to_disk_test")
    sc = new SparkContext(sconf)
    val expand_size = 100
    val data = sc.parallelize((1 to 5).toSeq).
      flatMap(x => Stream.range(0, expand_size))
    var persisted = data.persist(StorageLevel.MEMORY_ONLY)
    assert(persisted.count()===500)
    assert(persisted.filter(_==1).count()===5)
  }

  test("Serializer Reset") {
    val sconf = new SparkConf().setMaster("local").setAppName("serializer_reset_test")
      .set("spark.serializer.objectStreamReset", "10")
    sc = new SparkContext(sconf)
    val expand_size = 500
    val data = sc.parallelize(Seq(1, 2)).
      flatMap(x => Stream.range(1, expand_size).
      map(y => "%d: string test %d".format(y, x)))
    val persisted = data.persist(StorageLevel.MEMORY_ONLY_SER)
    assert(persisted.filter(_.startsWith("1:")).count()===2)
  }

}
