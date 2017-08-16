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
import org.apache.spark.storage.StorageLevel


class DatasetCacheSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  test("get storage level") {
    val ds1 = Seq("1", "2").toDS().as("a")
    val ds2 = Seq(2, 3).toDS().as("b")

    // default storage level
    ds1.persist()
    ds2.cache()
    assert(ds1.storageLevel == StorageLevel.MEMORY_AND_DISK)
    assert(ds2.storageLevel == StorageLevel.MEMORY_AND_DISK)
    // unpersist
    ds1.unpersist()
    assert(ds1.storageLevel == StorageLevel.NONE)
    // non-default storage level
    ds1.persist(StorageLevel.MEMORY_ONLY_2)
    assert(ds1.storageLevel == StorageLevel.MEMORY_ONLY_2)
    // joined Dataset should not be persisted
    val joined = ds1.joinWith(ds2, $"a.value" === $"b.value")
    assert(joined.storageLevel == StorageLevel.NONE)
  }

  test("persist and unpersist") {
    val ds = Seq(("a", 1), ("b", 2), ("c", 3)).toDS().select(expr("_2 + 1").as[Int])
    val cached = ds.cache()
    // count triggers the caching action. It should not throw.
    cached.count()
    // Make sure, the Dataset is indeed cached.
    assertCached(cached)
    // Check result.
    checkDataset(
      cached,
      2, 3, 4)
    // Drop the cache.
    cached.unpersist()
    assert(cached.storageLevel == StorageLevel.NONE, "The Dataset should not be cached.")
  }

  test("persist and then rebind right encoder when join 2 datasets") {
    val ds1 = Seq("1", "2").toDS().as("a")
    val ds2 = Seq(2, 3).toDS().as("b")

    ds1.persist()
    assertCached(ds1)
    ds2.persist()
    assertCached(ds2)

    val joined = ds1.joinWith(ds2, $"a.value" === $"b.value")
    checkDataset(joined, ("2", 2))
    assertCached(joined, 2)

    ds1.unpersist()
    assert(ds1.storageLevel == StorageLevel.NONE, "The Dataset ds1 should not be cached.")
    ds2.unpersist()
    assert(ds2.storageLevel == StorageLevel.NONE, "The Dataset ds2 should not be cached.")
  }

  test("persist and then groupBy columns asKey, map") {
    val ds = Seq(("a", 10), ("a", 20), ("b", 1), ("b", 2), ("c", 1)).toDS()
    val grouped = ds.groupByKey(_._1)
    val agged = grouped.mapGroups { case (g, iter) => (g, iter.map(_._2).sum) }
    agged.persist()

    checkDataset(
      agged.filter(_._1 == "b"),
      ("b", 3))
    assertCached(agged.filter(_._1 == "b"))

    ds.unpersist()
    assert(ds.storageLevel == StorageLevel.NONE, "The Dataset ds should not be cached.")
    agged.unpersist()
    assert(agged.storageLevel == StorageLevel.NONE, "The Dataset agged should not be cached.")
  }
}
