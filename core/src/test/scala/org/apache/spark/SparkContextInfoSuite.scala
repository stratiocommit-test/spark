/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark

import org.scalatest.Assertions

import org.apache.spark.storage.StorageLevel

class SparkContextInfoSuite extends SparkFunSuite with LocalSparkContext {
  test("getPersistentRDDs only returns RDDs that are marked as cached") {
    sc = new SparkContext("local", "test")
    assert(sc.getPersistentRDDs.isEmpty === true)

    val rdd = sc.makeRDD(Array(1, 2, 3, 4), 2)
    assert(sc.getPersistentRDDs.isEmpty === true)

    rdd.cache()
    assert(sc.getPersistentRDDs.size === 1)
    assert(sc.getPersistentRDDs.values.head === rdd)
  }

  test("getPersistentRDDs returns an immutable map") {
    sc = new SparkContext("local", "test")
    val rdd1 = sc.makeRDD(Array(1, 2, 3, 4), 2).cache()
    val myRdds = sc.getPersistentRDDs
    assert(myRdds.size === 1)
    assert(myRdds(0) === rdd1)
    assert(myRdds(0).getStorageLevel === StorageLevel.MEMORY_ONLY)

    // myRdds2 should have 2 RDDs, but myRdds should not change
    val rdd2 = sc.makeRDD(Array(5, 6, 7, 8), 1).cache()
    val myRdds2 = sc.getPersistentRDDs
    assert(myRdds2.size === 2)
    assert(myRdds2(0) === rdd1)
    assert(myRdds2(1) === rdd2)
    assert(myRdds2(0).getStorageLevel === StorageLevel.MEMORY_ONLY)
    assert(myRdds2(1).getStorageLevel === StorageLevel.MEMORY_ONLY)
    assert(myRdds.size === 1)
    assert(myRdds(0) === rdd1)
    assert(myRdds(0).getStorageLevel === StorageLevel.MEMORY_ONLY)
  }

  test("getRDDStorageInfo only reports on RDDs that actually persist data") {
    sc = new SparkContext("local", "test")
    val rdd = sc.makeRDD(Array(1, 2, 3, 4), 2).cache()
    assert(sc.getRDDStorageInfo.size === 0)
    rdd.collect()
    assert(sc.getRDDStorageInfo.size === 1)
    assert(sc.getRDDStorageInfo.head.isCached)
    assert(sc.getRDDStorageInfo.head.memSize > 0)
    assert(sc.getRDDStorageInfo.head.storageLevel === StorageLevel.MEMORY_ONLY)
  }

  test("call sites report correct locations") {
    sc = new SparkContext("local", "test")
    testPackage.runCallSiteTest(sc)
  }
}

/** Call site must be outside of usual org.apache.spark packages (see Utils#SPARK_CLASS_REGEX). */
package object testPackage extends Assertions {
  private val CALL_SITE_REGEX = "(.+) at (.+):([0-9]+)".r

  def runCallSiteTest(sc: SparkContext) {
    val rdd = sc.makeRDD(Array(1, 2, 3, 4), 2)
    val rddCreationSite = rdd.getCreationSite
    val curCallSite = sc.getCallSite().shortForm // note: 2 lines after definition of "rdd"

    val rddCreationLine = rddCreationSite match {
      case CALL_SITE_REGEX(func, file, line) =>
        assert(func === "makeRDD")
        assert(file === "SparkContextInfoSuite.scala")
        line.toInt
      case _ => fail("Did not match expected call site format")
    }

    curCallSite match {
      case CALL_SITE_REGEX(func, file, line) =>
        assert(func === "getCallSite") // this is correct because we called it from outside of Spark
        assert(file === "SparkContextInfoSuite.scala")
        assert(line.toInt === rddCreationLine.toInt + 2)
      case _ => fail("Did not match expected call site format")
    }
  }
}
