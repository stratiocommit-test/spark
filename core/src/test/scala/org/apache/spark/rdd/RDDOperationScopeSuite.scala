/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.rdd

import org.scalatest.BeforeAndAfter

import org.apache.spark.{Partition, SparkContext, SparkFunSuite, TaskContext}

/**
 * Tests whether scopes are passed from the RDD operation to the RDDs correctly.
 */
class RDDOperationScopeSuite extends SparkFunSuite with BeforeAndAfter {
  private var sc: SparkContext = null
  private val scope1 = new RDDOperationScope("scope1")
  private val scope2 = new RDDOperationScope("scope2", Some(scope1))
  private val scope3 = new RDDOperationScope("scope3", Some(scope2))

  before {
    sc = new SparkContext("local", "test")
  }

  after {
    sc.stop()
  }

  test("equals and hashCode") {
    val opScope1 = new RDDOperationScope("scope1", id = "1")
    val opScope2 = new RDDOperationScope("scope1", id = "1")
    assert(opScope1 === opScope2)
    assert(opScope1.hashCode() === opScope2.hashCode())
  }

  test("getAllScopes") {
    assert(scope1.getAllScopes === Seq(scope1))
    assert(scope2.getAllScopes === Seq(scope1, scope2))
    assert(scope3.getAllScopes === Seq(scope1, scope2, scope3))
  }

  test("json de/serialization") {
    val scope1Json = scope1.toJson
    val scope2Json = scope2.toJson
    val scope3Json = scope3.toJson
    assert(scope1Json === s"""{"id":"${scope1.id}","name":"scope1"}""")
    assert(scope2Json === s"""{"id":"${scope2.id}","name":"scope2","parent":$scope1Json}""")
    assert(scope3Json === s"""{"id":"${scope3.id}","name":"scope3","parent":$scope2Json}""")
    assert(RDDOperationScope.fromJson(scope1Json) === scope1)
    assert(RDDOperationScope.fromJson(scope2Json) === scope2)
    assert(RDDOperationScope.fromJson(scope3Json) === scope3)
  }

  test("withScope") {
    val rdd0: MyCoolRDD = new MyCoolRDD(sc)
    var rdd1: MyCoolRDD = null
    var rdd2: MyCoolRDD = null
    var rdd3: MyCoolRDD = null
    RDDOperationScope.withScope(sc, "scope1", allowNesting = false, ignoreParent = false) {
      rdd1 = new MyCoolRDD(sc)
      RDDOperationScope.withScope(sc, "scope2", allowNesting = false, ignoreParent = false) {
        rdd2 = new MyCoolRDD(sc)
        RDDOperationScope.withScope(sc, "scope3", allowNesting = false, ignoreParent = false) {
          rdd3 = new MyCoolRDD(sc)
        }
      }
    }
    assert(rdd0.scope.isEmpty)
    assert(rdd1.scope.isDefined)
    assert(rdd2.scope.isDefined)
    assert(rdd3.scope.isDefined)
    assert(rdd1.scope.get.getAllScopes.map(_.name) === Seq("scope1"))
    assert(rdd2.scope.get.getAllScopes.map(_.name) === Seq("scope1"))
    assert(rdd3.scope.get.getAllScopes.map(_.name) === Seq("scope1"))
  }

  test("withScope with partial nesting") {
    val rdd0: MyCoolRDD = new MyCoolRDD(sc)
    var rdd1: MyCoolRDD = null
    var rdd2: MyCoolRDD = null
    var rdd3: MyCoolRDD = null
    // allow nesting here
    RDDOperationScope.withScope(sc, "scope1", allowNesting = true, ignoreParent = false) {
      rdd1 = new MyCoolRDD(sc)
      // stop nesting here
      RDDOperationScope.withScope(sc, "scope2", allowNesting = false, ignoreParent = false) {
        rdd2 = new MyCoolRDD(sc)
        RDDOperationScope.withScope(sc, "scope3", allowNesting = false, ignoreParent = false) {
          rdd3 = new MyCoolRDD(sc)
        }
      }
    }
    assert(rdd0.scope.isEmpty)
    assert(rdd1.scope.isDefined)
    assert(rdd2.scope.isDefined)
    assert(rdd3.scope.isDefined)
    assert(rdd1.scope.get.getAllScopes.map(_.name) === Seq("scope1"))
    assert(rdd2.scope.get.getAllScopes.map(_.name) === Seq("scope1", "scope2"))
    assert(rdd3.scope.get.getAllScopes.map(_.name) === Seq("scope1", "scope2"))
  }

  test("withScope with multiple layers of nesting") {
    val rdd0: MyCoolRDD = new MyCoolRDD(sc)
    var rdd1: MyCoolRDD = null
    var rdd2: MyCoolRDD = null
    var rdd3: MyCoolRDD = null
    RDDOperationScope.withScope(sc, "scope1", allowNesting = true, ignoreParent = false) {
      rdd1 = new MyCoolRDD(sc)
      RDDOperationScope.withScope(sc, "scope2", allowNesting = true, ignoreParent = false) {
        rdd2 = new MyCoolRDD(sc)
        RDDOperationScope.withScope(sc, "scope3", allowNesting = true, ignoreParent = false) {
          rdd3 = new MyCoolRDD(sc)
        }
      }
    }
    assert(rdd0.scope.isEmpty)
    assert(rdd1.scope.isDefined)
    assert(rdd2.scope.isDefined)
    assert(rdd3.scope.isDefined)
    assert(rdd1.scope.get.getAllScopes.map(_.name) === Seq("scope1"))
    assert(rdd2.scope.get.getAllScopes.map(_.name) === Seq("scope1", "scope2"))
    assert(rdd3.scope.get.getAllScopes.map(_.name) === Seq("scope1", "scope2", "scope3"))
  }

}

private class MyCoolRDD(sc: SparkContext) extends RDD[Int](sc, Nil) {
  override def getPartitions: Array[Partition] = Array.empty
  override def compute(p: Partition, context: TaskContext): Iterator[Int] = { Nil.toIterator }
}
