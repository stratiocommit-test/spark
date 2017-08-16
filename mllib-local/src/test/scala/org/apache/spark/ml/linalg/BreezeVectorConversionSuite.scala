/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.ml.linalg

import breeze.linalg.{DenseVector => BDV, SparseVector => BSV}

import org.apache.spark.ml.SparkMLFunSuite

/**
 * Test Breeze vector conversions.
 */
class BreezeVectorConversionSuite extends SparkMLFunSuite {

  val arr = Array(0.1, 0.2, 0.3, 0.4)
  val n = 20
  val indices = Array(0, 3, 5, 10, 13)
  val values = Array(0.1, 0.5, 0.3, -0.8, -1.0)

  test("dense to breeze") {
    val vec = Vectors.dense(arr)
    assert(vec.asBreeze === new BDV[Double](arr))
  }

  test("sparse to breeze") {
    val vec = Vectors.sparse(n, indices, values)
    assert(vec.asBreeze === new BSV[Double](indices, values, n))
  }

  test("dense breeze to vector") {
    val breeze = new BDV[Double](arr)
    val vec = Vectors.fromBreeze(breeze).asInstanceOf[DenseVector]
    assert(vec.size === arr.length)
    assert(vec.values.eq(arr), "should not copy data")
  }

  test("sparse breeze to vector") {
    val breeze = new BSV[Double](indices, values, n)
    val vec = Vectors.fromBreeze(breeze).asInstanceOf[SparseVector]
    assert(vec.size === n)
    assert(vec.indices.eq(indices), "should not copy data")
    assert(vec.values.eq(values), "should not copy data")
  }

  test("sparse breeze with partially-used arrays to vector") {
    val activeSize = 3
    val breeze = new BSV[Double](indices, values, activeSize, n)
    val vec = Vectors.fromBreeze(breeze).asInstanceOf[SparseVector]
    assert(vec.size === n)
    assert(vec.indices === indices.slice(0, activeSize))
    assert(vec.values === values.slice(0, activeSize))
  }
}
