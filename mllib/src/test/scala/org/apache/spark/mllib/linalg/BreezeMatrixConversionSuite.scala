/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.mllib.linalg

import breeze.linalg.{CSCMatrix => BSM, DenseMatrix => BDM}

import org.apache.spark.SparkFunSuite

class BreezeMatrixConversionSuite extends SparkFunSuite {
  test("dense matrix to breeze") {
    val mat = Matrices.dense(3, 2, Array(0.0, 1.0, 2.0, 3.0, 4.0, 5.0))
    val breeze = mat.asBreeze.asInstanceOf[BDM[Double]]
    assert(breeze.rows === mat.numRows)
    assert(breeze.cols === mat.numCols)
    assert(breeze.data.eq(mat.asInstanceOf[DenseMatrix].values), "should not copy data")
  }

  test("dense breeze matrix to matrix") {
    val breeze = new BDM[Double](3, 2, Array(0.0, 1.0, 2.0, 3.0, 4.0, 5.0))
    val mat = Matrices.fromBreeze(breeze).asInstanceOf[DenseMatrix]
    assert(mat.numRows === breeze.rows)
    assert(mat.numCols === breeze.cols)
    assert(mat.values.eq(breeze.data), "should not copy data")
    // transposed matrix
    val matTransposed = Matrices.fromBreeze(breeze.t).asInstanceOf[DenseMatrix]
    assert(matTransposed.numRows === breeze.cols)
    assert(matTransposed.numCols === breeze.rows)
    assert(matTransposed.values.eq(breeze.data), "should not copy data")
  }

  test("sparse matrix to breeze") {
    val values = Array(1.0, 2.0, 4.0, 5.0)
    val colPtrs = Array(0, 2, 4)
    val rowIndices = Array(1, 2, 1, 2)
    val mat = Matrices.sparse(3, 2, colPtrs, rowIndices, values)
    val breeze = mat.asBreeze.asInstanceOf[BSM[Double]]
    assert(breeze.rows === mat.numRows)
    assert(breeze.cols === mat.numCols)
    assert(breeze.data.eq(mat.asInstanceOf[SparseMatrix].values), "should not copy data")
  }

  test("sparse breeze matrix to sparse matrix") {
    val values = Array(1.0, 2.0, 4.0, 5.0)
    val colPtrs = Array(0, 2, 4)
    val rowIndices = Array(1, 2, 1, 2)
    val breeze = new BSM[Double](values, 3, 2, colPtrs, rowIndices)
    val mat = Matrices.fromBreeze(breeze).asInstanceOf[SparseMatrix]
    assert(mat.numRows === breeze.rows)
    assert(mat.numCols === breeze.cols)
    assert(mat.values.eq(breeze.data), "should not copy data")
    val matTransposed = Matrices.fromBreeze(breeze.t).asInstanceOf[SparseMatrix]
    assert(matTransposed.numRows === breeze.cols)
    assert(matTransposed.numCols === breeze.rows)
    assert(!matTransposed.values.eq(breeze.data), "has to copy data")
  }
}
