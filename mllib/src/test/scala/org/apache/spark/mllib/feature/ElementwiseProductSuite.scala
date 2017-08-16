/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.mllib.feature

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vectors}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._

class ElementwiseProductSuite extends SparkFunSuite with MLlibTestSparkContext {

  test("elementwise (hadamard) product should properly apply vector to dense data set") {
    val denseData = Array(
      Vectors.dense(1.0, 4.0, 1.9, -9.0)
    )
    val scalingVec = Vectors.dense(2.0, 0.5, 0.0, 0.25)
    val transformer = new ElementwiseProduct(scalingVec)
    val transformedData = transformer.transform(sc.makeRDD(denseData))
    val transformedVecs = transformedData.collect()
    val transformedVec = transformedVecs(0)
    val expectedVec = Vectors.dense(2.0, 2.0, 0.0, -2.25)
    assert(transformedVec ~== expectedVec absTol 1E-5,
      s"Expected transformed vector $expectedVec but found $transformedVec")
  }

  test("elementwise (hadamard) product should properly apply vector to sparse data set") {
    val sparseData = Array(
      Vectors.sparse(3, Seq((1, -1.0), (2, -3.0)))
    )
    val dataRDD = sc.parallelize(sparseData, 3)
    val scalingVec = Vectors.dense(1.0, 0.0, 0.5)
    val transformer = new ElementwiseProduct(scalingVec)
    val data2 = sparseData.map(transformer.transform)
    val data2RDD = transformer.transform(dataRDD)

    assert((sparseData, data2, data2RDD.collect()).zipped.forall {
      case (v1: DenseVector, v2: DenseVector, v3: DenseVector) => true
      case (v1: SparseVector, v2: SparseVector, v3: SparseVector) => true
      case _ => false
    }, "The vector type should be preserved after hadamard product")

    assert((data2, data2RDD.collect()).zipped.forall((v1, v2) => v1 ~== v2 absTol 1E-5))
    assert(data2(0) ~== Vectors.sparse(3, Seq((1, 0.0), (2, -1.5))) absTol 1E-5)
  }
}
