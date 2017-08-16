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
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._

class PCASuite extends SparkFunSuite with MLlibTestSparkContext {

  private val data = Array(
    Vectors.sparse(5, Seq((1, 1.0), (3, 7.0))),
    Vectors.dense(2.0, 0.0, 3.0, 4.0, 5.0),
    Vectors.dense(4.0, 0.0, 0.0, 6.0, 7.0)
  )

  private lazy val dataRDD = sc.parallelize(data, 2)

  test("Correct computing use a PCA wrapper") {
    val k = dataRDD.count().toInt
    val pca = new PCA(k).fit(dataRDD)

    val mat = new RowMatrix(dataRDD)
    val (pc, explainedVariance) = mat.computePrincipalComponentsAndExplainedVariance(k)

    val pca_transform = pca.transform(dataRDD).collect()
    val mat_multiply = mat.multiply(pc).rows.collect()

    pca_transform.zip(mat_multiply).foreach { case (calculated, expected) =>
      assert(calculated ~== expected relTol 1e-8)
    }
    assert(pca.explainedVariance ~== explainedVariance relTol 1e-8)
  }
}
