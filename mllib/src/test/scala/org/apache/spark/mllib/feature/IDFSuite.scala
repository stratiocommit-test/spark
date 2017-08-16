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
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._

class IDFSuite extends SparkFunSuite with MLlibTestSparkContext {

  test("idf") {
    val n = 4
    val localTermFrequencies = Seq(
      Vectors.sparse(n, Array(1, 3), Array(1.0, 2.0)),
      Vectors.dense(0.0, 1.0, 2.0, 3.0),
      Vectors.sparse(n, Array(1), Array(1.0))
    )
    val m = localTermFrequencies.size
    val termFrequencies = sc.parallelize(localTermFrequencies, 2)
    val idf = new IDF
    val model = idf.fit(termFrequencies)
    val expected = Vectors.dense(Array(0, 3, 1, 2).map { x =>
      math.log((m + 1.0) / (x + 1.0))
    })
    assert(model.idf ~== expected absTol 1e-12)

    val assertHelper = (tfidf: Array[Vector]) => {
      assert(tfidf.size === 3)
      val tfidf0 = tfidf(0).asInstanceOf[SparseVector]
      assert(tfidf0.indices === Array(1, 3))
      assert(Vectors.dense(tfidf0.values) ~==
          Vectors.dense(1.0 * expected(1), 2.0 * expected(3)) absTol 1e-12)
      val tfidf1 = tfidf(1).asInstanceOf[DenseVector]
      assert(Vectors.dense(tfidf1.values) ~==
          Vectors.dense(0.0, 1.0 * expected(1), 2.0 * expected(2), 3.0 * expected(3)) absTol 1e-12)
      val tfidf2 = tfidf(2).asInstanceOf[SparseVector]
      assert(tfidf2.indices === Array(1))
      assert(tfidf2.values(0) ~== (1.0 * expected(1)) absTol 1e-12)
    }
    // Transforms a RDD
    val tfidf = model.transform(termFrequencies).collect()
    assertHelper(tfidf)
    // Transforms local vectors
    val localTfidf = localTermFrequencies.map(model.transform(_)).toArray
    assertHelper(localTfidf)
  }

  test("idf minimum document frequency filtering") {
    val n = 4
    val localTermFrequencies = Seq(
      Vectors.sparse(n, Array(1, 3), Array(1.0, 2.0)),
      Vectors.dense(0.0, 1.0, 2.0, 3.0),
      Vectors.sparse(n, Array(1), Array(1.0))
    )
    val m = localTermFrequencies.size
    val termFrequencies = sc.parallelize(localTermFrequencies, 2)
    val idf = new IDF(minDocFreq = 1)
    val model = idf.fit(termFrequencies)
    val expected = Vectors.dense(Array(0, 3, 1, 2).map { x =>
      if (x > 0) {
        math.log((m + 1.0) / (x + 1.0))
      } else {
        0
      }
    })
    assert(model.idf ~== expected absTol 1e-12)

    val assertHelper = (tfidf: Array[Vector]) => {
      assert(tfidf.size === 3)
      val tfidf0 = tfidf(0).asInstanceOf[SparseVector]
      assert(tfidf0.indices === Array(1, 3))
      assert(Vectors.dense(tfidf0.values) ~==
          Vectors.dense(1.0 * expected(1), 2.0 * expected(3)) absTol 1e-12)
      val tfidf1 = tfidf(1).asInstanceOf[DenseVector]
      assert(Vectors.dense(tfidf1.values) ~==
          Vectors.dense(0.0, 1.0 * expected(1), 2.0 * expected(2), 3.0 * expected(3)) absTol 1e-12)
      val tfidf2 = tfidf(2).asInstanceOf[SparseVector]
      assert(tfidf2.indices === Array(1))
      assert(tfidf2.values(0) ~== (1.0 * expected(1)) absTol 1e-12)
    }
    // Transforms a RDD
    val tfidf = model.transform(termFrequencies).collect()
    assertHelper(tfidf)
    // Transforms local vectors
    val localTfidf = localTermFrequencies.map(model.transform(_)).toArray
    assertHelper(localTfidf)
  }

}
