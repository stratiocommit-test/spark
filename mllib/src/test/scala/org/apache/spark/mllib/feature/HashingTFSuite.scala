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
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._

class HashingTFSuite extends SparkFunSuite with MLlibTestSparkContext {

  test("hashing tf on a single doc") {
    val hashingTF = new HashingTF(1000)
    val doc = "a a b b c d".split(" ")
    val n = hashingTF.numFeatures
    val termFreqs = Seq(
      (hashingTF.indexOf("a"), 2.0),
      (hashingTF.indexOf("b"), 2.0),
      (hashingTF.indexOf("c"), 1.0),
      (hashingTF.indexOf("d"), 1.0))
    assert(termFreqs.map(_._1).forall(i => i >= 0 && i < n),
      "index must be in range [0, #features)")
    assert(termFreqs.map(_._1).toSet.size === 4, "expecting perfect hashing")
    val expected = Vectors.sparse(n, termFreqs)
    assert(hashingTF.transform(doc) === expected)
  }

  test("hashing tf on an RDD") {
    val hashingTF = new HashingTF
    val localDocs: Seq[Seq[String]] = Seq(
      "a a b b b c d".split(" "),
      "a b c d a b c".split(" "),
      "c b a c b a a".split(" "))
    val docs = sc.parallelize(localDocs, 2)
    assert(hashingTF.transform(docs).collect().toSet === localDocs.map(hashingTF.transform).toSet)
  }

  test("applying binary term freqs") {
    val hashingTF = new HashingTF(100).setBinary(true)
    val doc = "a a b c c c".split(" ")
    val n = hashingTF.numFeatures
    val expected = Vectors.sparse(n, Seq(
      (hashingTF.indexOf("a"), 1.0),
      (hashingTF.indexOf("b"), 1.0),
      (hashingTF.indexOf("c"), 1.0)))
    assert(hashingTF.transform(doc) ~== expected absTol 1e-14)
  }
}
