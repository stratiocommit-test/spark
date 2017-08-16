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

import org.apache.spark.{SharedSparkContext, SparkFunSuite}
import org.apache.spark.util.random.{BernoulliSampler, PoissonSampler, RandomSampler}

/** a sampler that outputs its seed */
class MockSampler extends RandomSampler[Long, Long] {

  private var s: Long = _

  override def setSeed(seed: Long) {
    s = seed
  }

  override def sample(): Int = 1

  override def sample(items: Iterator[Long]): Iterator[Long] = {
    Iterator(s)
  }

  override def clone: MockSampler = new MockSampler
}

class PartitionwiseSampledRDDSuite extends SparkFunSuite with SharedSparkContext {

  test("seed distribution") {
    val rdd = sc.makeRDD(Array(1L, 2L, 3L, 4L), 2)
    val sampler = new MockSampler
    val sample = new PartitionwiseSampledRDD[Long, Long](rdd, sampler, false, 0L)
    assert(sample.distinct().count == 2, "Seeds must be different.")
  }

  test("concurrency") {
    // SPARK-2251: zip with self computes each partition twice.
    // We want to make sure there are no concurrency issues.
    val rdd = sc.parallelize(0 until 111, 10)
    for (sampler <- Seq(new BernoulliSampler[Int](0.5), new PoissonSampler[Int](0.5))) {
      val sampled = new PartitionwiseSampledRDD[Int, Int](rdd, sampler, true)
      sampled.zip(sampled).count()
    }
  }
}

