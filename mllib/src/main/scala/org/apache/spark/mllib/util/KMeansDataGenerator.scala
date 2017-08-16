/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.mllib.util

import scala.util.Random

import org.apache.spark.SparkContext
import org.apache.spark.annotation.{DeveloperApi, Since}
import org.apache.spark.rdd.RDD

/**
 * :: DeveloperApi ::
 * Generate test data for KMeans. This class first chooses k cluster centers
 * from a d-dimensional Gaussian distribution scaled by factor r and then creates a Gaussian
 * cluster with scale 1 around each center.
 */
@DeveloperApi
@Since("0.8.0")
object KMeansDataGenerator {

  /**
   * Generate an RDD containing test data for KMeans.
   *
   * @param sc SparkContext to use for creating the RDD
   * @param numPoints Number of points that will be contained in the RDD
   * @param k Number of clusters
   * @param d Number of dimensions
   * @param r Scaling factor for the distribution of the initial centers
   * @param numPartitions Number of partitions of the generated RDD; default 2
   */
  @Since("0.8.0")
  def generateKMeansRDD(
      sc: SparkContext,
      numPoints: Int,
      k: Int,
      d: Int,
      r: Double,
      numPartitions: Int = 2)
    : RDD[Array[Double]] =
  {
    // First, generate some centers
    val rand = new Random(42)
    val centers = Array.fill(k)(Array.fill(d)(rand.nextGaussian() * r))
    // Then generate points around each center
    sc.parallelize(0 until numPoints, numPartitions).map { idx =>
      val center = centers(idx % k)
      val rand2 = new Random(42 + idx)
      Array.tabulate(d)(i => center(i) + rand2.nextGaussian())
    }
  }

  @Since("0.8.0")
  def main(args: Array[String]) {
    if (args.length < 6) {
      // scalastyle:off println
      println("Usage: KMeansGenerator " +
        "<master> <output_dir> <num_points> <k> <d> <r> [<num_partitions>]")
      // scalastyle:on println
      System.exit(1)
    }

    val sparkMaster = args(0)
    val outputPath = args(1)
    val numPoints = args(2).toInt
    val k = args(3).toInt
    val d = args(4).toInt
    val r = args(5).toDouble
    val parts = if (args.length >= 7) args(6).toInt else 2

    val sc = new SparkContext(sparkMaster, "KMeansDataGenerator")
    val data = generateKMeansRDD(sc, numPoints, k, d, r, parts)
    data.map(_.mkString(" ")).saveAsTextFile(outputPath)

    System.exit(0)
  }
}

