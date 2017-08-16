/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.util.random

import scala.language.reflectiveCalls

import org.apache.commons.math3.stat.inference.ChiSquareTest
import org.scalatest.Matchers

import org.apache.spark.SparkFunSuite
import org.apache.spark.util.Utils.times

class XORShiftRandomSuite extends SparkFunSuite with Matchers {

  private def fixture = new {
    val seed = 1L
    val xorRand = new XORShiftRandom(seed)
    val hundMil = 1e8.toInt
  }

  /*
   * This test is based on a chi-squared test for randomness.
   */
  test ("XORShift generates valid random numbers") {

    val f = fixture

    val numBins = 10 // create 10 bins
    val numRows = 5 // create 5 rows
    val bins = Array.ofDim[Long](numRows, numBins)

    // populate bins based on modulus of the random number for each row
    for (r <- 0 to numRows-1) {
      times(f.hundMil) {bins(r)(math.abs(f.xorRand.nextInt) % numBins) += 1}
    }

    /*
     * Perform the chi square test on the 5 rows of randomly generated numbers evenly divided into
     * 10 bins. chiSquareTest returns true iff the null hypothesis (that the classifications
     * represented by the counts in the columns of the input 2-way table are independent of the
     * rows) can be rejected with 100 * (1 - alpha) percent confidence, where alpha is prespecified
     * as 0.05
     */
    val chiTest = new ChiSquareTest
    assert(chiTest.chiSquareTest(bins, 0.05) === false)
  }

  test ("XORShift with zero seed") {
    val random = new XORShiftRandom(0L)
    assert(random.nextInt() != 0)
  }

  test ("hashSeed has random bits throughout") {
    val totalBitCount = (0 until 10).map { seed =>
      val hashed = XORShiftRandom.hashSeed(seed)
      val bitCount = java.lang.Long.bitCount(hashed)
      // make sure we have roughly equal numbers of 0s and 1s.  Mostly just check that we
      // don't have all 0s or 1s in the high bits
      bitCount should be > 20
      bitCount should be < 44
      bitCount
    }.sum
    // and over all the seeds, very close to equal numbers of 0s & 1s
    totalBitCount should be > (32 * 10 - 30)
    totalBitCount should be < (32 * 10 + 30)
  }
}
