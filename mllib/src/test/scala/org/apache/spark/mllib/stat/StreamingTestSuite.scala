/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.mllib.stat

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.stat.test.{BinarySample, StreamingTest, StreamingTestResult,
  StudentTTest, WelchTTest}
import org.apache.spark.streaming.TestSuiteBase
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.util.StatCounter
import org.apache.spark.util.random.XORShiftRandom

class StreamingTestSuite extends SparkFunSuite with TestSuiteBase {

  override def maxWaitTimeMillis: Int = 30000

  test("accuracy for null hypothesis using welch t-test") {
    // set parameters
    val testMethod = "welch"
    val numBatches = 2
    val pointsPerBatch = 1000
    val meanA = 0
    val stdevA = 0.001
    val meanB = 0
    val stdevB = 0.001

    val model = new StreamingTest()
      .setWindowSize(0)
      .setPeacePeriod(0)
      .setTestMethod(testMethod)

    val input = generateTestData(
      numBatches, pointsPerBatch, meanA, stdevA, meanB, stdevB, 42)

    // setup and run the model
    val ssc = setupStreams(
      input, (inputDStream: DStream[BinarySample]) => model.registerStream(inputDStream))
    val outputBatches = runStreams[StreamingTestResult](ssc, numBatches, numBatches)

    assert(outputBatches.flatten.forall(res =>
      res.pValue > 0.05 && res.method == WelchTTest.methodName))
  }

  test("accuracy for alternative hypothesis using welch t-test") {
    // set parameters
    val testMethod = "welch"
    val numBatches = 2
    val pointsPerBatch = 1000
    val meanA = -10
    val stdevA = 1
    val meanB = 10
    val stdevB = 1

    val model = new StreamingTest()
      .setWindowSize(0)
      .setPeacePeriod(0)
      .setTestMethod(testMethod)

    val input = generateTestData(
      numBatches, pointsPerBatch, meanA, stdevA, meanB, stdevB, 42)

    // setup and run the model
    val ssc = setupStreams(
      input, (inputDStream: DStream[BinarySample]) => model.registerStream(inputDStream))
    val outputBatches = runStreams[StreamingTestResult](ssc, numBatches, numBatches)

    assert(outputBatches.flatten.forall(res =>
      res.pValue < 0.05 && res.method == WelchTTest.methodName))
  }

  test("accuracy for null hypothesis using student t-test") {
    // set parameters
    val testMethod = "student"
    val numBatches = 2
    val pointsPerBatch = 1000
    val meanA = 0
    val stdevA = 0.001
    val meanB = 0
    val stdevB = 0.001

    val model = new StreamingTest()
      .setWindowSize(0)
      .setPeacePeriod(0)
      .setTestMethod(testMethod)

    val input = generateTestData(
      numBatches, pointsPerBatch, meanA, stdevA, meanB, stdevB, 42)

    // setup and run the model
    val ssc = setupStreams(
      input, (inputDStream: DStream[BinarySample]) => model.registerStream(inputDStream))
    val outputBatches = runStreams[StreamingTestResult](ssc, numBatches, numBatches)


    assert(outputBatches.flatten.forall(res =>
      res.pValue > 0.05 && res.method == StudentTTest.methodName))
  }

  test("accuracy for alternative hypothesis using student t-test") {
    // set parameters
    val testMethod = "student"
    val numBatches = 2
    val pointsPerBatch = 1000
    val meanA = -10
    val stdevA = 1
    val meanB = 10
    val stdevB = 1

    val model = new StreamingTest()
      .setWindowSize(0)
      .setPeacePeriod(0)
      .setTestMethod(testMethod)

    val input = generateTestData(
      numBatches, pointsPerBatch, meanA, stdevA, meanB, stdevB, 42)

    // setup and run the model
    val ssc = setupStreams(
      input, (inputDStream: DStream[BinarySample]) => model.registerStream(inputDStream))
    val outputBatches = runStreams[StreamingTestResult](ssc, numBatches, numBatches)

    assert(outputBatches.flatten.forall(res =>
      res.pValue < 0.05 && res.method == StudentTTest.methodName))
  }

  test("batches within same test window are grouped") {
    // set parameters
    val testWindow = 3
    val numBatches = 5
    val pointsPerBatch = 100
    val meanA = -10
    val stdevA = 1
    val meanB = 10
    val stdevB = 1

    val model = new StreamingTest()
      .setWindowSize(testWindow)
      .setPeacePeriod(0)

    val input = generateTestData(
      numBatches, pointsPerBatch, meanA, stdevA, meanB, stdevB, 42)

    // setup and run the model
    val ssc = setupStreams(
      input,
      (inputDStream: DStream[BinarySample]) => model.summarizeByKeyAndWindow(inputDStream))
    val outputBatches = runStreams[(Boolean, StatCounter)](ssc, numBatches, numBatches)
    val outputCounts = outputBatches.flatten.map(_._2.count)

    // number of batches seen so far does not exceed testWindow, expect counts to continue growing
    for (i <- 0 until testWindow) {
      assert(outputCounts.slice(2 * i, 2 * i + 2).forall(_ == (i + 1) * pointsPerBatch / 2))
    }

    // number of batches seen exceeds testWindow, expect counts to be constant
    assert(outputCounts.drop(2 * (testWindow - 1)).forall(_ == testWindow * pointsPerBatch / 2))
  }


  test("entries in peace period are dropped") {
    // set parameters
    val peacePeriod = 3
    val numBatches = 7
    val pointsPerBatch = 1000
    val meanA = -10
    val stdevA = 1
    val meanB = 10
    val stdevB = 1

    val model = new StreamingTest()
      .setWindowSize(0)
      .setPeacePeriod(peacePeriod)

    val input = generateTestData(
      numBatches, pointsPerBatch, meanA, stdevA, meanB, stdevB, 42)

    // setup and run the model
    val ssc = setupStreams(
      input, (inputDStream: DStream[BinarySample]) => model.dropPeacePeriod(inputDStream))
    val outputBatches = runStreams[(Boolean, Double)](ssc, numBatches, numBatches)

    assert(outputBatches.flatten.length == (numBatches - peacePeriod) * pointsPerBatch)
  }

  test("null hypothesis when only data from one group is present") {
    // set parameters
    val numBatches = 2
    val pointsPerBatch = 1000
    val meanA = 0
    val stdevA = 0.001
    val meanB = 0
    val stdevB = 0.001

    val model = new StreamingTest()
      .setWindowSize(0)
      .setPeacePeriod(0)

    val input = generateTestData(numBatches, pointsPerBatch, meanA, stdevA, meanB, stdevB, 42)
      .map(batch => batch.filter(_.isExperiment)) // only keep one test group

    // setup and run the model
    val ssc = setupStreams(
      input, (inputDStream: DStream[BinarySample]) => model.registerStream(inputDStream))
    val outputBatches = runStreams[StreamingTestResult](ssc, numBatches, numBatches)

    assert(outputBatches.flatten.forall(result => (result.pValue - 1.0).abs < 0.001))
  }

  // Generate testing input with half of the entries in group A and half in group B
  private def generateTestData(
      numBatches: Int,
      pointsPerBatch: Int,
      meanA: Double,
      stdevA: Double,
      meanB: Double,
      stdevB: Double,
      seed: Int): (IndexedSeq[IndexedSeq[BinarySample]]) = {
    val rand = new XORShiftRandom(seed)
    val numTrues = pointsPerBatch / 2
    val data = (0 until numBatches).map { i =>
      (0 until numTrues).map { idx => BinarySample(true, meanA + stdevA * rand.nextGaussian())} ++
        (pointsPerBatch / 2 until pointsPerBatch).map { idx =>
          BinarySample(false, meanB + stdevB * rand.nextGaussian())
        }
    }

    data
  }
}
