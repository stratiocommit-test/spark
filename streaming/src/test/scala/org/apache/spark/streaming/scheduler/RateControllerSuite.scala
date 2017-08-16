/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.streaming.scheduler

import org.scalatest.concurrent.Eventually._
import org.scalatest.time.SpanSugar._

import org.apache.spark.streaming._
import org.apache.spark.streaming.scheduler.rate.RateEstimator

class RateControllerSuite extends TestSuiteBase {

  override def useManualClock: Boolean = false

  override def batchDuration: Duration = Milliseconds(50)

  test("RateController - rate controller publishes updates after batches complete") {
    val ssc = new StreamingContext(conf, batchDuration)
    withStreamingContext(ssc) { ssc =>
      val dstream = new RateTestInputDStream(ssc)
      dstream.register()
      ssc.start()

      eventually(timeout(10.seconds)) {
        assert(dstream.publishedRates > 0)
      }
    }
  }

  test("ReceiverRateController - published rates reach receivers") {
    val ssc = new StreamingContext(conf, batchDuration)
    withStreamingContext(ssc) { ssc =>
      val estimator = new ConstantEstimator(100)
      val dstream = new RateTestInputDStream(ssc) {
        override val rateController =
          Some(new ReceiverRateController(id, estimator))
      }
      dstream.register()
      ssc.start()

      // Wait for receiver to start
      eventually(timeout(5.seconds)) {
        RateTestReceiver.getActive().nonEmpty
      }

      // Update rate in the estimator and verify whether the rate was published to the receiver
      def updateRateAndVerify(rate: Long): Unit = {
        estimator.updateRate(rate)
        eventually(timeout(5.seconds)) {
          assert(RateTestReceiver.getActive().get.getDefaultBlockGeneratorRateLimit() === rate)
        }
      }

      // Verify multiple rate update
      Seq(100, 200, 300).foreach { rate =>
        updateRateAndVerify(rate)
      }
    }
  }
}

private[streaming] class ConstantEstimator(@volatile private var rate: Long)
  extends RateEstimator {

  def updateRate(newRate: Long): Unit = {
    rate = newRate
  }

  def compute(
      time: Long,
      elements: Long,
      processingDelay: Long,
      schedulingDelay: Long): Option[Double] = Some(rate)
}
