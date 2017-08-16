/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.streaming.scheduler.rate

import org.apache.spark.SparkConf
import org.apache.spark.streaming.Duration

/**
 * A component that estimates the rate at which an `InputDStream` should ingest
 * records, based on updates at every batch completion.
 *
 * @see [[org.apache.spark.streaming.scheduler.RateController]]
 */
private[streaming] trait RateEstimator extends Serializable {

  /**
   * Computes the number of records the stream attached to this `RateEstimator`
   * should ingest per second, given an update on the size and completion
   * times of the latest batch.
   *
   * @param time The timestamp of the current batch interval that just finished
   * @param elements The number of records that were processed in this batch
   * @param processingDelay The time in ms that took for the job to complete
   * @param schedulingDelay The time in ms that the job spent in the scheduling queue
   */
  def compute(
      time: Long,
      elements: Long,
      processingDelay: Long,
      schedulingDelay: Long): Option[Double]
}

object RateEstimator {

  /**
   * Return a new `RateEstimator` based on the value of
   * `spark.streaming.backpressure.rateEstimator`.
   *
   * The only known and acceptable estimator right now is `pid`.
   *
   * @return An instance of RateEstimator
   * @throws IllegalArgumentException if the configured RateEstimator is not `pid`.
   */
  def create(conf: SparkConf, batchInterval: Duration): RateEstimator =
    conf.get("spark.streaming.backpressure.rateEstimator", "pid") match {
      case "pid" =>
        val proportional = conf.getDouble("spark.streaming.backpressure.pid.proportional", 1.0)
        val integral = conf.getDouble("spark.streaming.backpressure.pid.integral", 0.2)
        val derived = conf.getDouble("spark.streaming.backpressure.pid.derived", 0.0)
        val minRate = conf.getDouble("spark.streaming.backpressure.pid.minRate", 100)
        new PIDRateEstimator(batchInterval.milliseconds, proportional, integral, derived, minRate)

      case estimator =>
        throw new IllegalArgumentException(s"Unknown rate estimator: $estimator")
    }
}
