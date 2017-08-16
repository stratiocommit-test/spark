/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.partial

import scala.collection.Map
import scala.collection.mutable.HashMap
import scala.reflect.ClassTag

import org.apache.spark.util.collection.OpenHashMap

/**
 * An ApproximateEvaluator for counts by key. Returns a map of key to confidence interval.
 */
private[spark] class GroupedCountEvaluator[T : ClassTag](totalOutputs: Int, confidence: Double)
  extends ApproximateEvaluator[OpenHashMap[T, Long], Map[T, BoundedDouble]] {

  private var outputsMerged = 0
  private val sums = new OpenHashMap[T, Long]()   // Sum of counts for each key

  override def merge(outputId: Int, taskResult: OpenHashMap[T, Long]): Unit = {
    outputsMerged += 1
    taskResult.foreach { case (key, value) =>
      sums.changeValue(key, value, _ + value)
    }
  }

  override def currentResult(): Map[T, BoundedDouble] = {
    if (outputsMerged == totalOutputs) {
      sums.map { case (key, sum) => (key, new BoundedDouble(sum, 1.0, sum, sum)) }.toMap
    } else if (outputsMerged == 0) {
      new HashMap[T, BoundedDouble]
    } else {
      val p = outputsMerged.toDouble / totalOutputs
      sums.map { case (key, sum) => (key, CountEvaluator.bound(confidence, sum, p)) }.toMap
    }
  }
}
