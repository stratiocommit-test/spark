/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.streaming.util

import org.apache.spark.SparkContext
import org.apache.spark.util.collection.OpenHashMap

private[streaming]
object RawTextHelper {

  /**
   * Splits lines and counts the words.
   */
  def splitAndCountPartitions(iter: Iterator[String]): Iterator[(String, Long)] = {
    val map = new OpenHashMap[String, Long]
    var i = 0
    var j = 0
    while (iter.hasNext) {
      val s = iter.next()
      i = 0
      while (i < s.length) {
        j = i
        while (j < s.length && s.charAt(j) != ' ') {
          j += 1
        }
        if (j > i) {
          val w = s.substring(i, j)
          map.changeValue(w, 1L, _ + 1L)
        }
        i = j
        while (i < s.length && s.charAt(i) == ' ') {
          i += 1
        }
      }
      map.toIterator.map {
        case (k, v) => (k, v)
      }
    }
    map.toIterator.map{case (k, v) => (k, v)}
  }

  /**
   * Gets the top k words in terms of word counts. Assumes that each word exists only once
   * in the `data` iterator (that is, the counts have been reduced).
   */
  def topK(data: Iterator[(String, Long)], k: Int): Iterator[(String, Long)] = {
    val taken = new Array[(String, Long)](k)

    var i = 0
    var len = 0
    var done = false
    var value: (String, Long) = null
    var swap: (String, Long) = null
    var count = 0

    while(data.hasNext) {
      value = data.next()
      if (value != null) {
        count += 1
        if (len == 0) {
          taken(0) = value
          len = 1
        } else if (len < k || value._2 > taken(len - 1)._2) {
          if (len < k) {
            len += 1
          }
          taken(len - 1) = value
          i = len - 1
          while(i > 0 && taken(i - 1)._2 < taken(i)._2) {
            swap = taken(i)
            taken(i) = taken(i-1)
            taken(i - 1) = swap
            i -= 1
          }
        }
      }
    }
    taken.toIterator
  }

  /**
   * Warms up the SparkContext in master and slave by running tasks to force JIT kick in
   * before real workload starts.
   */
  def warmUp(sc: SparkContext) {
    for (i <- 0 to 1) {
      sc.parallelize(1 to 200000, 1000)
        .map(_ % 1331).map(_.toString)
        .mapPartitions(splitAndCountPartitions).reduceByKey(_ + _, 10)
        .count()
    }
  }

  def add(v1: Long, v2: Long): Long = {
    v1 + v2
  }

  def subtract(v1: Long, v2: Long): Long = {
    v1 - v2
  }

  def max(v1: Long, v2: Long): Long = math.max(v1, v2)
}
