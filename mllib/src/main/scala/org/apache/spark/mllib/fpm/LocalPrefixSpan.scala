/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.mllib.fpm

import scala.collection.mutable

import org.apache.spark.internal.Logging

/**
 * Calculate all patterns of a projected database in local mode.
 *
 * @param minCount minimal count for a frequent pattern
 * @param maxPatternLength max pattern length for a frequent pattern
 */
private[fpm] class LocalPrefixSpan(
    val minCount: Long,
    val maxPatternLength: Int) extends Logging with Serializable {
  import PrefixSpan.Postfix
  import LocalPrefixSpan.ReversedPrefix

  /**
   * Generates frequent patterns on the input array of postfixes.
   * @param postfixes an array of postfixes
   * @return an iterator of (frequent pattern, count)
   */
  def run(postfixes: Array[Postfix]): Iterator[(Array[Int], Long)] = {
    genFreqPatterns(ReversedPrefix.empty, postfixes).map { case (prefix, count) =>
      (prefix.toSequence, count)
    }
  }

  /**
   * Recursively generates frequent patterns.
   * @param prefix current prefix
   * @param postfixes projected postfixes w.r.t. the prefix
   * @return an iterator of (prefix, count)
   */
  private def genFreqPatterns(
      prefix: ReversedPrefix,
      postfixes: Array[Postfix]): Iterator[(ReversedPrefix, Long)] = {
    if (maxPatternLength == prefix.length || postfixes.length < minCount) {
      return Iterator.empty
    }
    // find frequent items
    val counts = mutable.Map.empty[Int, Long].withDefaultValue(0)
    postfixes.foreach { postfix =>
      postfix.genPrefixItems.foreach { case (x, _) =>
        counts(x) += 1L
      }
    }
    val freqItems = counts.toSeq.filter { case (_, count) =>
      count >= minCount
    }.sorted
    // project and recursively call genFreqPatterns
    freqItems.toIterator.flatMap { case (item, count) =>
      val newPrefix = prefix :+ item
      Iterator.single((newPrefix, count)) ++ {
        val projected = postfixes.map(_.project(item)).filter(_.nonEmpty)
        genFreqPatterns(newPrefix, projected)
      }
    }
  }
}

private object LocalPrefixSpan {

  /**
   * Represents a prefix stored as a list in reversed order.
   * @param items items in the prefix in reversed order
   * @param length length of the prefix, not counting delimiters
   */
  class ReversedPrefix private (val items: List[Int], val length: Int) extends Serializable {
    /**
     * Expands the prefix by one item.
     */
    def :+(item: Int): ReversedPrefix = {
      require(item != 0)
      if (item < 0) {
        new ReversedPrefix(-item :: items, length + 1)
      } else {
        new ReversedPrefix(item :: 0 :: items, length + 1)
      }
    }

    /**
     * Converts this prefix to a sequence.
     */
    def toSequence: Array[Int] = (0 :: items).toArray.reverse
  }

  object ReversedPrefix {
    /** An empty prefix. */
    val empty: ReversedPrefix = new ReversedPrefix(List.empty, 0)
  }
}
