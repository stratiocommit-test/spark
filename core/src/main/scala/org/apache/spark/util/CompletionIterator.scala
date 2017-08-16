/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.util

/**
 * Wrapper around an iterator which calls a completion method after it successfully iterates
 * through all the elements.
 */
private[spark]
// scalastyle:off
abstract class CompletionIterator[ +A, +I <: Iterator[A]](sub: I) extends Iterator[A] {
// scalastyle:on

  private[this] var completed = false
  def next(): A = sub.next()
  def hasNext: Boolean = {
    val r = sub.hasNext
    if (!r && !completed) {
      completed = true
      completion()
    }
    r
  }

  def completion(): Unit
}

private[spark] object CompletionIterator {
  def apply[A, I <: Iterator[A]](sub: I, completionFunction: => Unit) : CompletionIterator[A, I] = {
    new CompletionIterator[A, I](sub) {
      def completion(): Unit = completionFunction
    }
  }
}
