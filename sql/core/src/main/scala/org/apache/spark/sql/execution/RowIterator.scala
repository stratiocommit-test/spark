/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.execution

import java.util.NoSuchElementException

import org.apache.spark.sql.catalyst.InternalRow

/**
 * An internal iterator interface which presents a more restrictive API than
 * [[scala.collection.Iterator]].
 *
 * One major departure from the Scala iterator API is the fusing of the `hasNext()` and `next()`
 * calls: Scala's iterator allows users to call `hasNext()` without immediately advancing the
 * iterator to consume the next row, whereas RowIterator combines these calls into a single
 * [[advanceNext()]] method.
 */
abstract class RowIterator {
  /**
   * Advance this iterator by a single row. Returns `false` if this iterator has no more rows
   * and `true` otherwise. If this returns `true`, then the new row can be retrieved by calling
   * [[getRow]].
   */
  def advanceNext(): Boolean

  /**
   * Retrieve the row from this iterator. This method is idempotent. It is illegal to call this
   * method after [[advanceNext()]] has returned `false`.
   */
  def getRow: InternalRow

  /**
   * Convert this RowIterator into a [[scala.collection.Iterator]].
   */
  def toScala: Iterator[InternalRow] = new RowIteratorToScala(this)
}

object RowIterator {
  def fromScala(scalaIter: Iterator[InternalRow]): RowIterator = {
    scalaIter match {
      case wrappedRowIter: RowIteratorToScala => wrappedRowIter.rowIter
      case _ => new RowIteratorFromScala(scalaIter)
    }
  }
}

private final class RowIteratorToScala(val rowIter: RowIterator) extends Iterator[InternalRow] {
  private [this] var hasNextWasCalled: Boolean = false
  private [this] var _hasNext: Boolean = false
  override def hasNext: Boolean = {
    // Idempotency:
    if (!hasNextWasCalled) {
      _hasNext = rowIter.advanceNext()
      hasNextWasCalled = true
    }
    _hasNext
  }
  override def next(): InternalRow = {
    if (!hasNext) throw new NoSuchElementException
    hasNextWasCalled = false
    rowIter.getRow
  }
}

private final class RowIteratorFromScala(scalaIter: Iterator[InternalRow]) extends RowIterator {
  private[this] var _next: InternalRow = null
  override def advanceNext(): Boolean = {
    if (scalaIter.hasNext) {
      _next = scalaIter.next()
      true
    } else {
      _next = null
      false
    }
  }
  override def getRow: InternalRow = _next
  override def toScala: Iterator[InternalRow] = scalaIter
}
