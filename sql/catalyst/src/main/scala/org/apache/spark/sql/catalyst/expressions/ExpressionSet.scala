/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.catalyst.expressions

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object ExpressionSet {
  /** Constructs a new [[ExpressionSet]] by applying [[Canonicalize]] to `expressions`. */
  def apply(expressions: TraversableOnce[Expression]): ExpressionSet = {
    val set = new ExpressionSet()
    expressions.foreach(set.add)
    set
  }
}

/**
 * A [[Set]] where membership is determined based on a canonical representation of an [[Expression]]
 * (i.e. one that attempts to ignore cosmetic differences).  See [[Canonicalize]] for more details.
 *
 * Internally this set uses the canonical representation, but keeps also track of the original
 * expressions to ease debugging.  Since different expressions can share the same canonical
 * representation, this means that operations that extract expressions from this set are only
 * guaranteed to see at least one such expression.  For example:
 *
 * {{{
 *   val set = AttributeSet(a + 1, 1 + a)
 *
 *   set.iterator => Iterator(a + 1)
 *   set.contains(a + 1) => true
 *   set.contains(1 + a) => true
 *   set.contains(a + 2) => false
 * }}}
 */
class ExpressionSet protected(
    protected val baseSet: mutable.Set[Expression] = new mutable.HashSet,
    protected val originals: mutable.Buffer[Expression] = new ArrayBuffer)
  extends Set[Expression] {

  protected def add(e: Expression): Unit = {
    if (!baseSet.contains(e.canonicalized)) {
      baseSet.add(e.canonicalized)
      originals += e
    }
  }

  override def contains(elem: Expression): Boolean = baseSet.contains(elem.canonicalized)

  override def +(elem: Expression): ExpressionSet = {
    val newSet = new ExpressionSet(baseSet.clone(), originals.clone())
    newSet.add(elem)
    newSet
  }

  override def -(elem: Expression): ExpressionSet = {
    val newBaseSet = baseSet.clone().filterNot(_ == elem.canonicalized)
    val newOriginals = originals.clone().filterNot(_.canonicalized == elem.canonicalized)
    new ExpressionSet(newBaseSet, newOriginals)
  }

  override def iterator: Iterator[Expression] = originals.iterator

  /**
   * Returns a string containing both the post [[Canonicalize]] expressions and the original
   * expressions in this set.
   */
  def toDebugString: String =
    s"""
       |baseSet: ${baseSet.mkString(", ")}
       |originals: ${originals.mkString(", ")}
     """.stripMargin
}
