/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.catalyst

import org.apache.spark.internal.Logging

/**
 * A library for easily manipulating trees of operators.  Operators that extend TreeNode are
 * granted the following interface:
 * <ul>
 *   <li>Scala collection like methods (foreach, map, flatMap, collect, etc)</li>
 *   <li>
 *     transform - accepts a partial function that is used to generate a new tree.  When the
 *     partial function can be applied to a given tree segment, that segment is replaced with the
 *     result.  After attempting to apply the partial function to a given node, the transform
 *     function recursively attempts to apply the function to that node's children.
 *   </li>
 *   <li>debugging support - pretty printing, easy splicing of trees, etc.</li>
 * </ul>
 */
package object trees extends Logging {
  // Since we want tree nodes to be lightweight, we create one logger for all treenode instances.
  protected override def logName = "catalyst.trees"

  /**
   * A [[TreeNode]] companion for reference equality for Hash based Collection.
   */
  class TreeNodeRef(val obj: TreeNode[_]) {
    override def equals(o: Any): Boolean = o match {
      case that: TreeNodeRef => that.obj.eq(obj)
      case _ => false
    }

    override def hashCode: Int = if (obj == null) 0 else obj.hashCode
  }
}
