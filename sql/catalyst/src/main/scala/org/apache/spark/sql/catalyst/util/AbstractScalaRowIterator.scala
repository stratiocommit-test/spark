/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.catalyst.util

/**
 * Shim to allow us to implement [[scala.Iterator]] in Java. Scala 2.11+ has an AbstractIterator
 * class for this, but that class is `private[scala]` in 2.10. We need to explicitly fix this to
 * `Row` in order to work around a spurious IntelliJ compiler error. This cannot be an abstract
 * class because that leads to compilation errors under Scala 2.11.
 */
class AbstractScalaRowIterator[T] extends Iterator[T] {
  override def hasNext: Boolean = throw new NotImplementedError

  override def next(): T = throw new NotImplementedError
}
