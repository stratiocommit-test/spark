/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.types

import scala.math.Ordering
import scala.reflect.runtime.universe.typeTag

import org.apache.spark.annotation.InterfaceStability
import org.apache.spark.sql.catalyst.ScalaReflectionLock
import org.apache.spark.sql.catalyst.util.TypeUtils


/**
 * The data type representing `Array[Byte]` values.
 * Please use the singleton `DataTypes.BinaryType`.
 */
@InterfaceStability.Stable
class BinaryType private() extends AtomicType {
  // The companion object and this class is separated so the companion object also subclasses
  // this type. Otherwise, the companion object would be of type "BinaryType$" in byte code.
  // Defined with a private constructor so the companion object is the only possible instantiation.

  private[sql] type InternalType = Array[Byte]

  @transient private[sql] lazy val tag = ScalaReflectionLock.synchronized { typeTag[InternalType] }

  private[sql] val ordering = new Ordering[InternalType] {
    def compare(x: Array[Byte], y: Array[Byte]): Int = {
      TypeUtils.compareBinary(x, y)
    }
  }

  /**
   * The default size of a value of the BinaryType is 100 bytes.
   */
  override def defaultSize: Int = 100

  private[spark] override def asNullable: BinaryType = this
}

/**
 * @since 1.3.0
 */
@InterfaceStability.Stable
case object BinaryType extends BinaryType
