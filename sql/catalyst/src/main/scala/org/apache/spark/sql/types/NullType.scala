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

import org.apache.spark.annotation.InterfaceStability


/**
 * The data type representing `NULL` values. Please use the singleton `DataTypes.NullType`.
 *
 * @since 1.3.0
 */
@InterfaceStability.Stable
class NullType private() extends DataType {
  // The companion object and this class is separated so the companion object also subclasses
  // this type. Otherwise, the companion object would be of type "NullType$" in byte code.
  // Defined with a private constructor so the companion object is the only possible instantiation.
  override def defaultSize: Int = 1

  private[spark] override def asNullable: NullType = this
}

/**
 * @since 1.3.0
 */
@InterfaceStability.Stable
case object NullType extends NullType
