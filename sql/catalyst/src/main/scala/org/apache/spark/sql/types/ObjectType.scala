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

import scala.language.existentials

import org.apache.spark.annotation.InterfaceStability

@InterfaceStability.Evolving
object ObjectType extends AbstractDataType {
  override private[sql] def defaultConcreteType: DataType =
    throw new UnsupportedOperationException("null literals can't be casted to ObjectType")

  override private[sql] def acceptsType(other: DataType): Boolean = other match {
    case ObjectType(_) => true
    case _ => false
  }

  override private[sql] def simpleString: String = "Object"
}

/**
 * Represents a JVM object that is passing through Spark SQL expression evaluation.
 */
@InterfaceStability.Evolving
case class ObjectType(cls: Class[_]) extends DataType {
  override def defaultSize: Int = 4096

  def asNullable: DataType = this

  override def simpleString: String = cls.getName
}
