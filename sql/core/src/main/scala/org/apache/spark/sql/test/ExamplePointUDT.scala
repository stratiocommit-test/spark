/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.test

import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.types._

/**
 * An example class to demonstrate UDT in Scala, Java, and Python.
 * @param x x coordinate
 * @param y y coordinate
 */
@SQLUserDefinedType(udt = classOf[ExamplePointUDT])
private[sql] class ExamplePoint(val x: Double, val y: Double) extends Serializable {

  override def hashCode(): Int = 31 * (31 * x.hashCode()) + y.hashCode()

  override def equals(other: Any): Boolean = other match {
    case that: ExamplePoint => this.x == that.x && this.y == that.y
    case _ => false
  }
}

/**
 * User-defined type for [[ExamplePoint]].
 */
private[sql] class ExamplePointUDT extends UserDefinedType[ExamplePoint] {

  override def sqlType: DataType = ArrayType(DoubleType, false)

  override def pyUDT: String = "pyspark.sql.tests.ExamplePointUDT"

  override def serialize(p: ExamplePoint): GenericArrayData = {
    val output = new Array[Any](2)
    output(0) = p.x
    output(1) = p.y
    new GenericArrayData(output)
  }

  override def deserialize(datum: Any): ExamplePoint = {
    datum match {
      case values: ArrayData =>
        new ExamplePoint(values.getDouble(0), values.getDouble(1))
    }
  }

  override def userClass: Class[ExamplePoint] = classOf[ExamplePoint]

  private[spark] override def asNullable: ExamplePointUDT = this
}
