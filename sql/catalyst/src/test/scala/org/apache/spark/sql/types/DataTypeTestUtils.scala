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

/**
 * Utility functions for working with DataTypes in tests.
 */
object DataTypeTestUtils {

  /**
   * Instances of all [[IntegralType]]s.
   */
  val integralType: Set[IntegralType] = Set(
    ByteType, ShortType, IntegerType, LongType
  )

  /**
   * Instances of all [[FractionalType]]s, including both fixed- and unlimited-precision
   * decimal types.
   */
  val fractionalTypes: Set[FractionalType] = Set(
    DecimalType.USER_DEFAULT,
    DecimalType(20, 5),
    DecimalType.SYSTEM_DEFAULT,
    DoubleType,
    FloatType
  )

  /**
   * Instances of all [[NumericType]]s.
   */
  val numericTypes: Set[NumericType] = integralType ++ fractionalTypes

  // TODO: remove this once we find out how to handle decimal properly in property check
  val numericTypeWithoutDecimal: Set[DataType] = integralType ++ Set(DoubleType, FloatType)

  /**
   * Instances of all [[NumericType]]s and [[CalendarIntervalType]]
   */
  val numericAndInterval: Set[DataType] = numericTypeWithoutDecimal + CalendarIntervalType

  /**
   * All the types that support ordering
   */
  val ordered: Set[DataType] =
    numericTypeWithoutDecimal + BooleanType + TimestampType + DateType + StringType + BinaryType

  /**
   * All the types that we can use in a property check
   */
  val propertyCheckSupported: Set[DataType] = ordered

  /**
   * Instances of all [[AtomicType]]s.
   */
  val atomicTypes: Set[DataType] = numericTypes ++ Set(
    BinaryType,
    BooleanType,
    DateType,
    StringType,
    TimestampType
  )

  /**
   * Instances of [[ArrayType]] for all [[AtomicType]]s. Arrays of these types may contain null.
   */
  val atomicArrayTypes: Set[ArrayType] = atomicTypes.map(ArrayType(_, containsNull = true))
}
