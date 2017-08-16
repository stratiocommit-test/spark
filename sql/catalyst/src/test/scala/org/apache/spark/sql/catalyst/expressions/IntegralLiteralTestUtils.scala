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

/**
 * Utilities to make sure we pass the proper numeric ranges
 */
object IntegralLiteralTestUtils {

  val positiveShort: Short = (Byte.MaxValue + 1).toShort
  val negativeShort: Short = (Byte.MinValue - 1).toShort

  val positiveShortLit: Literal = Literal(positiveShort)
  val negativeShortLit: Literal = Literal(negativeShort)

  val positiveInt: Int = Short.MaxValue + 1
  val negativeInt: Int = Short.MinValue - 1

  val positiveIntLit: Literal = Literal(positiveInt)
  val negativeIntLit: Literal = Literal(negativeInt)

  val positiveLong: Long = Int.MaxValue + 1L
  val negativeLong: Long = Int.MinValue - 1L

  val positiveLongLit: Literal = Literal(positiveLong)
  val negativeLongLit: Literal = Literal(negativeLong)
}
