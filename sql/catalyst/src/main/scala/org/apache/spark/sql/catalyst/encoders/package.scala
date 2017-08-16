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

import org.apache.spark.sql.Encoder

package object encoders {
  /**
   * Returns an internal encoder object that can be used to serialize / deserialize JVM objects
   * into Spark SQL rows.  The implicit encoder should always be unresolved (i.e. have no attribute
   * references from a specific schema.)  This requirement allows us to preserve whether a given
   * object type is being bound by name or by ordinal when doing resolution.
   */
  def encoderFor[A : Encoder]: ExpressionEncoder[A] = implicitly[Encoder[A]] match {
    case e: ExpressionEncoder[A] =>
      e.assertUnresolved()
      e
    case _ => sys.error(s"Only expression encoders are supported today")
  }
}
