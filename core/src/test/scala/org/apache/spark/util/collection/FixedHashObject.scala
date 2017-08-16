/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.util.collection

/**
 * A dummy class that always returns the same hash code, to easily test hash collisions
 */
case class FixedHashObject(v: Int, h: Int) extends Serializable {
  override def hashCode(): Int = h
  override def equals(other: Any): Boolean = other match {
    case that: FixedHashObject => v == that.v && h == that.h
    case _ => false
  }
}
