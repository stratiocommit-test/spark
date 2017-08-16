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
 * Build a map with String type of key, and it also supports either key case
 * sensitive or insensitive.
 */
object StringKeyHashMap {
  def apply[T](caseSensitive: Boolean): StringKeyHashMap[T] = if (caseSensitive) {
    new StringKeyHashMap[T](identity)
  } else {
    new StringKeyHashMap[T](_.toLowerCase)
  }
}


class StringKeyHashMap[T](normalizer: (String) => String) {
  private val base = new collection.mutable.HashMap[String, T]()

  def apply(key: String): T = base(normalizer(key))

  def get(key: String): Option[T] = base.get(normalizer(key))

  def put(key: String, value: T): Option[T] = base.put(normalizer(key), value)

  def remove(key: String): Option[T] = base.remove(normalizer(key))

  def iterator: Iterator[(String, T)] = base.toIterator

  def clear(): Unit = base.clear()
}
