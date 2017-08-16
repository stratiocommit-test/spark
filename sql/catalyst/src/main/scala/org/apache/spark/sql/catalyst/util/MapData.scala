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

import org.apache.spark.sql.types.DataType

/**
 * This is an internal data representation for map type in Spark SQL. This should not implement
 * `equals` and `hashCode` because the type cannot be used as join keys, grouping keys, or
 * in equality tests. See SPARK-9415 and PR#13847 for the discussions.
 */
abstract class MapData extends Serializable {

  def numElements(): Int

  def keyArray(): ArrayData

  def valueArray(): ArrayData

  def copy(): MapData

  def foreach(keyType: DataType, valueType: DataType, f: (Any, Any) => Unit): Unit = {
    val length = numElements()
    val keys = keyArray()
    val values = valueArray()
    var i = 0
    while (i < length) {
      f(keys.get(i, keyType), values.get(i, valueType))
      i += 1
    }
  }
}
