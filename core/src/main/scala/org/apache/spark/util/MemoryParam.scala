/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.util

/**
 * An extractor object for parsing JVM memory strings, such as "10g", into an Int representing
 * the number of megabytes. Supports the same formats as Utils.memoryStringToMb.
 */
private[spark] object MemoryParam {
  def unapply(str: String): Option[Int] = {
    try {
      Some(Utils.memoryStringToMb(str))
    } catch {
      case e: NumberFormatException => None
    }
  }
}
