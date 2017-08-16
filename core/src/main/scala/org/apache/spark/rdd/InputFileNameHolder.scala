/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.rdd

import org.apache.spark.unsafe.types.UTF8String

/**
 * This holds file names of the current Spark task. This is used in HadoopRDD,
 * FileScanRDD, NewHadoopRDD and InputFileName function in Spark SQL.
 *
 * The returned value should never be null but empty string if it is unknown.
 */
private[spark] object InputFileNameHolder {
  /**
   * The thread variable for the name of the current file being read. This is used by
   * the InputFileName function in Spark SQL.
   */
  private[this] val inputFileName: ThreadLocal[UTF8String] = new ThreadLocal[UTF8String] {
    override protected def initialValue(): UTF8String = UTF8String.fromString("")
  }

  /**
   * Returns the holding file name or empty string if it is unknown.
   */
  def getInputFileName(): UTF8String = inputFileName.get()

  private[spark] def setInputFileName(file: String) = {
    require(file != null, "The input file name cannot be null")
    inputFileName.set(UTF8String.fromString(file))
  }

  private[spark] def unsetInputFileName(): Unit = inputFileName.remove()

}
