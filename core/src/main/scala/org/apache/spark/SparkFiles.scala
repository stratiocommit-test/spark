/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark

import java.io.File

/**
 * Resolves paths to files added through `SparkContext.addFile()`.
 */
object SparkFiles {

  /**
   * Get the absolute path of a file added through `SparkContext.addFile()`.
   */
  def get(filename: String): String =
    new File(getRootDirectory(), filename).getAbsolutePath()

  /**
   * Get the root directory that contains files added through `SparkContext.addFile()`.
   */
  def getRootDirectory(): String =
    SparkEnv.get.driverTmpDir.getOrElse(".")

}
