/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.execution

/**
 * An interface for relations that are backed by files.  When a class implements this interface,
 * the list of paths that it returns will be returned to a user who calls `inputPaths` on any
 * DataFrame that queries this relation.
 */
trait FileRelation {
  /** Returns the list of files that will be read when scanning this relation. */
  def inputFiles: Array[String]
}
