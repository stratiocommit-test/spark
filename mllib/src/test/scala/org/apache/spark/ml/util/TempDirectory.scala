/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.ml.util

import java.io.File

import org.scalatest.{BeforeAndAfterAll, Suite}

import org.apache.spark.util.Utils

/**
 * Trait that creates a temporary directory before all tests and deletes it after all.
 */
trait TempDirectory extends BeforeAndAfterAll { self: Suite =>

  private var _tempDir: File = _

  /** Returns the temporary directory as a [[File]] instance. */
  protected def tempDir: File = _tempDir

  override def beforeAll(): Unit = {
    super.beforeAll()
    _tempDir = Utils.createTempDir(namePrefix = this.getClass.getName)
  }

  override def afterAll(): Unit = {
    try {
      Utils.deleteRecursively(_tempDir)
    } finally {
      super.afterAll()
    }
  }
}
