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

import java.util.Properties

import org.apache.commons.lang3.SerializationUtils
import org.scalatest.{BeforeAndAfterEach, Suite}

/**
 * Mixin for automatically resetting system properties that are modified in ScalaTest tests.
 * This resets the properties after each individual test.
 *
 * The order in which fixtures are mixed in affects the order in which they are invoked by tests.
 * If we have a suite `MySuite extends SparkFunSuite with Foo with Bar`, then
 * Bar's `super` is Foo, so Bar's beforeEach() will and afterEach() methods will be invoked first
 * by the rest runner.
 *
 * This means that ResetSystemProperties should appear as the last trait in test suites that it's
 * mixed into in order to ensure that the system properties snapshot occurs as early as possible.
 * ResetSystemProperties calls super.afterEach() before performing its own cleanup, ensuring that
 * the old properties are restored as late as possible.
 *
 * See the "Composing fixtures by stacking traits" section at
 * http://www.scalatest.org/user_guide/sharing_fixtures for more details about this pattern.
 */
private[spark] trait ResetSystemProperties extends BeforeAndAfterEach { this: Suite =>
  var oldProperties: Properties = null

  override def beforeEach(): Unit = {
    // we need SerializationUtils.clone instead of `new Properties(System.getProperties())` because
    // the later way of creating a copy does not copy the properties but it initializes a new
    // Properties object with the given properties as defaults. They are not recognized at all
    // by standard Scala wrapper over Java Properties then.
    oldProperties = SerializationUtils.clone(System.getProperties)
    super.beforeEach()
  }

  override def afterEach(): Unit = {
    try {
      super.afterEach()
    } finally {
      System.setProperties(oldProperties)
      oldProperties = null
    }
  }
}
