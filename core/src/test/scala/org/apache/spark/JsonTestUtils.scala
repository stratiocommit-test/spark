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

import org.json4s._
import org.json4s.jackson.JsonMethods

trait JsonTestUtils {
  def assertValidDataInJson(validateJson: JValue, expectedJson: JValue) {
    val Diff(c, a, d) = validateJson.diff(expectedJson)
    val validatePretty = JsonMethods.pretty(validateJson)
    val expectedPretty = JsonMethods.pretty(expectedJson)
    val errorMessage = s"Expected:\n$expectedPretty\nFound:\n$validatePretty"
    import org.scalactic.TripleEquals._
    assert(c === JNothing, s"$errorMessage\nChanged:\n${JsonMethods.pretty(c)}")
    assert(a === JNothing, s"$errorMessage\nAdded:\n${JsonMethods.pretty(a)}")
    assert(d === JNothing, s"$errorMessage\nDeleted:\n${JsonMethods.pretty(d)}")
  }

}
