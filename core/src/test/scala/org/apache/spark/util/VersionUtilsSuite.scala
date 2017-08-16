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

import org.apache.spark.SparkFunSuite

class VersionUtilsSuite extends SparkFunSuite {

  import org.apache.spark.util.VersionUtils._

  test("Parse Spark major version") {
    assert(majorVersion("2.0") === 2)
    assert(majorVersion("12.10.11") === 12)
    assert(majorVersion("2.0.1-SNAPSHOT") === 2)
    assert(majorVersion("2.0.x") === 2)
    withClue("majorVersion parsing should fail for invalid major version number") {
      intercept[IllegalArgumentException] {
        majorVersion("2z.0")
      }
    }
    withClue("majorVersion parsing should fail for invalid minor version number") {
      intercept[IllegalArgumentException] {
        majorVersion("2.0z")
      }
    }
  }

  test("Parse Spark minor version") {
    assert(minorVersion("2.0") === 0)
    assert(minorVersion("12.10.11") === 10)
    assert(minorVersion("2.0.1-SNAPSHOT") === 0)
    assert(minorVersion("2.0.x") === 0)
    withClue("minorVersion parsing should fail for invalid major version number") {
      intercept[IllegalArgumentException] {
        minorVersion("2z.0")
      }
    }
    withClue("minorVersion parsing should fail for invalid minor version number") {
      intercept[IllegalArgumentException] {
        minorVersion("2.0z")
      }
    }
  }

  test("Parse Spark major and minor versions") {
    assert(majorMinorVersion("2.0") === (2, 0))
    assert(majorMinorVersion("12.10.11") === (12, 10))
    assert(majorMinorVersion("2.0.1-SNAPSHOT") === (2, 0))
    assert(majorMinorVersion("2.0.x") === (2, 0))
    withClue("majorMinorVersion parsing should fail for invalid major version number") {
      intercept[IllegalArgumentException] {
        majorMinorVersion("2z.0")
      }
    }
    withClue("majorMinorVersion parsing should fail for invalid minor version number") {
      intercept[IllegalArgumentException] {
        majorMinorVersion("2.0z")
      }
    }
  }
}
