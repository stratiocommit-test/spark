/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.streaming.kinesis

import org.apache.spark.SparkFunSuite

/**
 * Helper class that runs Kinesis real data transfer tests or
 * ignores them based on env variable is set or not.
 */
trait KinesisFunSuite extends SparkFunSuite  {
  import KinesisTestUtils._

  /** Run the test if environment variable is set or ignore the test */
  def testIfEnabled(testName: String)(testBody: => Unit) {
    if (shouldRunTests) {
      test(testName)(testBody)
    } else {
      ignore(s"$testName [enable by setting env var $envVarNameForEnablingTests=1]")(testBody)
    }
  }

  /** Run the give body of code only if Kinesis tests are enabled */
  def runIfTestsEnabled(message: String)(body: => Unit): Unit = {
    if (shouldRunTests) {
      body
    } else {
      ignore(s"$message [enable by setting env var $envVarNameForEnablingTests=1]")(())
    }
  }
}
