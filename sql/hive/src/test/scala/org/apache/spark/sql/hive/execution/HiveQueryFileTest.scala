/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.hive.execution

import java.io.File

import org.apache.spark.sql.catalyst.util._

/**
 * A framework for running the query tests that are listed as a set of text files.
 *
 * TestSuites that derive from this class must provide a map of testCaseName -> testCaseFiles
 * that should be included. Additionally, there is support for whitelisting and blacklisting
 * tests as development progresses.
 */
abstract class HiveQueryFileTest extends HiveComparisonTest {
  /** A list of tests deemed out of scope and thus completely disregarded */
  def blackList: Seq[String] = Nil

  /**
   * The set of tests that are believed to be working in catalyst. Tests not in whiteList
   * blacklist are implicitly marked as ignored.
   */
  def whiteList: Seq[String] = ".*" :: Nil

  def testCases: Seq[(String, File)]

  val runAll: Boolean =
    !(System.getProperty("spark.hive.alltests") == null) ||
    runOnlyDirectories.nonEmpty ||
    skipDirectories.nonEmpty

  val whiteListProperty: String = "spark.hive.whitelist"
  // Allow the whiteList to be overridden by a system property
  val realWhiteList: Seq[String] =
    Option(System.getProperty(whiteListProperty)).map(_.split(",").toSeq).getOrElse(whiteList)

  // Go through all the test cases and add them to scala test.
  testCases.sorted.foreach {
    case (testCaseName, testCaseFile) =>
      if (blackList.map(_.r.pattern.matcher(testCaseName).matches()).reduceLeft(_||_)) {
        logDebug(s"Blacklisted test skipped $testCaseName")
      } else if (realWhiteList.map(_.r.pattern.matcher(testCaseName).matches()).reduceLeft(_||_) ||
        runAll) {
        // Build a test case and submit it to scala test framework...
        val queriesString = fileToString(testCaseFile)
        createQueryTest(testCaseName, queriesString, reset = true, tryWithoutResettingFirst = true)
      } else {
        // Only output warnings for the built in whitelist as this clutters the output when the user
        // trying to execute a single test from the commandline.
        if (System.getProperty(whiteListProperty) == null && !runAll) {
          ignore(testCaseName) {}
        }
      }
  }
}
