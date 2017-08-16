/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.internal

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.AnalysisException

class VariableSubstitutionSuite extends SparkFunSuite {

  private lazy val conf = new SQLConf
  private lazy val sub = new VariableSubstitution(conf)

  test("system property") {
    System.setProperty("varSubSuite.var", "abcd")
    assert(sub.substitute("${system:varSubSuite.var}") == "abcd")
  }

  test("environmental variables") {
    assert(sub.substitute("${env:SPARK_TESTING}") == "1")
  }

  test("Spark configuration variable") {
    conf.setConfString("some-random-string-abcd", "1234abcd")
    assert(sub.substitute("${hiveconf:some-random-string-abcd}") == "1234abcd")
    assert(sub.substitute("${sparkconf:some-random-string-abcd}") == "1234abcd")
    assert(sub.substitute("${spark:some-random-string-abcd}") == "1234abcd")
    assert(sub.substitute("${some-random-string-abcd}") == "1234abcd")
  }

  test("multiple substitutes") {
    val q = "select ${bar} ${foo} ${doo} this is great"
    conf.setConfString("bar", "1")
    conf.setConfString("foo", "2")
    conf.setConfString("doo", "3")
    assert(sub.substitute(q) == "select 1 2 3 this is great")
  }

  test("test nested substitutes") {
    val q = "select ${bar} ${foo} this is great"
    conf.setConfString("bar", "1")
    conf.setConfString("foo", "${bar}")
    assert(sub.substitute(q) == "select 1 1 this is great")
  }

}
