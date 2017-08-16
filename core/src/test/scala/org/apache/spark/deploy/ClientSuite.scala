/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.deploy

import org.scalatest.Matchers

import org.apache.spark.SparkFunSuite

class ClientSuite extends SparkFunSuite with Matchers {
  test("correctly validates driver jar URL's") {
    ClientArguments.isValidJarUrl("http://someHost:8080/foo.jar") should be (true)
    ClientArguments.isValidJarUrl("https://someHost:8080/foo.jar") should be (true)

    // file scheme with authority and path is valid.
    ClientArguments.isValidJarUrl("file://somehost/path/to/a/jarFile.jar") should be (true)

    // file scheme without path is not valid.
    // In this case, jarFile.jar is recognized as authority.
    ClientArguments.isValidJarUrl("file://jarFile.jar") should be (false)

    // file scheme without authority but with triple slash is valid.
    ClientArguments.isValidJarUrl("file:///some/path/to/a/jarFile.jar") should be (true)
    ClientArguments.isValidJarUrl("hdfs://someHost:1234/foo.jar") should be (true)

    ClientArguments.isValidJarUrl("hdfs://someHost:1234/foo") should be (false)
    ClientArguments.isValidJarUrl("/missing/a/protocol/jarfile.jar") should be (false)
    ClientArguments.isValidJarUrl("not-even-a-path.jar") should be (false)

    // This URI doesn't have authority and path.
    ClientArguments.isValidJarUrl("hdfs:someHost:1234/jarfile.jar") should be (false)

    // Invalid syntax.
    ClientArguments.isValidJarUrl("hdfs:") should be (false)
  }
}
