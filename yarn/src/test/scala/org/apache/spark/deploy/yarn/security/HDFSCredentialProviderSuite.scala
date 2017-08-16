/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.deploy.yarn.security

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.scalatest.{Matchers, PrivateMethodTester}

import org.apache.spark.{SparkConf, SparkException, SparkFunSuite}

class HDFSCredentialProviderSuite
    extends SparkFunSuite
    with PrivateMethodTester
    with Matchers {
  private val _getTokenRenewer = PrivateMethod[String]('getTokenRenewer)

  private def getTokenRenewer(
      hdfsCredentialProvider: HDFSCredentialProvider, conf: Configuration): String = {
    hdfsCredentialProvider invokePrivate _getTokenRenewer(conf)
  }

  private var hdfsCredentialProvider: HDFSCredentialProvider = null

  override def beforeAll() {
    super.beforeAll()

    if (hdfsCredentialProvider == null) {
      hdfsCredentialProvider = new HDFSCredentialProvider()
    }
  }

  override def afterAll() {
    if (hdfsCredentialProvider != null) {
      hdfsCredentialProvider = null
    }

    super.afterAll()
  }

  test("check token renewer") {
    val hadoopConf = new Configuration()
    hadoopConf.set("yarn.resourcemanager.address", "myrm:8033")
    hadoopConf.set("yarn.resourcemanager.principal", "yarn/myrm:8032@SPARKTEST.COM")
    val renewer = getTokenRenewer(hdfsCredentialProvider, hadoopConf)
    renewer should be ("yarn/myrm:8032@SPARKTEST.COM")
  }

  test("check token renewer default") {
    val hadoopConf = new Configuration()
    val caught =
      intercept[SparkException] {
        getTokenRenewer(hdfsCredentialProvider, hadoopConf)
      }
    assert(caught.getMessage === "Can't get Master Kerberos principal for use as renewer")
  }
}
