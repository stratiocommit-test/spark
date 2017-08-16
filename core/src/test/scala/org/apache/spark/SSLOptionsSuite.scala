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

import java.io.File
import javax.net.ssl.SSLContext

import org.scalatest.BeforeAndAfterAll

class SSLOptionsSuite extends SparkFunSuite with BeforeAndAfterAll {

  test("test resolving property file as spark conf ") {
    val keyStorePath = new File(this.getClass.getResource("/keystore").toURI).getAbsolutePath
    val trustStorePath = new File(this.getClass.getResource("/truststore").toURI).getAbsolutePath

    // Pick two cipher suites that the provider knows about
    val sslContext = SSLContext.getInstance("TLSv1.2")
    sslContext.init(null, null, null)
    val algorithms = sslContext
      .getServerSocketFactory
      .getDefaultCipherSuites
      .take(2)
      .toSet

    val conf = new SparkConf
    conf.set("spark.ssl.enabled", "true")
    conf.set("spark.ssl.keyStore", keyStorePath)
    conf.set("spark.ssl.keyStorePassword", "password")
    conf.set("spark.ssl.keyPassword", "password")
    conf.set("spark.ssl.trustStore", trustStorePath)
    conf.set("spark.ssl.trustStorePassword", "password")
    conf.set("spark.ssl.enabledAlgorithms", algorithms.mkString(","))
    conf.set("spark.ssl.protocol", "TLSv1.2")

    val opts = SSLOptions.parse(conf, "spark.ssl")

    assert(opts.enabled === true)
    assert(opts.trustStore.isDefined === true)
    assert(opts.trustStore.get.getName === "truststore")
    assert(opts.trustStore.get.getAbsolutePath === trustStorePath)
    assert(opts.keyStore.isDefined === true)
    assert(opts.keyStore.get.getName === "keystore")
    assert(opts.keyStore.get.getAbsolutePath === keyStorePath)
    assert(opts.trustStorePassword === Some("password"))
    assert(opts.keyStorePassword === Some("password"))
    assert(opts.keyPassword === Some("password"))
    assert(opts.protocol === Some("TLSv1.2"))
    assert(opts.enabledAlgorithms === algorithms)
  }

  test("test resolving property with defaults specified ") {
    val keyStorePath = new File(this.getClass.getResource("/keystore").toURI).getAbsolutePath
    val trustStorePath = new File(this.getClass.getResource("/truststore").toURI).getAbsolutePath

    val conf = new SparkConf
    conf.set("spark.ssl.enabled", "true")
    conf.set("spark.ssl.keyStore", keyStorePath)
    conf.set("spark.ssl.keyStorePassword", "password")
    conf.set("spark.ssl.keyPassword", "password")
    conf.set("spark.ssl.trustStore", trustStorePath)
    conf.set("spark.ssl.trustStorePassword", "password")
    conf.set("spark.ssl.enabledAlgorithms",
      "TLS_RSA_WITH_AES_128_CBC_SHA, TLS_RSA_WITH_AES_256_CBC_SHA")
    conf.set("spark.ssl.protocol", "SSLv3")

    val defaultOpts = SSLOptions.parse(conf, "spark.ssl", defaults = None)
    val opts = SSLOptions.parse(conf, "spark.ssl.ui", defaults = Some(defaultOpts))

    assert(opts.enabled === true)
    assert(opts.trustStore.isDefined === true)
    assert(opts.trustStore.get.getName === "truststore")
    assert(opts.trustStore.get.getAbsolutePath === trustStorePath)
    assert(opts.keyStore.isDefined === true)
    assert(opts.keyStore.get.getName === "keystore")
    assert(opts.keyStore.get.getAbsolutePath === keyStorePath)
    assert(opts.trustStorePassword === Some("password"))
    assert(opts.keyStorePassword === Some("password"))
    assert(opts.keyPassword === Some("password"))
    assert(opts.protocol === Some("SSLv3"))
    assert(opts.enabledAlgorithms ===
      Set("TLS_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_256_CBC_SHA"))
  }

  test("test whether defaults can be overridden ") {
    val keyStorePath = new File(this.getClass.getResource("/keystore").toURI).getAbsolutePath
    val trustStorePath = new File(this.getClass.getResource("/truststore").toURI).getAbsolutePath

    val conf = new SparkConf
    conf.set("spark.ssl.enabled", "true")
    conf.set("spark.ssl.ui.enabled", "false")
    conf.set("spark.ssl.keyStore", keyStorePath)
    conf.set("spark.ssl.keyStorePassword", "password")
    conf.set("spark.ssl.ui.keyStorePassword", "12345")
    conf.set("spark.ssl.keyPassword", "password")
    conf.set("spark.ssl.trustStore", trustStorePath)
    conf.set("spark.ssl.trustStorePassword", "password")
    conf.set("spark.ssl.enabledAlgorithms",
      "TLS_RSA_WITH_AES_128_CBC_SHA, TLS_RSA_WITH_AES_256_CBC_SHA")
    conf.set("spark.ssl.ui.enabledAlgorithms", "ABC, DEF")
    conf.set("spark.ssl.protocol", "SSLv3")

    val defaultOpts = SSLOptions.parse(conf, "spark.ssl", defaults = None)
    val opts = SSLOptions.parse(conf, "spark.ssl.ui", defaults = Some(defaultOpts))

    assert(opts.enabled === false)
    assert(opts.trustStore.isDefined === true)
    assert(opts.trustStore.get.getName === "truststore")
    assert(opts.trustStore.get.getAbsolutePath === trustStorePath)
    assert(opts.keyStore.isDefined === true)
    assert(opts.keyStore.get.getName === "keystore")
    assert(opts.keyStore.get.getAbsolutePath === keyStorePath)
    assert(opts.trustStorePassword === Some("password"))
    assert(opts.keyStorePassword === Some("12345"))
    assert(opts.keyPassword === Some("password"))
    assert(opts.protocol === Some("SSLv3"))
    assert(opts.enabledAlgorithms === Set("ABC", "DEF"))
  }

}
