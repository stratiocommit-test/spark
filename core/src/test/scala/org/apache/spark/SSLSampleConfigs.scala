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

object SSLSampleConfigs {
  val keyStorePath = new File(this.getClass.getResource("/keystore").toURI).getAbsolutePath
  val untrustedKeyStorePath = new File(
    this.getClass.getResource("/untrusted-keystore").toURI).getAbsolutePath
  val trustStorePath = new File(this.getClass.getResource("/truststore").toURI).getAbsolutePath

  val enabledAlgorithms =
    // A reasonable set of TLSv1.2 Oracle security provider suites
    "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384, " +
    "TLS_RSA_WITH_AES_256_CBC_SHA256, " +
    "TLS_DHE_RSA_WITH_AES_256_CBC_SHA256, " +
    "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256, " +
    "TLS_DHE_RSA_WITH_AES_128_CBC_SHA256, " +
    // and their equivalent names in the IBM Security provider
    "SSL_ECDHE_RSA_WITH_AES_256_CBC_SHA384, " +
    "SSL_RSA_WITH_AES_256_CBC_SHA256, " +
    "SSL_DHE_RSA_WITH_AES_256_CBC_SHA256, " +
    "SSL_ECDHE_RSA_WITH_AES_128_CBC_SHA256, " +
    "SSL_DHE_RSA_WITH_AES_128_CBC_SHA256"

  def sparkSSLConfig(): SparkConf = {
    val conf = new SparkConf(loadDefaults = false)
    conf.set("spark.ssl.enabled", "true")
    conf.set("spark.ssl.keyStore", keyStorePath)
    conf.set("spark.ssl.keyStorePassword", "password")
    conf.set("spark.ssl.keyPassword", "password")
    conf.set("spark.ssl.trustStore", trustStorePath)
    conf.set("spark.ssl.trustStorePassword", "password")
    conf.set("spark.ssl.enabledAlgorithms", enabledAlgorithms)
    conf.set("spark.ssl.protocol", "TLSv1.2")
    conf
  }

  def sparkSSLConfigUntrusted(): SparkConf = {
    val conf = new SparkConf(loadDefaults = false)
    conf.set("spark.ssl.enabled", "true")
    conf.set("spark.ssl.keyStore", untrustedKeyStorePath)
    conf.set("spark.ssl.keyStorePassword", "password")
    conf.set("spark.ssl.keyPassword", "password")
    conf.set("spark.ssl.trustStore", trustStorePath)
    conf.set("spark.ssl.trustStorePassword", "password")
    conf.set("spark.ssl.enabledAlgorithms", enabledAlgorithms)
    conf.set("spark.ssl.protocol", "TLSv1.2")
    conf
  }

}
