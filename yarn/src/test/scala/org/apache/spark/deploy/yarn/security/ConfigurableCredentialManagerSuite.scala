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
import org.apache.hadoop.io.Text
import org.apache.hadoop.security.Credentials
import org.apache.hadoop.security.token.Token
import org.scalatest.{BeforeAndAfter, Matchers}

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.yarn.config._

class ConfigurableCredentialManagerSuite extends SparkFunSuite with Matchers with BeforeAndAfter {
  private var credentialManager: ConfigurableCredentialManager = null
  private var sparkConf: SparkConf = null
  private var hadoopConf: Configuration = null

  override def beforeAll(): Unit = {
    super.beforeAll()

    sparkConf = new SparkConf()
    hadoopConf = new Configuration()
    System.setProperty("SPARK_YARN_MODE", "true")
  }

  override def afterAll(): Unit = {
    System.clearProperty("SPARK_YARN_MODE")

    super.afterAll()
  }

  test("Correctly load default credential providers") {
    credentialManager = new ConfigurableCredentialManager(sparkConf, hadoopConf)

    credentialManager.getServiceCredentialProvider("hdfs") should not be (None)
    credentialManager.getServiceCredentialProvider("hbase") should not be (None)
    credentialManager.getServiceCredentialProvider("hive") should not be (None)
  }

  test("disable hive credential provider") {
    sparkConf.set("spark.yarn.security.credentials.hive.enabled", "false")
    credentialManager = new ConfigurableCredentialManager(sparkConf, hadoopConf)

    credentialManager.getServiceCredentialProvider("hdfs") should not be (None)
    credentialManager.getServiceCredentialProvider("hbase") should not be (None)
    credentialManager.getServiceCredentialProvider("hive") should be (None)
  }

  test("using deprecated configurations") {
    sparkConf.set("spark.yarn.security.tokens.hdfs.enabled", "false")
    sparkConf.set("spark.yarn.security.tokens.hive.enabled", "false")
    credentialManager = new ConfigurableCredentialManager(sparkConf, hadoopConf)

    credentialManager.getServiceCredentialProvider("hdfs") should be (None)
    credentialManager.getServiceCredentialProvider("hive") should be (None)
    credentialManager.getServiceCredentialProvider("test") should not be (None)
    credentialManager.getServiceCredentialProvider("hbase") should not be (None)
  }

  test("verify obtaining credentials from provider") {
    credentialManager = new ConfigurableCredentialManager(sparkConf, hadoopConf)
    val creds = new Credentials()

    // Tokens can only be obtained from TestTokenProvider, for hdfs, hbase and hive tokens cannot
    // be obtained.
    credentialManager.obtainCredentials(hadoopConf, creds)
    val tokens = creds.getAllTokens
    tokens.size() should be (1)
    tokens.iterator().next().getService should be (new Text("test"))
  }

  test("verify getting credential renewal info") {
    credentialManager = new ConfigurableCredentialManager(sparkConf, hadoopConf)
    val creds = new Credentials()

    val testCredentialProvider = credentialManager.getServiceCredentialProvider("test").get
      .asInstanceOf[TestCredentialProvider]
    // Only TestTokenProvider can get the time of next token renewal
    val nextRenewal = credentialManager.obtainCredentials(hadoopConf, creds)
    nextRenewal should be (testCredentialProvider.timeOfNextTokenRenewal)
  }

  test("obtain tokens For HiveMetastore") {
    val hadoopConf = new Configuration()
    hadoopConf.set("hive.metastore.kerberos.principal", "bob")
    // thrift picks up on port 0 and bails out, without trying to talk to endpoint
    hadoopConf.set("hive.metastore.uris", "http://localhost:0")

    val hiveCredentialProvider = new HiveCredentialProvider()
    val credentials = new Credentials()
    hiveCredentialProvider.obtainCredentials(hadoopConf, sparkConf, credentials)

    credentials.getAllTokens.size() should be (0)
  }

  test("Obtain tokens For HBase") {
    val hadoopConf = new Configuration()
    hadoopConf.set("hbase.security.authentication", "kerberos")

    val hbaseTokenProvider = new HBaseCredentialProvider()
    val creds = new Credentials()
    hbaseTokenProvider.obtainCredentials(hadoopConf, sparkConf, creds)

    creds.getAllTokens.size should be (0)
  }
}

class TestCredentialProvider extends ServiceCredentialProvider {
  val tokenRenewalInterval = 86400 * 1000L
  var timeOfNextTokenRenewal = 0L

  override def serviceName: String = "test"

  override def credentialsRequired(conf: Configuration): Boolean = true

  override def obtainCredentials(
      hadoopConf: Configuration,
      sparkConf: SparkConf,
      creds: Credentials): Option[Long] = {
    if (creds == null) {
      // Guard out other unit test failures.
      return None
    }

    val emptyToken = new Token()
    emptyToken.setService(new Text("test"))
    creds.addToken(emptyToken.getService, emptyToken)

    val currTime = System.currentTimeMillis()
    timeOfNextTokenRenewal = (currTime - currTime % tokenRenewalInterval) + tokenRenewalInterval

    Some(timeOfNextTokenRenewal)
  }
}
