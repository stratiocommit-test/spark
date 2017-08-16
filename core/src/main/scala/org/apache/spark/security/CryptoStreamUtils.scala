/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.security

import java.io.{InputStream, OutputStream}
import java.util.Properties
import javax.crypto.KeyGenerator
import javax.crypto.spec.{IvParameterSpec, SecretKeySpec}

import org.apache.commons.crypto.random._
import org.apache.commons.crypto.stream._

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._

/**
 * A util class for manipulating IO encryption and decryption streams.
 */
private[spark] object CryptoStreamUtils extends Logging {

  // The initialization vector length in bytes.
  val IV_LENGTH_IN_BYTES = 16
  // The prefix of IO encryption related configurations in Spark configuration.
  val SPARK_IO_ENCRYPTION_COMMONS_CONFIG_PREFIX = "spark.io.encryption.commons.config."
  // The prefix for the configurations passing to Apache Commons Crypto library.
  val COMMONS_CRYPTO_CONF_PREFIX = "commons.crypto."

  /**
   * Helper method to wrap `OutputStream` with `CryptoOutputStream` for encryption.
   */
  def createCryptoOutputStream(
      os: OutputStream,
      sparkConf: SparkConf,
      key: Array[Byte]): OutputStream = {
    val properties = toCryptoConf(sparkConf)
    val iv = createInitializationVector(properties)
    os.write(iv)
    val transformationStr = sparkConf.get(IO_CRYPTO_CIPHER_TRANSFORMATION)
    new CryptoOutputStream(transformationStr, properties, os,
      new SecretKeySpec(key, "AES"), new IvParameterSpec(iv))
  }

  /**
   * Helper method to wrap `InputStream` with `CryptoInputStream` for decryption.
   */
  def createCryptoInputStream(
      is: InputStream,
      sparkConf: SparkConf,
      key: Array[Byte]): InputStream = {
    val properties = toCryptoConf(sparkConf)
    val iv = new Array[Byte](IV_LENGTH_IN_BYTES)
    is.read(iv, 0, iv.length)
    val transformationStr = sparkConf.get(IO_CRYPTO_CIPHER_TRANSFORMATION)
    new CryptoInputStream(transformationStr, properties, is,
      new SecretKeySpec(key, "AES"), new IvParameterSpec(iv))
  }

  /**
   * Get Commons-crypto configurations from Spark configurations identified by prefix.
   */
  def toCryptoConf(conf: SparkConf): Properties = {
    val props = new Properties()
    conf.getAll.foreach { case (k, v) =>
      if (k.startsWith(SPARK_IO_ENCRYPTION_COMMONS_CONFIG_PREFIX)) {
        props.put(COMMONS_CRYPTO_CONF_PREFIX + k.substring(
          SPARK_IO_ENCRYPTION_COMMONS_CONFIG_PREFIX.length()), v)
      }
    }
    props
  }

  /**
   * Creates a new encryption key.
   */
  def createKey(conf: SparkConf): Array[Byte] = {
    val keyLen = conf.get(IO_ENCRYPTION_KEY_SIZE_BITS)
    val ioKeyGenAlgorithm = conf.get(IO_ENCRYPTION_KEYGEN_ALGORITHM)
    val keyGen = KeyGenerator.getInstance(ioKeyGenAlgorithm)
    keyGen.init(keyLen)
    keyGen.generateKey().getEncoded()
  }

  /**
   * This method to generate an IV (Initialization Vector) using secure random.
   */
  private[this] def createInitializationVector(properties: Properties): Array[Byte] = {
    val iv = new Array[Byte](IV_LENGTH_IN_BYTES)
    val initialIVStart = System.currentTimeMillis()
    CryptoRandomFactory.getCryptoRandom(properties).nextBytes(iv)
    val initialIVFinish = System.currentTimeMillis()
    val initialIVTime = initialIVFinish - initialIVStart
    if (initialIVTime > 2000) {
      logWarning(s"It costs ${initialIVTime} milliseconds to create the Initialization Vector " +
        s"used by CryptoStream")
    }
    iv
  }
}
