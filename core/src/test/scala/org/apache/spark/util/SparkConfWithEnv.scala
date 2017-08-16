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

import org.apache.spark.SparkConf

/**
 * Customized SparkConf that allows env variables to be overridden.
 */
class SparkConfWithEnv(env: Map[String, String]) extends SparkConf(false) {
  override def getenv(name: String): String = env.getOrElse(name, super.getenv(name))

  override def clone: SparkConf = {
    new SparkConfWithEnv(env).setAll(getAll)
  }

}
