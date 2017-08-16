/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.sql.test.SharedSQLContext

class SerializationSuite extends SparkFunSuite with SharedSQLContext {

  test("[SPARK-5235] SQLContext should be serializable") {
    val spark = SparkSession.builder.getOrCreate()
    new JavaSerializer(new SparkConf()).newInstance().serialize(spark.sqlContext)
  }
}
