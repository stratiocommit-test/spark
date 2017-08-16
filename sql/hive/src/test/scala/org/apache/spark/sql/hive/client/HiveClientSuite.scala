/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.hive.client

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.conf.HiveConf

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Literal}
import org.apache.spark.sql.hive.HiveUtils
import org.apache.spark.sql.types.IntegerType

class HiveClientSuite extends SparkFunSuite {
  private val clientBuilder = new HiveClientBuilder

  private val tryDirectSqlKey = HiveConf.ConfVars.METASTORE_TRY_DIRECT_SQL.varname

  test(s"getPartitionsByFilter returns all partitions when $tryDirectSqlKey=false") {
    val testPartitionCount = 5

    val storageFormat = CatalogStorageFormat(
      locationUri = None,
      inputFormat = None,
      outputFormat = None,
      serde = None,
      compressed = false,
      properties = Map.empty)

    val hadoopConf = new Configuration()
    hadoopConf.setBoolean(tryDirectSqlKey, false)
    val client = clientBuilder.buildClient(HiveUtils.hiveExecutionVersion, hadoopConf)
    client.runSqlHive("CREATE TABLE test (value INT) PARTITIONED BY (part INT)")

    val partitions = (1 to testPartitionCount).map { part =>
      CatalogTablePartition(Map("part" -> part.toString), storageFormat)
    }
    client.createPartitions(
      "default", "test", partitions, ignoreIfExists = false)

    val filteredPartitions = client.getPartitionsByFilter(client.getTable("default", "test"),
      Seq(EqualTo(AttributeReference("part", IntegerType)(), Literal(3))))

    assert(filteredPartitions.size == testPartitionCount)
  }
}
