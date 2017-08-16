/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.hive.thriftserver

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.types.{NullType, StructField, StructType}

class SparkExecuteStatementOperationSuite extends SparkFunSuite {
  test("SPARK-17112 `select null` via JDBC triggers IllegalArgumentException in ThriftServer") {
    val field1 = StructField("NULL", NullType)
    val field2 = StructField("(IF(true, NULL, NULL))", NullType)
    val tableSchema = StructType(Seq(field1, field2))
    val columns = SparkExecuteStatementOperation.getTableSchema(tableSchema).getColumnDescriptors()
    assert(columns.size() == 2)
    assert(columns.get(0).getType() == org.apache.hive.service.cli.Type.NULL_TYPE)
    assert(columns.get(1).getType() == org.apache.hive.service.cli.Type.NULL_TYPE)
  }
}
