/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.spark.sql.hive

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

class MetastoreRelationSuite extends QueryTest with SQLTestUtils with TestHiveSingleton {
  test("makeCopy and toJSON should work") {
    val table = CatalogTable(
      identifier = TableIdentifier("test", Some("db")),
      tableType = CatalogTableType.VIEW,
      storage = CatalogStorageFormat.empty,
      schema = StructType(StructField("a", IntegerType, true) :: Nil))
    val relation = MetastoreRelation("db", "test")(table, null)

    // No exception should be thrown
    relation.makeCopy(Array("db", "test"))
    // No exception should be thrown
    relation.toJSON
  }

  test("SPARK-17409: Do Not Optimize Query in CTAS (Hive Serde Table) More Than Once") {
    withTable("bar") {
      withTempView("foo") {
        sql("select 0 as id").createOrReplaceTempView("foo")
        // If we optimize the query in CTAS more than once, the following saveAsTable will fail
        // with the error: `GROUP BY position 0 is not in select list (valid range is [1, 1])`
        sql("CREATE TABLE bar AS SELECT * FROM foo group by id")
        checkAnswer(spark.table("bar"), Row(0) :: Nil)
        val tableMetadata = spark.sessionState.catalog.getTableMetadata(TableIdentifier("bar"))
        assert(tableMetadata.provider == Some("hive"), "the expected table is a Hive serde table")
      }
    }
  }
}
