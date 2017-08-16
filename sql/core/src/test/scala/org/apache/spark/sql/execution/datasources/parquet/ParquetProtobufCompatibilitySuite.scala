/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.execution.datasources.parquet

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.SharedSQLContext

class ParquetProtobufCompatibilitySuite extends ParquetCompatibilityTest with SharedSQLContext {
  test("unannotated array of primitive type") {
    checkAnswer(readResourceParquetFile("test-data/old-repeated-int.parquet"), Row(Seq(1, 2, 3)))
  }

  test("unannotated array of struct") {
    checkAnswer(
      readResourceParquetFile("test-data/old-repeated-message.parquet"),
      Row(
        Seq(
          Row("First inner", null, null),
          Row(null, "Second inner", null),
          Row(null, null, "Third inner"))))

    checkAnswer(
      readResourceParquetFile("test-data/proto-repeated-struct.parquet"),
      Row(
        Seq(
          Row("0 - 1", "0 - 2", "0 - 3"),
          Row("1 - 1", "1 - 2", "1 - 3"))))

    checkAnswer(
      readResourceParquetFile("test-data/proto-struct-with-array-many.parquet"),
      Seq(
        Row(
          Seq(
            Row("0 - 0 - 1", "0 - 0 - 2", "0 - 0 - 3"),
            Row("0 - 1 - 1", "0 - 1 - 2", "0 - 1 - 3"))),
        Row(
          Seq(
            Row("1 - 0 - 1", "1 - 0 - 2", "1 - 0 - 3"),
            Row("1 - 1 - 1", "1 - 1 - 2", "1 - 1 - 3"))),
        Row(
          Seq(
            Row("2 - 0 - 1", "2 - 0 - 2", "2 - 0 - 3"),
            Row("2 - 1 - 1", "2 - 1 - 2", "2 - 1 - 3")))))
  }

  test("struct with unannotated array") {
    checkAnswer(
      readResourceParquetFile("test-data/proto-struct-with-array.parquet"),
      Row(10, 9, Seq.empty, null, Row(9), Seq(Row(9), Row(10))))
  }

  test("unannotated array of struct with unannotated array") {
    checkAnswer(
      readResourceParquetFile("test-data/nested-array-struct.parquet"),
      Seq(
        Row(2, Seq(Row(1, Seq(Row(3))))),
        Row(5, Seq(Row(4, Seq(Row(6))))),
        Row(8, Seq(Row(7, Seq(Row(9)))))))
  }

  test("unannotated array of string") {
    checkAnswer(
      readResourceParquetFile("test-data/proto-repeated-string.parquet"),
      Seq(
        Row(Seq("hello", "world")),
        Row(Seq("good", "bye")),
        Row(Seq("one", "two", "three"))))
  }
}
