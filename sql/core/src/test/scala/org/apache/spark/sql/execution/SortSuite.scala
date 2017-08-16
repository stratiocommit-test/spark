/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.execution

import scala.util.Random

import org.apache.spark.AccumulatorSuite
import org.apache.spark.sql.{RandomDataGenerator, Row}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._

/**
 * Test sorting. Many of the test cases generate random data and compares the sorted result with one
 * sorted by a reference implementation ([[ReferenceSort]]).
 */
class SortSuite extends SparkPlanTest with SharedSQLContext {
  import testImplicits.newProductEncoder
  import testImplicits.localSeqToDatasetHolder

  test("basic sorting using ExternalSort") {

    val input = Seq(
      ("Hello", 4, 2.0),
      ("Hello", 1, 1.0),
      ("World", 8, 3.0)
    )

    checkAnswer(
      input.toDF("a", "b", "c"),
      (child: SparkPlan) => SortExec('a.asc :: 'b.asc :: Nil, global = true, child = child),
      input.sortBy(t => (t._1, t._2)).map(Row.fromTuple),
      sortAnswers = false)

    checkAnswer(
      input.toDF("a", "b", "c"),
      (child: SparkPlan) => SortExec('b.asc :: 'a.asc :: Nil, global = true, child = child),
      input.sortBy(t => (t._2, t._1)).map(Row.fromTuple),
      sortAnswers = false)
  }

  test("sorting all nulls") {
    checkThatPlansAgree(
      (1 to 100).map(v => Tuple1(v)).toDF().selectExpr("NULL as a"),
      (child: SparkPlan) =>
        GlobalLimitExec(10, SortExec('a.asc :: Nil, global = true, child = child)),
      (child: SparkPlan) =>
        GlobalLimitExec(10, ReferenceSort('a.asc :: Nil, global = true, child)),
      sortAnswers = false
    )
  }

  test("sort followed by limit") {
    checkThatPlansAgree(
      (1 to 100).map(v => Tuple1(v)).toDF("a"),
      (child: SparkPlan) =>
        GlobalLimitExec(10, SortExec('a.asc :: Nil, global = true, child = child)),
      (child: SparkPlan) =>
        GlobalLimitExec(10, ReferenceSort('a.asc :: Nil, global = true, child)),
      sortAnswers = false
    )
  }

  test("sorting does not crash for large inputs") {
    val sortOrder = 'a.asc :: Nil
    val stringLength = 1024 * 1024 * 2
    checkThatPlansAgree(
      Seq(Tuple1("a" * stringLength), Tuple1("b" * stringLength)).toDF("a").repartition(1),
      SortExec(sortOrder, global = true, _: SparkPlan, testSpillFrequency = 1),
      ReferenceSort(sortOrder, global = true, _: SparkPlan),
      sortAnswers = false
    )
  }

  test("sorting updates peak execution memory") {
    AccumulatorSuite.verifyPeakExecutionMemorySet(sparkContext, "unsafe external sort") {
      checkThatPlansAgree(
        (1 to 100).map(v => Tuple1(v)).toDF("a"),
        (child: SparkPlan) => SortExec('a.asc :: Nil, global = true, child = child),
        (child: SparkPlan) => ReferenceSort('a.asc :: Nil, global = true, child),
        sortAnswers = false)
    }
  }

  // Test sorting on different data types
  for (
    dataType <- DataTypeTestUtils.atomicTypes ++ Set(NullType);
    nullable <- Seq(true, false);
    sortOrder <-
      Seq('a.asc :: Nil, 'a.asc_nullsLast :: Nil, 'a.desc :: Nil, 'a.desc_nullsFirst :: Nil);
    randomDataGenerator <- RandomDataGenerator.forType(dataType, nullable)
  ) {
    test(s"sorting on $dataType with nullable=$nullable, sortOrder=$sortOrder") {
      val inputData = Seq.fill(1000)(randomDataGenerator())
      val inputDf = spark.createDataFrame(
        sparkContext.parallelize(Random.shuffle(inputData).map(v => Row(v))),
        StructType(StructField("a", dataType, nullable = true) :: Nil)
      )
      checkThatPlansAgree(
        inputDf,
        p => SortExec(sortOrder, global = true, p: SparkPlan, testSpillFrequency = 23),
        ReferenceSort(sortOrder, global = true, _: SparkPlan),
        sortAnswers = false
      )
    }
  }
}
